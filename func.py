"""Function que exporta métricas do OCI Monitoring para o Dynatrace.

O fluxo principal do projeto original foi preservado. A diferença é que todas
as métricas recebidas na invocação são transformadas antes do envio e depois
agrupadas em um ou poucos payloads HTTP multiline.
"""

from __future__ import annotations

import io
import json
import logging
import os
import resource
import time
from typing import Dict, Iterable, Optional
from urllib.parse import quote, urlparse, urlunparse

from aggregation import create_minutely_summary_buckets
from dynatrace_client import DynatraceClient, HttpOptions
from metric_mapping import namespace_map
from mint import MintMetric
from summary_stat import SummaryStat


_CLIENT: Optional[DynatraceClient] = None
_CLIENT_SIGNATURE: Optional[tuple[str, ...]] = None

# Mapeamento fixo das dimensões OCI para chaves estáveis no Dynatrace.
GENERIC_DIMENSIONS = {
    "resourceGroup": "oci.resource_group",
    "compartmentId": "oci.compartment_id",
    "resourceId": "oci.resource_id",
    "region": "oci.region",
    "resourceDisplayName": "oci.resource_display_name",
    "availabilityDomain": "oci.availability_domain",
    "faultDomain": "oci.fault_domain",
}


def _log(level: int, event: str, **fields: object) -> None:
    message = {"event": event, **{k: v for k, v in fields.items() if v is not None}}
    # Cada evento é serializado como uma única linha JSON para facilitar filtros e
    # correlação no OCI Logging sem depender de parsing de mensagens livres.
    logging.getLogger().log(
        level,
        json.dumps(message, sort_keys=True, separators=(",", ":"), default=str),
    )

# Configurações de OCI Functions chegam como strings. Valores inválidos geram
# exceção de forma intencional para evitar execução com parâmetros ambíguos.
def _bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() == "true"


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))

# Normaliza caracteres que quebrariam a linha MINT. MintMetric também aplica
# escape; qualquer mudança nesta função deve ser testada com aspas e barras
# invertidas para evitar escape duplicado.
def _safe_dimension(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")

# Combina dimensões do objeto `dimensions` com campos que o Connector Hub envia
# no nível superior do registro, como compartmentId e resourceGroup.
def _generic_dimensions(body: Dict, namespace: str) -> Dict[str, str]:
    source = dict(body.get("dimensions") or {})
    source["resourceGroup"] = body.get("resourceGroup")
    source["compartmentId"] = body.get("compartmentId")

    result = {
        "cloud.provider": "oci",
        "oci.namespace": namespace,
    }
    for source_key, target_key in GENERIC_DIMENSIONS.items():
        value = source.get(source_key)
        if value not in (None, ""):
            result[target_key] = _safe_dimension(value)
    return result


def _metric_lines(body: Dict) -> list[str]:
    """Converte uma metric stream recebida do Connector Hub em linhas MINT."""

    namespace = body.get("namespace")
    metric_name = body.get("name")
    datapoints = body.get("datapoints") or []

    if not namespace or not metric_name:
        raise ValueError("Metric record is missing namespace or name")
    if not isinstance(datapoints, list):
        raise ValueError(f"datapoints must be a list for {namespace}/{metric_name}")

    oci_dimensions = dict(body.get("dimensions") or {})
    oci_dimensions["resourceGroup"] = body.get("resourceGroup")
    oci_dimensions["compartmentId"] = body.get("compartmentId")

    # Modo genérico: mantém o nome OCI da métrica e resume todos os datapoints do
    # mesmo minuto em uma única linha gauge com min/max/sum/count.
    if _bool("IMPORT_ALL_METRICS"):
        key = f"cloud.oci.{namespace.replace('oci_', '')}.{metric_name}"
        lines: list[str] = []
        for timestamp, bucket in sorted(
            create_minutely_summary_buckets(
                datapoints
            ).items()
        ):
            summary = SummaryStat(
                bucket.value_min,
                bucket.value_max,
                bucket.value_sum,
                bucket.value_count,
            )
            lines.append(
                str(
                    MintMetric(
                        key,
                        summary,
                        _generic_dimensions(body, namespace),
                        timestamp * 1000,
                    )
                )
            )
        return lines

    # Modo curado: delega nome, agregação e dimensões ao catálogo de mappings.
    # Este caminho depende de `metric_mapping.py` definir `namespace_map`.
    metric_map = namespace_map.get(namespace)
    if metric_map is None:
        _log(
            logging.WARNING,
            "mapping_not_found",
            namespace=namespace,
            metric=metric_name,
        )
        return []

    mapped = metric_map.value_from_oci_metric_name(metric_name, oci_dimensions, datapoints)
    if not mapped:
        _log(
            logging.DEBUG,
            "metric_not_mapped",
            namespace=namespace,
            metric=metric_name,
        )
        return []

    metric_key, results = mapped
    dimensions = {
        key: _safe_dimension(value)
        for key, value in metric_map.dimensions(oci_dimensions).items()
        if value not in (None, "")
    }
    # No projeto original, a chamada de envio ficava fora deste loop e somente o
    # último resultado mapeado era entregue. Aqui todos os resultados são retornados.
    return [
        str(MintMetric(metric_key, result.value, dimensions, result.timestamp * 1000))
        for result in results
    ]

# Constrói as opções efetivamente suportadas pelo cliente HTTP.
def _http_options() -> HttpOptions:
    return HttpOptions(
        max_payload_bytes=_int("MAX_PAYLOAD_BYTES", 750_000),
        max_lines_per_request=_int("MAX_LINES_PER_REQUEST", 5_000),
        connect_timeout_seconds=_float("HTTP_CONNECT_TIMEOUT_SECONDS", 3.0),
        read_timeout_seconds=_float("HTTP_READ_TIMEOUT_SECONDS", 10.0),
        max_attempts=_int("HTTP_MAX_ATTEMPTS", 2),
        backoff_base_seconds=_float("HTTP_BACKOFF_BASE_SECONDS", 0.5),
        safety_margin_seconds=_float("FUNCTION_SAFETY_MARGIN_SECONDS", 3.0),
    )


def _client() -> DynatraceClient:
    """Reutiliza o cliente no container aquecido e o recria se a configuração mudar."""

    global _CLIENT, _CLIENT_SIGNATURE
    auth_method = os.environ.get("AUTH_METHOD", "token").strip().lower()
    # A assinatura serve apenas para detectar mudança de configuração e invalidar o
    # cliente em cache. Nunca registre este valor, pois ele contém credenciais.
    signature = (
        os.environ["DYNATRACE_TENANT"],
        auth_method,
        os.environ.get("DYNATRACE_API_KEY", ""),
        os.environ.get("OAUTH_CLIENT_ID", ""),
        os.environ.get("OAUTH_CLIENT_SECRET", ""),
        os.environ.get("OAUTH_ACCOUNT_URN", ""),
        str(_http_options()),
    )
    if _CLIENT is not None and signature == _CLIENT_SIGNATURE:
        return _CLIENT

    client = DynatraceClient(signature[0], _http_options())
    if auth_method == "token":
        client.using_api_token(signature[2])
    elif auth_method == "oauth":
        client.using_oauth(signature[3], signature[4], signature[5])
    else:
        raise ValueError("AUTH_METHOD must be 'token' or 'oauth'")

    _CLIENT, _CLIENT_SIGNATURE = client, signature
    return client

# Monta a URL de proxy aceita pelo requests. Usuário e senha são URL-encoded.
# Evite registrar a URL completa, pois ela pode conter credenciais.
def create_proxy_connection() -> Optional[str]:
    value = os.environ.get("PROXY_URL", "").strip()
    if not value:
        return None
    if value.startswith("<"):
        raise ValueError("PROXY_URL still contains a placeholder")

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("PROXY_URL must include http:// or https:// and a hostname")

    username = os.environ.get("PROXY_USERNAME", "")
    password = os.environ.get("PROXY_PASSWORD", "")
    authentication = ""
    if username:
        authentication = quote(username, safe="")
        if password:
            authentication += f":{quote(password, safe='')}"
        authentication += "@"

    host = parsed.hostname
    if parsed.port:
        host += f":{parsed.port}"
    return urlunparse((parsed.scheme, f"{authentication}{host}", parsed.path, "", "", ""))

# Tenta obter um identificador de correlação de diferentes formatos de contexto.
def _request_id(ctx: object) -> Optional[str]:
    for name in ("RequestID", "request_id", "requestId"):
        value = getattr(ctx, name, None)
        if callable(value):
            try:
                return str(value())
            except Exception:
                continue
        if value:
            return str(value)
    return None

# Lê o RSS atual do processo em /proc. O valor representa memória residente do
# container naquele instante e não a memória alocada pela plataforma.
def _current_rss_mb() -> Optional[float]:
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        with open("/proc/self/statm", "r", encoding="utf-8") as stream:
            resident_pages = int(stream.read().split()[1])
        return round(resident_pages * page_size / 1024 / 1024, 2)
    except (OSError, ValueError, IndexError):
        return None


def _peak_rss_mb() -> float:
    # No Linux, ru_maxrss é retornado em KiB. OCI Functions executa este código
    # em runtime Linux, por isso a conversão abaixo divide o valor por 1024.
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)

# Calcula idade da origem apenas para timestamps numéricos. Timestamps ISO-8601
# continuam sendo processados pela agregação, mas não entram nesta telemetria.
def _oldest_timestamp_ms(records: Iterable[Dict]) -> Optional[int]:
    timestamps = [
        point.get("timestamp")
        for record in records
        for point in (record.get("datapoints") or [])
        if isinstance(point, dict) and isinstance(point.get("timestamp"), (int, float))
    ]
    return int(min(timestamps)) if timestamps else None

# O handler é o entrypoint exigido pelo FDK. Toda exceção não tratada faz a
# invocação falhar e permite que o Connector Hub aplique retry at-least-once.
def handler(ctx, data: io.BytesIO = None):
    logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    started = time.monotonic()
    cpu_started = time.process_time()
    request_id = _request_id(ctx)

    try:
        # O Connector Hub envia um objeto JSON ou uma lista de objetos. Cada objeto
        # representa uma metric stream e pode conter vários datapoints.
        raw = data.getvalue() if data is not None else b""
        body = json.loads(raw)
        records = body if isinstance(body, list) else [body]
        if not all(isinstance(record, dict) for record in records):
            raise ValueError("Connector Hub payload must contain JSON objects")

        datapoints_in = sum(len(record.get("datapoints") or []) for record in records)
        oldest = _oldest_timestamp_ms(records)
        _log(
            logging.INFO,
            "invocation_start",
            request_id=request_id,
            records_in=len(records),
            datapoints_in=datapoints_in,
            input_bytes=len(raw),
            current_rss_mb=_current_rss_mb(),
            peak_rss_mb=_peak_rss_mb(),
            oldest_source_age_seconds=(
                round(time.time() - oldest / 1000, 1) if oldest is not None else None
            ),
        )

        transform_started = time.monotonic()
        # Toda a transformação ocorre antes do primeiro POST. Assim, um erro local de
        # parsing/mapping não deixa parte do lote já enviada ao Dynatrace.
        lines = [
            line
            for record in records
            for line in _metric_lines(record)
        ]
        transform_ms = round((time.monotonic() - transform_started) * 1000)

        # Deadline local baseado na configuração. Mantenha FUNCTION_TIMEOUT_SECONDS
        # alinhado ao timeout real da Function.
        timeout_seconds = _float("FUNCTION_TIMEOUT_SECONDS", 60.0)
        deadline = started + timeout_seconds
        proxy_url = create_proxy_connection()
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        # O cliente divide as linhas por bytes/quantidade, reutiliza conexão e aplica
        # retry somente a falhas classificadas como transitórias.
        stats = _client().send_metric_lines(
            lines,
            proxies=proxies,
            deadline_monotonic=deadline,
            request_id=request_id,
        )

        total_ms = round((time.monotonic() - started) * 1000)
        # Resumo único da invocação: use estes campos para comparar transformação,
        # espera HTTP, CPU e memória na mesma janela.
        _log(
            logging.INFO,
            "invocation_end",
            request_id=request_id,
            records_in=len(records),
            datapoints_in=datapoints_in,
            metric_lines=len(lines),
            transform_ms=transform_ms,
            total_ms=total_ms,
            cpu_ms=round((time.process_time() - cpu_started) * 1000),
            current_rss_mb=_current_rss_mb(),
            peak_rss_mb=_peak_rss_mb(),
            function_memory_limit_mb=os.environ.get("FN_MEMORY"),
            chunks=stats.chunks,
            http_attempts=stats.attempts,
            retries=stats.retries,
            http_total_ms=stats.http_total_ms,
            http_max_ms=stats.http_max_ms,
            lines_ok=stats.lines_ok,
            lines_invalid=stats.lines_invalid,
            payload_bytes=stats.payload_bytes,
        )

    except Exception as exc:
        _log(
            logging.ERROR,
            "invocation_failed",
            request_id=request_id,
            total_ms=round((time.monotonic() - started) * 1000),
            cpu_ms=round((time.process_time() - cpu_started) * 1000),
            current_rss_mb=_current_rss_mb(),
            peak_rss_mb=_peak_rss_mb(),
            error_type=type(exc).__name__,
            error=str(exc)[:1000],
        )
        # Importante: o Connector Hub só tenta novamente quando a invocação da Function realmente falha. Nunca ignore/capture exceções de entrega.
        raise
