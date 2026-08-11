"""Cliente para a Metrics API v2 do Dynatrace.

Mantém a interface pública do projeto original e adiciona somente os controles
necessários para esta integração: reutilização de conexão, payload multiline,
retry limitado, validação de status e logs estruturados de tempo/rede.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import random
import time
from typing import Dict, Iterator, Optional, Sequence
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ConnectTimeout,
    ConnectionError as RequestsConnectionError,
    ProxyError,
    ReadTimeout,
    SSLError,
)

METRIC_INGEST_ENDPOINT = "/api/v2/metrics/ingest"
OAUTH_TOKEN_URL = "https://sso.dynatrace.com/sso/oauth2/token"
TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}

# Separa falhas transitórias, que podem ser repetidas, de falhas permanentes,
# que exigem correção de configuração ou payload.
class DynatraceError(RuntimeError):
    pass


class DynatraceTransientError(DynatraceError):
    pass


class DynatracePermanentError(DynatraceError):
    pass


@dataclass(frozen=True)
class HttpOptions:
    """Parâmetros de chunking, timeout e retry usados pelo cliente HTTP."""
    max_payload_bytes: int = 750_000
    max_lines_per_request: int = 5_000
    connect_timeout_seconds: float = 3.0
    read_timeout_seconds: float = 10.0
    max_attempts: int = 2
    backoff_base_seconds: float = 0.5
    safety_margin_seconds: float = 3.0


@dataclass
class DeliveryStats:
    """Contadores agregados dos chunks enviados durante uma invocação."""
    chunks: int = 0
    attempts: int = 0
    retries: int = 0
    lines_ok: int = 0
    lines_invalid: int = 0
    payload_bytes: int = 0
    http_total_ms: int = 0
    http_max_ms: int = 0


def _log(level: int, event: str, **fields: object) -> None:
    message = {"event": event, **{k: v for k, v in fields.items() if v is not None}}
    logging.getLogger().log(
        level,
        json.dumps(message, sort_keys=True, separators=(",", ":"), default=str),
    )

# Extrai somente a metric key para logs, sem registrar dimensões ou valores.
def _key(line: str) -> str:
    return line.split(",", 1)[0].split(" ", 1)[0]

# Agrupa linhas pelo primeiro limite atingido: bytes UTF-8 ou quantidade de
# linhas. Uma linha individual maior que o limite é um erro permanente.
def _chunks(lines: Sequence[str], max_bytes: int, max_lines: int) -> Iterator[list[str]]:
    current: list[str] = []
    size = 0
    for line in lines:
        line_size = len(line.encode("utf-8"))
        if line_size > max_bytes:
            raise DynatracePermanentError(
                f"Metric line exceeds payload limit: key={_key(line)} bytes={line_size}"
            )
        separator = 1 if current else 0
        if current and (size + separator + line_size > max_bytes or len(current) >= max_lines):
            yield current
            current, size, separator = [], 0, 0
        current.append(line)
        size += separator + line_size
    if current:
        yield current

# Sessão global reutilizada por containers aquecidos. O pool reduz conexões TCP
# e handshakes TLS, principalmente quando há vários chunks por invocação.
_SESSION = requests.Session()
_ADAPTER = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0)
_SESSION.mount("https://", _ADAPTER)
_SESSION.mount("http://", _ADAPTER)


class DynatraceClient:
    """Fachada compatível com a interface pública do projeto original."""

    def __init__(self, tenant: str, options: Optional[HttpOptions] = None):
        self._tenant = self._validate_tenant(tenant)
        self._endpoint = f"{self._tenant}{METRIC_INGEST_ENDPOINT}"
        self._options = options or HttpOptions()
        self._auth_method: Optional[str] = None
        self._api_token: Optional[str] = None
        self._oauth_client_id: Optional[str] = None
        self._oauth_client_secret: Optional[str] = None
        self._oauth_urn: Optional[str] = None
        self._oauth_token: Optional[str] = None
        self._oauth_expiration = 0.0

    @staticmethod
    # A interface web usa apps.dynatrace.com, mas a Metrics API do ambiente SaaS
    # deve ser chamada no endpoint live.dynatrace.com.
    def _validate_tenant(tenant: str) -> str:
        value = tenant.strip().rstrip("/")
        if value.endswith(METRIC_INGEST_ENDPOINT):
            value = value[: -len(METRIC_INGEST_ENDPOINT)].rstrip("/")
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("DYNATRACE_TENANT must be a valid HTTPS base URL")
        if parsed.hostname.endswith(".apps.dynatrace.com"):
            raise ValueError("Use *.live.dynatrace.com, not *.apps.dynatrace.com")
        return value

    def using_api_token(self, api_token: str) -> "DynatraceClient":
        if not api_token or api_token.startswith("<"):
            raise ValueError("DYNATRACE_API_KEY is empty or still contains a placeholder")
        self._auth_method, self._api_token = "token", api_token
        return self

    def using_oauth(self, client_id: str, client_secret: str, urn: str) -> "DynatraceClient":
        if not client_id or not client_secret or not urn:
            raise ValueError("OAuth configuration is incomplete")
        self._auth_method = "oauth"
        self._oauth_client_id = client_id
        self._oauth_client_secret = client_secret
        self._oauth_urn = urn
        return self

    # Método de compatibilidade com o projeto original; o caminho otimizado usa
    # send_metric_lines para enviar várias linhas em conjunto.
    def send_mint_metric(
        self,
        mint_metric: object,
        proxies: Optional[Dict[str, str]],
    ) -> DeliveryStats:
        return self.send_metric_lines([str(mint_metric)], proxies=proxies)

    # Cada chunk é enviado de forma sequencial. Se um chunk posterior falhar, o
    # Connector Hub pode repetir o lote completo e duplicar chunks já aceitos.
    def send_metric_lines(
        self,
        lines: Sequence[str],
        proxies: Optional[Dict[str, str]] = None,
        deadline_monotonic: Optional[float] = None,
        request_id: Optional[str] = None,
    ) -> DeliveryStats:
        stats = DeliveryStats()
        for chunk_number, chunk in enumerate(
            _chunks(
                lines,
                self._options.max_payload_bytes,
                self._options.max_lines_per_request,
            ),
            1,
        ):
            payload = "\n".join(chunk).encode("utf-8")
            chunk_stats = self._send_chunk(
                payload,
                chunk,
                chunk_number,
                proxies,
                deadline_monotonic,
                request_id,
            )
            stats.chunks += 1
            stats.attempts += chunk_stats.attempts
            stats.retries += chunk_stats.retries
            stats.lines_ok += chunk_stats.lines_ok
            stats.lines_invalid += chunk_stats.lines_invalid
            stats.payload_bytes += len(payload)
            stats.http_total_ms += chunk_stats.http_total_ms
            stats.http_max_ms = max(stats.http_max_ms, chunk_stats.http_max_ms)
        return stats

    def _send_chunk(
        self,
        payload: bytes,
        lines: Sequence[str],
        chunk_number: int,
        proxies: Optional[Dict[str, str]],
        deadline: Optional[float],
        request_id: Optional[str],
    ) -> DeliveryStats:
        stats = DeliveryStats()
        # Mantém uma amostra limitada de metric keys para diagnóstico sem aumentar
        # excessivamente o volume dos logs. O limite atual é fixo em 20.
        keys = sorted({_key(line) for line in lines})[:20]

        # O retry interno é curto para não consumir todo o timeout da Function. Depois
        # do limite, a exceção é propagada para o Connector Hub.
        for attempt in range(1, self._options.max_attempts + 1):
            connect_timeout, read_timeout = self._timeouts(deadline)
            started = time.perf_counter()
            try:
                # A autorização pode renovar o token OAuth antes do POST. O tempo medido nesta
                # tentativa inclui essa renovação quando ela ocorre.
                response = _SESSION.post(
                    self._endpoint,
                    data=payload,
                    headers={
                        "Authorization": self._authorization(proxies, request_id),
                        "Content-Type": "text/plain; charset=utf-8",
                        "Accept": "application/json",
                    },
                    proxies=proxies,
                    timeout=(connect_timeout, read_timeout),
                )
                elapsed = round((time.perf_counter() - started) * 1000)
                stats.attempts += 1
                stats.http_total_ms += elapsed
                stats.http_max_ms = max(stats.http_max_ms, elapsed)
                body = self._json(response)

                _log(
                    logging.INFO,
                    "dynatrace_http",
                    request_id=request_id,
                    chunk=chunk_number,
                    attempt=attempt,
                    status=response.status_code,
                    lines=len(lines),
                    payload_bytes=len(payload),
                    elapsed_ms=elapsed,
                    lines_ok=body.get("linesOk") if isinstance(body, dict) else None,
                    lines_invalid=body.get("linesInvalid") if isinstance(body, dict) else None,
                    metric_keys=keys,
                )

                # HTTP 202 confirma que o Dynatrace aceitou o payload para processamento.
                if response.status_code == 202:
                    stats.lines_ok = self._int(body, "linesOk", len(lines))
                    stats.lines_invalid = self._int(body, "linesInvalid", 0)
                    return stats

                if response.status_code == 400:
                    # Em uma resposta HTTP 400, as linhas válidas já podem ter sido aceitas.
                    # Não reenvie o chunk inteiro, pois isso duplicaria os datapoints aceitos.
                    stats.lines_ok = self._int(body, "linesOk", 0)
                    stats.lines_invalid = self._int(body, "linesInvalid", len(lines))
                    self._log_invalid(body, lines, request_id, chunk_number)
                    return stats

                # Apenas códigos transitórios entram em retry. Outros códigos são classificados
                # como permanentes para evitar repetição sem possibilidade de recuperação.
                if response.status_code in TRANSIENT_STATUSES:
                    if attempt < self._options.max_attempts:
                        stats.retries += 1
                        self._backoff(attempt, response.headers.get("Retry-After"), deadline)
                        continue
                    raise DynatraceTransientError(
                        f"HTTP {response.status_code} after {attempt} attempts"
                    )

                raise DynatracePermanentError(
                    f"Non-retryable HTTP {response.status_code}: {response.text[:500]}"
                )

            # Classifica a fase provável da falha. A mensagem da exceção pode conter dados
            # do proxy; mantenha acesso ao log restrito enquanto não houver redaction.
            except (ConnectTimeout, ReadTimeout, ProxyError, SSLError, RequestsConnectionError) as exc:
                elapsed = round((time.perf_counter() - started) * 1000)
                stats.attempts += 1
                stats.http_total_ms += elapsed
                stats.http_max_ms = max(stats.http_max_ms, elapsed)
                phase = self._network_phase(exc)
                _log(
                    logging.ERROR,
                    "dynatrace_network_error",
                    request_id=request_id,
                    chunk=chunk_number,
                    attempt=attempt,
                    phase=phase,
                    elapsed_ms=elapsed,
                    error_type=type(exc).__name__,
                    error=str(exc)[:500],
                )
                if attempt < self._options.max_attempts:
                    stats.retries += 1
                    self._backoff(attempt, None, deadline)
                    continue
                raise DynatraceTransientError(
                    f"Network failure ({phase}) after {attempt} attempts"
                ) from exc

        raise DynatraceTransientError("Retry loop ended unexpectedly")

    def _authorization(
        self,
        proxies: Optional[Dict[str, str]],
        request_id: Optional[str],
    ) -> str:
        if self._auth_method == "token":
            return f"Api-Token {self._api_token}"
        if self._auth_method != "oauth":
            raise DynatracePermanentError("Authentication method is not configured")

        if self._oauth_token is None or time.time() >= self._oauth_expiration - 60:
            started = time.perf_counter()
            response = _SESSION.post(
                OAUTH_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._oauth_client_id,
                    "client_secret": self._oauth_client_secret,
                    "resource": self._oauth_urn,
                    "scope": "storage:metrics:write",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                proxies=proxies,
                timeout=(
                    self._options.connect_timeout_seconds,
                    self._options.read_timeout_seconds,
                ),
            )
            _log(
                logging.INFO,
                "dynatrace_oauth",
                request_id=request_id,
                status=response.status_code,
                elapsed_ms=round((time.perf_counter() - started) * 1000),
            )
            if response.status_code != 200:
                raise DynatracePermanentError(
                    f"OAuth failed with HTTP {response.status_code}: {response.text[:500]}"
                )
            body = response.json()
            self._oauth_token = body["access_token"]
            self._oauth_expiration = time.time() + int(body.get("expires_in", 300))
        return f"Bearer {self._oauth_token}"

    # Reduz os timeouts HTTP quando o deadline local está próximo. O cálculo usa o
    # deadline recebido do handler, não ctx.Deadline() do FDK.
    def _timeouts(self, deadline: Optional[float]) -> tuple[float, float]:
        if deadline is None:
            return self._options.connect_timeout_seconds, self._options.read_timeout_seconds
        usable = deadline - time.monotonic() - self._options.safety_margin_seconds
        if usable <= 1:
            raise DynatraceTransientError("Not enough Function time for another request")
        connect = min(self._options.connect_timeout_seconds, max(0.5, usable / 3))
        read = min(self._options.read_timeout_seconds, max(0.5, usable - connect))
        return connect, read

    # Usa Retry-After quando disponível; caso contrário aplica backoff exponencial
    # com jitter.
    def _backoff(self, attempt: int, retry_after: Optional[str], deadline: Optional[float]) -> None:
        try:
            delay = float(retry_after) if retry_after else None
        except ValueError:
            delay = None
        if delay is None:
            delay = self._options.backoff_base_seconds * (2 ** (attempt - 1))
            delay += random.uniform(0, self._options.backoff_base_seconds)
        if deadline and delay + self._options.safety_margin_seconds >= deadline - time.monotonic():
            raise DynatraceTransientError("Not enough Function time for retry")
        time.sleep(max(0.0, delay))

    @staticmethod
    def _json(response: requests.Response) -> object:
        try:
            return response.json()
        except ValueError:
            return {}

    @staticmethod
    def _int(body: object, field: str, default: int) -> int:
        try:
            return int(body.get(field, default)) if isinstance(body, dict) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _network_phase(exc: BaseException) -> str:
        if isinstance(exc, ConnectTimeout):
            return "connect_timeout"
        if isinstance(exc, ReadTimeout):
            return "read_timeout"
        if isinstance(exc, ProxyError):
            return "proxy"
        if isinstance(exc, SSLError):
            return "tls"
        return "connection"

    @staticmethod
    # A resposta do Dynatrace identifica linhas inválidas por número. O código
    # registra metric key e motivo, mas não mantém DLQ nem reenvia essas linhas.
    def _log_invalid(
        body: object,
        lines: Sequence[str],
        request_id: Optional[str],
        chunk_number: int,
    ) -> None:
        error = body.get("error", {}) if isinstance(body, dict) else {}
        invalid = error.get("invalidLines", []) if isinstance(error, dict) else []
        if not invalid:
            _log(logging.ERROR, "dynatrace_validation_error", request_id=request_id, response=body)
            return
        for item in invalid:
            line_number = item.get("line") if isinstance(item, dict) else None
            index = line_number - 1 if isinstance(line_number, int) else -1
            _log(
                logging.ERROR,
                "dynatrace_line_rejected",
                request_id=request_id,
                chunk=chunk_number,
                line=line_number,
                metric_key=_key(lines[index]) if 0 <= index < len(lines) else None,
                reason=item.get("error") if isinstance(item, dict) else None,
            )
