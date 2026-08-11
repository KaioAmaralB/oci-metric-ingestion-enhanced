"""Funções de agregação para datapoints do OCI Monitoring.

Derivado do projeto público de ingestão de métricas OCI da Dynatrace. Esta
versão valida os dados e aceita timestamps epoch ou ISO-8601.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Dict, Iterable, List, Mapping, Union


Number = Union[int, float]


@dataclass(frozen=True)
class AggregateResult:
    """Resultado agregado: timestamp do bucket e valor calculado."""
    timestamp: int
    value: float

@dataclass
class SummaryBucket:
    """Resumo ponderado dos datapoints pertencentes ao mesmo minuto UTC."""

    value_min: float
    value_max: float
    value_sum: float
    value_count: int

def timestamp_to_epoch_ms(value: object) -> int:
    """Converte um timestamp OCI para milissegundos desde o Unix epoch.

    Em ambientes existentes, o Connector Hub pode entregar valores epoch, enquanto
    exemplos públicos também utilizam ISO-8601. Aceitar ambos evita dependência de
    um único formato de serialização.
    """

    if isinstance(value, bool):
        raise ValueError("Boolean is not a valid timestamp")

    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"Non-finite timestamp: {value!r}")
        # Epoch atual em segundos está na ordem de 1e9; em milissegundos, na ordem de
        # 1e12. O limite abaixo diferencia os dois formatos numéricos.
        return int(numeric if abs(numeric) >= 100_000_000_000 else numeric * 1000)

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise ValueError("Empty timestamp")

        try:
            return timestamp_to_epoch_ms(float(raw))
        except ValueError:
            pass

        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Unsupported timestamp format: {value!r}") from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)

    raise ValueError(f"Unsupported timestamp type: {type(value).__name__}")

# Rejeita booleanos, valores não numéricos, NaN e infinito antes da agregação.
def _validated_value(point: Mapping[str, object]) -> float:
    if "value" not in point:
        raise ValueError("Datapoint does not contain 'value'")

    value = point["value"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Datapoint value is not numeric: {value!r}")

    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"Datapoint value is not finite: {value!r}")
    return numeric

def _validated_count(point: Mapping[str, object]) -> int:
    """Retorna quantas ocorrências o datapoint representa.

    O OCI Monitoring pode consolidar várias ocorrências do mesmo valor em um
    único datapoint. Quando o campo count não está presente, seu valor
    semântico é 1.
    """

    raw_count = point.get("count", 1)

    if isinstance(raw_count, bool) or not isinstance(
        raw_count,
        (int, float),
    ):
        raise ValueError(
            f"Datapoint count is not numeric: {raw_count!r}"
        )

    numeric_count = float(raw_count)

    if (
        not math.isfinite(numeric_count)
        or numeric_count <= 0
        or not numeric_count.is_integer()
    ):
        raise ValueError(
            f"Datapoint count must be a positive integer: "
            f"{raw_count!r}"
        )

    return int(numeric_count)

def create_minutely_summary_buckets(
    datapoints: Iterable[Mapping[str, object]],
) -> Dict[int, SummaryBucket]:
    """Agrupa datapoints por minuto preservando value e count do OCI.

    Para cada datapoint:

        soma representada = value * count

    O min e o max continuam sendo calculados a partir do value. A soma e a
    quantidade de medições são ponderadas pelo count informado pelo OCI.
    """

    buckets: Dict[int, SummaryBucket] = {}

    for point in datapoints:
        if "timestamp" not in point:
            raise ValueError(
                "Datapoint does not contain 'timestamp'"
            )

        timestamp_ms = timestamp_to_epoch_ms(
            point["timestamp"]
        )

        minute_bucket = (
            timestamp_ms // 60_000
        ) * 60

        value = _validated_value(point)
        occurrence_count = _validated_count(point)

        weighted_value = value * occurrence_count

        bucket = buckets.get(minute_bucket)

        if bucket is None:
            buckets[minute_bucket] = SummaryBucket(
                value_min=value,
                value_max=value,
                value_sum=weighted_value,
                value_count=occurrence_count,
            )
            continue

        bucket.value_min = min(
            bucket.value_min,
            value,
        )

        bucket.value_max = max(
            bucket.value_max,
            value,
        )

        bucket.value_sum += weighted_value
        bucket.value_count += occurrence_count

    return buckets

# Funções mantidas separadamente por compatibilidade com metric_mapping.py.
def aggregate_max(
    datapoints: List[Mapping[str, object]],
) -> List[AggregateResult]:
    """Retorna o maior valor observado em cada minuto UTC."""

    return [
        AggregateResult(
            timestamp,
            bucket.value_max,
        )
        for timestamp, bucket in sorted(
            create_minutely_summary_buckets(
                datapoints
            ).items()
        )
    ]


def aggregate_min(
    datapoints: List[Mapping[str, object]],
) -> List[AggregateResult]:
    """Retorna o menor valor observado em cada minuto UTC."""

    return [
        AggregateResult(
            timestamp,
            bucket.value_min,
        )
        for timestamp, bucket in sorted(
            create_minutely_summary_buckets(
                datapoints
            ).items()
        )
    ]


def aggregate_sum(
    datapoints: List[Mapping[str, object]],
) -> List[AggregateResult]:
    """Retorna a soma ponderada das ocorrências de cada minuto UTC."""

    return [
        AggregateResult(
            timestamp,
            bucket.value_sum,
        )
        for timestamp, bucket in sorted(
            create_minutely_summary_buckets(
                datapoints
            ).items()
        )
    ]


def aggregate_mean(
    datapoints: List[Mapping[str, object]],
) -> List[AggregateResult]:
    """Retorna a média ponderada das ocorrências de cada minuto UTC."""

    return [
        AggregateResult(
            timestamp,
            bucket.value_sum
            / bucket.value_count,
        )
        for timestamp, bucket in sorted(
            create_minutely_summary_buckets(
                datapoints
            ).items()
        )
        if bucket.value_count > 0
    ]
