"""Serialização para o line protocol da Metrics API v2 do Dynatrace.

O nome da classe e o construtor permanecem compatíveis com o projeto original.
A implementação remove dimensões vazias, ordena as chaves e escapa os valores.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional, Union

from summary_stat import SummaryStat

# Escapa caracteres especiais permitidos no valor de dimensão do protocolo MINT.
def _escape_dimension_value(value: object) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


class MintMetric(str):
    """Representa uma linha MINT completa como uma subclasse imutável de str."""
    def __new__(
        cls,
        key: str,
        value: Union[float, SummaryStat],
        dimensions: Optional[Dict[str, object]] = None,
        time: Optional[int] = None,
    ) -> "MintMetric":
        if not key or not isinstance(key, str):
            raise ValueError("Metric key must be a non-empty string")
        if any(character in key for character in (" ", "\t", "\r", "\n", ",")):
            raise ValueError(f"Metric key contains an invalid separator: {key!r}")

        # Normaliza chaves para minúsculas e remove dimensões sem valor. A ordenação
        # determinística facilita deduplicação e comparação de linhas.
        clean_dimensions = {
            str(k).lower(): _escape_dimension_value(v)
            for k, v in (dimensions or {}).items()
            if v is not None and str(v) != ""
        }
        dimensions_string = ",".join(
            f'{key_name}="{clean_dimensions[key_name]}"'
            for key_name in sorted(clean_dimensions)
        )
        
        # O horário atual é apenas fallback. No fluxo do Connector Hub, o timestamp da
        # origem deve ser informado para preservar o instante real do datapoint.
        timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        if time is not None:
            timestamp = int(time)

        # O payload é sempre enviado como gauge. SummaryStat produz min/max/sum/count;
        # valores escalares são enviados como gauge simples.
        return super().__new__(
            cls,
            f"{key}{',' if dimensions_string else ''}{dimensions_string} "
            f"gauge,{value} {timestamp}",
        )
