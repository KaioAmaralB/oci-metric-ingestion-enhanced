"""Representação do resumo estatístico de uma métrica gauge no Dynatrace."""

from __future__ import annotations


class SummaryStat:
    """Armazena min, max, soma e quantidade de amostras de um bucket."""
    def __init__(
        self,
        value_min: float,
        value_max: float,
        value_sum: float,
        value_count: float,
    ) -> None:
        self.value_min = value_min
        self.value_max = value_max
        self.value_sum = value_sum
        self.value_count = value_count

    # Gera exatamente o trecho esperado pelo payload gauge do protocolo MINT.
    def __str__(self) -> str:
        return (
            f"min={self.value_min},max={self.value_max},"
            f"sum={self.value_sum},count={self.value_count}"
        )
