"""
Modulo de validacao de funcoes de agregacao.

Valida se a combinacao de agregacao e tipo de coluna e valida,
corrigindo automaticamente quando necessario (hibrido inteligente).
"""

import logging
from typing import Dict, Literal, Optional, Tuple

logger = logging.getLogger(__name__)


AggregationType = Literal["sum", "avg", "count", "count_distinct", "min", "max", "median", "std", "var"]
ColumnType = Literal["numeric", "categorical", "temporal"]


class AggregationValidator:
    """
    Valida e corrige combinacoes invalidas de agregacao e tipo de coluna.

    Estrategia Hibrida Inteligente:
    1. Respeitar agregacao se valida
    2. Corrigir silenciosamente se invalida
    3. Logar todas as mudancas para transparencia
    """

    # Agregacoes validas por tipo de coluna
    VALID_AGGREGATIONS: Dict[ColumnType, set] = {
        "numeric": {"sum", "avg", "count", "count_distinct", "min", "max", "median", "std", "var"},
        "categorical": {"count", "count_distinct"},
        "temporal": {"count", "count_distinct", "min", "max"}
    }

    # Agregacao padrao por tipo de coluna (para correcoes)
    DEFAULT_AGGREGATION: Dict[ColumnType, str] = {
        "numeric": "sum",
        "categorical": "count",
        "temporal": "count"
    }

    def __init__(self, strict_mode: bool = False):
        """
        Inicializa o validador.

        Args:
            strict_mode: Se True, levanta excecoes em vez de corrigir
        """
        self.strict_mode = strict_mode
        self.corrections_log: list = []

    def is_valid_combination(
        self,
        aggregation: str,
        column_type: ColumnType
    ) -> bool:
        """
        Verifica se a combinacao agregacao + tipo de coluna e valida.

        Args:
            aggregation: Funcao de agregacao (sum, count, etc.)
            column_type: Tipo da coluna (numeric, categorical, temporal)

        Returns:
            True se a combinacao e valida
        """
        aggregation_lower = aggregation.lower()
        valid_aggs = self.VALID_AGGREGATIONS.get(column_type, set())

        return aggregation_lower in valid_aggs

    def validate_and_correct(
        self,
        column_name: str,
        aggregation: str,
        column_type: ColumnType,
        context: Optional[str] = None
    ) -> Tuple[str, bool]:
        """
        Valida a agregacao e corrige se invalida.

        Args:
            column_name: Nome da coluna
            aggregation: Funcao de agregacao proposta
            column_type: Tipo da coluna
            context: Contexto adicional para logging

        Returns:
            Tupla (agregacao_final, foi_corrigida)
        """
        aggregation_lower = aggregation.lower()

        # Verifica se e valida
        if self.is_valid_combination(aggregation_lower, column_type):
            logger.debug(
                f"Agregacao valida: {aggregation_lower} para coluna '{column_name}' ({column_type})"
            )
            return aggregation_lower, False

        # Combinacao invalida
        logger.warning(
            f"Combinacao INVALIDA detectada: {aggregation_lower} para coluna "
            f"'{column_name}' ({column_type})"
        )

        if self.strict_mode:
            raise ValueError(
                f"Agregacao '{aggregation}' invalida para coluna '{column_name}' "
                f"do tipo '{column_type}'. Agregacoes validas: "
                f"{self.VALID_AGGREGATIONS.get(column_type)}"
            )

        # Corrige automaticamente
        corrected = self.DEFAULT_AGGREGATION[column_type]

        correction = {
            "column": column_name,
            "original": aggregation_lower,
            "corrected": corrected,
            "column_type": column_type,
            "context": context or "N/A"
        }
        self.corrections_log.append(correction)

        logger.warning(
            f"CORRECAO AUTOMATICA: '{column_name}' - {aggregation_lower} -> {corrected} "
            f"(tipo: {column_type})"
        )

        return corrected, True

    def validate_metric_spec(
        self,
        metric: Dict,
        column_type: ColumnType,
        auto_correct: bool = True
    ) -> Dict:
        """
        Valida e corrige uma especificacao de metrica completa.

        Args:
            metric: Especificacao da metrica (dict com name, aggregation, alias)
            column_type: Tipo da coluna
            auto_correct: Se True, corrige automaticamente

        Returns:
            Metrica corrigida (modificada in-place)
        """
        column_name = metric.get("name", "unknown")
        aggregation = metric.get("aggregation", "sum")

        if auto_correct:
            corrected_agg, was_corrected = self.validate_and_correct(
                column_name=column_name,
                aggregation=aggregation,
                column_type=column_type
            )
            metric["aggregation"] = corrected_agg

            if was_corrected:
                # Adiciona flag para rastreamento
                metric["_auto_corrected"] = True
                metric["_original_aggregation"] = aggregation
        else:
            # Apenas valida
            if not self.is_valid_combination(aggregation, column_type):
                logger.error(
                    f"Metrica invalida: {column_name} com {aggregation} (tipo: {column_type})"
                )

        return metric

    def get_corrections_summary(self) -> str:
        """
        Retorna um resumo das correcoes realizadas.

        Returns:
            String formatada com todas as correcoes
        """
        if not self.corrections_log:
            return "Nenhuma correcao de agregacao foi necessaria."

        summary = f"\n{'='*70}\n"
        summary += f"RESUMO DE CORRECOES DE AGREGACAO ({len(self.corrections_log)} correcoes)\n"
        summary += f"{'='*70}\n\n"

        for i, correction in enumerate(self.corrections_log, 1):
            summary += f"{i}. Coluna: {correction['column']}\n"
            summary += f"   Tipo: {correction['column_type']}\n"
            summary += f"   Original: {correction['original'].upper()}\n"
            summary += f"   Corrigido: {correction['corrected'].upper()}\n"
            summary += f"   Contexto: {correction['context']}\n"
            summary += "-" * 70 + "\n"

        return summary

    def clear_log(self):
        """Limpa o log de correcoes."""
        self.corrections_log = []

    def validate_chart_spec(
        self,
        chart_spec: Dict,
        column_types: Dict[str, ColumnType],
        auto_correct: bool = True
    ) -> Dict:
        """
        Valida e corrige todas as metricas de uma especificacao de grafico.

        Args:
            chart_spec: Especificacao completa do grafico
            column_types: Mapeamento coluna -> tipo
            auto_correct: Se True, corrige automaticamente

        Returns:
            chart_spec modificado (in-place)
        """
        metrics = chart_spec.get("metrics", [])

        for metric in metrics:
            column_name = metric.get("name")
            column_type = column_types.get(column_name)

            if not column_type:
                logger.warning(
                    f"Tipo da coluna '{column_name}' nao encontrado. Pulando validacao."
                )
                continue

            self.validate_metric_spec(metric, column_type, auto_correct)

        return chart_spec

    def suggest_alternative(
        self,
        column_name: str,
        invalid_aggregation: str,
        column_type: ColumnType
    ) -> str:
        """
        Sugere uma agregacao alternativa quando a original e invalida.

        Args:
            column_name: Nome da coluna
            invalid_aggregation: Agregacao invalida tentada
            column_type: Tipo da coluna

        Returns:
            Sugestao de agregacao alternativa
        """
        valid_options = self.VALID_AGGREGATIONS.get(column_type, set())

        # Tenta manter a semantica da agregacao original
        semantics_map = {
            "sum": ["avg", "count"],
            "avg": ["sum", "median"],
            "count": ["count_distinct"],
            "count_distinct": ["count"],
            "min": ["max"],
            "max": ["min"],
        }

        alternatives = semantics_map.get(invalid_aggregation.lower(), [])

        for alt in alternatives:
            if alt in valid_options:
                logger.info(
                    f"Sugestao: usar '{alt}' em vez de '{invalid_aggregation}' "
                    f"para coluna '{column_name}' ({column_type})"
                )
                return alt

        # Fallback: retorna agregacao padrao
        default = self.DEFAULT_AGGREGATION[column_type]
        logger.info(
            f"Sugestao: usar agregacao padrao '{default}' para coluna "
            f"'{column_name}' ({column_type})"
        )
        return default
