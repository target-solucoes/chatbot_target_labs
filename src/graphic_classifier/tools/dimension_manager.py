"""
Dimension Manager - FASE 2: Correcao de Dimensoes.

Este modulo implementa a hierarquia de dimensoes (primary/series) e detectores
de dimensoes temporais conforme especificado em planning_graph_classifier_diagnosis.md
- FASE 2, Etapa 2.2.

O dimension_manager garante que:
1. Dimensoes sejam corretamente classificadas como primary vs series
2. Dimensoes temporais sejam identificadas automaticamente
3. Estrutura de dimensoes seja validada para cada tipo de grafico
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# =============================================================================
# DIMENSION TYPES
# =============================================================================

@dataclass
class DimensionInfo:
    """
    Informacao sobre uma dimensao.

    Attributes:
        name: Nome da coluna
        is_temporal: Se a dimensao e temporal
        granularity: Granularidade temporal (dia, mes, ano, etc.)
        role: Papel da dimensao (primary, series, filter)
    """
    name: str
    is_temporal: bool = False
    granularity: Optional[str] = None
    role: Optional[str] = None  # "primary", "series", "filter"


# =============================================================================
# TEMPORAL DIMENSION DETECTOR
# =============================================================================

class TemporalDimensionDetector:
    """
    Detector de dimensoes temporais.

    Este detector identifica automaticamente se uma dimensao e temporal
    e qual e sua granularidade.
    """

    # Mapeamento de nomes de colunas para granularidade
    # IMPORTANTE: Ordem importa! Patterns mais especificos primeiro
    TEMPORAL_COLUMN_PATTERNS = [
        # Trimestre (antes de "mes" para evitar match parcial)
        ("quarter", "quarter"),
        ("trimestre", "quarter"),
        ("q1", "quarter"),
        ("q2", "quarter"),
        ("q3", "quarter"),
        ("q4", "quarter"),
        # Semestre
        ("semester", "semester"),
        ("semestre", "semester"),
        ("s1", "semester"),
        ("s2", "semester"),
        # Mes (depois de trimestre/semestre)
        ("month", "month"),
        ("mes", "month"),
        ("mês", "month"),
        # Dia
        ("day", "day"),
        ("dia", "day"),
        ("date", "day"),
        ("data", "day"),
        # Ano
        ("year", "year"),
        ("ano", "year"),
    ]

    @staticmethod
    def is_temporal(column_name: str) -> bool:
        """
        Verifica se uma coluna e temporal.

        Args:
            column_name: Nome da coluna

        Returns:
            True se a coluna e temporal, False caso contrario
        """
        if not column_name:
            return False

        column_lower = column_name.lower()

        # Verificar patterns exatos
        for pattern, _ in TemporalDimensionDetector.TEMPORAL_COLUMN_PATTERNS:
            if pattern in column_lower:
                return True

        # Verificar patterns adicionais (data formatada)
        date_patterns = [
            r"\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
            r"\d{2}/\d{2}/\d{4}",  # DD/MM/YYYY
            r"\d{4}/\d{2}",  # YYYY/MM
        ]

        for pattern in date_patterns:
            if re.search(pattern, column_name):
                return True

        return False

    @staticmethod
    def get_granularity(column_name: str) -> Optional[str]:
        """
        Retorna a granularidade temporal de uma coluna.

        Args:
            column_name: Nome da coluna

        Returns:
            Granularidade (day, month, quarter, semester, year) ou None
        """
        if not column_name:
            return None

        column_lower = column_name.lower()

        # Verificar patterns exatos (ordem importa!)
        for pattern, granularity in TemporalDimensionDetector.TEMPORAL_COLUMN_PATTERNS:
            if pattern in column_lower:
                return granularity

        # Se contem "data" ou "date", assumir dia
        if "data" in column_lower or "date" in column_lower:
            return "day"

        return None

    @staticmethod
    def analyze(column_name: str) -> DimensionInfo:
        """
        Analisa uma coluna e retorna informacoes sobre ela.

        Args:
            column_name: Nome da coluna

        Returns:
            DimensionInfo com is_temporal e granularity preenchidos
        """
        is_temporal = TemporalDimensionDetector.is_temporal(column_name)
        granularity = TemporalDimensionDetector.get_granularity(column_name) if is_temporal else None

        return DimensionInfo(
            name=column_name,
            is_temporal=is_temporal,
            granularity=granularity
        )


# =============================================================================
# DIMENSION HIERARCHY MANAGER
# =============================================================================

class DimensionHierarchyManager:
    """
    Gerenciador de hierarquia de dimensoes.

    Este gerenciador determina qual dimensao deve ser "primary" (eixo X/principal)
    e qual deve ser "series" (cor/agrupamento) baseado no tipo de grafico e no intent.
    """

    @staticmethod
    def assign_roles(
        dimensions: List[str],
        chart_type: str,
        intent_config: Optional[Dict] = None
    ) -> Dict[str, DimensionInfo]:
        """
        Atribui roles (primary/series) para cada dimensao.

        Args:
            dimensions: Lista de nomes de colunas das dimensoes
            chart_type: Tipo de grafico
            intent_config: Configuracao do intent (opcional)

        Returns:
            Dicionario mapeando nome da dimensao para DimensionInfo com role atribuido
        """
        if not dimensions:
            return {}

        # Analisar cada dimensao
        dimension_infos = {}
        for dim_name in dimensions:
            dim_info = TemporalDimensionDetector.analyze(dim_name)
            dimension_infos[dim_name] = dim_info

        # Aplicar regras de hierarquia baseadas no chart_type
        if chart_type == "bar_vertical_composed":
            return DimensionHierarchyManager._assign_for_composed(
                dimension_infos, intent_config
            )
        elif chart_type == "bar_vertical_stacked":
            return DimensionHierarchyManager._assign_for_stacked(
                dimension_infos, intent_config
            )
        elif chart_type == "line":
            return DimensionHierarchyManager._assign_for_line(
                dimension_infos, intent_config
            )
        elif chart_type == "line_composed":
            return DimensionHierarchyManager._assign_for_line_composed(
                dimension_infos, intent_config
            )
        else:
            # Para outros chart types, primeira dimensao e primary
            return DimensionHierarchyManager._assign_simple(dimension_infos)

    @staticmethod
    def _assign_for_composed(
        dimension_infos: Dict[str, DimensionInfo],
        intent_config: Optional[Dict] = None
    ) -> Dict[str, DimensionInfo]:
        """
        Atribui roles para bar_vertical_composed.

        Regra (FASE 2, planning_graph_classifier_diagnosis.md):
        - primary_dimension = entity (Produto, Cliente, etc.)
        - series_dimension = temporal (Mes, Trimestre, etc.)
        """
        if len(dimension_infos) != 2:
            logger.warning(
                f"[DimensionHierarchyManager] bar_vertical_composed requires 2 dimensions, "
                f"got {len(dimension_infos)}"
            )

        # Separar temporal vs entity dimensions
        temporal_dims = [name for name, info in dimension_infos.items() if info.is_temporal]
        entity_dims = [name for name, info in dimension_infos.items() if not info.is_temporal]

        # Atribuir roles
        if len(temporal_dims) >= 1 and len(entity_dims) >= 1:
            # Caso ideal: 1 temporal + 1 entity
            entity_dims[0]
            dimension_infos[entity_dims[0]].role = "primary"
            dimension_infos[temporal_dims[0]].role = "series"

            logger.debug(
                f"[DimensionHierarchyManager] bar_vertical_composed: "
                f"primary={entity_dims[0]}, series={temporal_dims[0]}"
            )
        elif len(temporal_dims) == 2:
            # Caso: 2 temporais (ex: Mes vs Trimestre)
            # Primary = granularidade mais fina
            dim1, dim2 = list(dimension_infos.keys())
            gran1 = dimension_infos[dim1].granularity
            gran2 = dimension_infos[dim2].granularity

            granularity_order = ["day", "month", "quarter", "semester", "year"]
            if granularity_order.index(gran1) < granularity_order.index(gran2):
                dimension_infos[dim1].role = "series"
                dimension_infos[dim2].role = "primary"
            else:
                dimension_infos[dim1].role = "primary"
                dimension_infos[dim2].role = "series"

            logger.debug(
                f"[DimensionHierarchyManager] bar_vertical_composed (2 temporal): "
                f"primary={dimension_infos[dim1].role}, series={dimension_infos[dim2].role}"
            )
        else:
            # Caso: 2 entities (nao-temporal)
            # Primary = primeira dimensao
            dims_list = list(dimension_infos.keys())
            dimension_infos[dims_list[0]].role = "primary"
            if len(dims_list) > 1:
                dimension_infos[dims_list[1]].role = "series"

            logger.debug(
                f"[DimensionHierarchyManager] bar_vertical_composed (2 entity): "
                f"primary={dims_list[0]}, series={dims_list[1] if len(dims_list) > 1 else 'none'}"
            )

        return dimension_infos

    @staticmethod
    def _assign_for_stacked(
        dimension_infos: Dict[str, DimensionInfo],
        intent_config: Optional[Dict] = None
    ) -> Dict[str, DimensionInfo]:
        """
        Atribui roles para bar_vertical_stacked.

        Regra:
        - primary = dimensao do eixo X (geralmente temporal ou outer entity)
        - series = dimensao de quebra (inner entity)

        FASE 7: Suporta nested ranking com ordem semantica de dimensoes.
        Para "top N X dos M Y", Y e primary (eixo X) e X e series (stack/hue).
        """
        if len(dimension_infos) != 2:
            logger.warning(
                f"[DimensionHierarchyManager] bar_vertical_stacked requires 2 dimensions, "
                f"got {len(dimension_infos)}"
            )

        # Se tiver dimension_structure no intent_config, usar
        if intent_config and "dimension_structure" in intent_config:
            dim_structure = intent_config["dimension_structure"]

            # FASE 7: Verificar se ha ordered_dimensions do nested ranking
            ordered_dims = dim_structure.get("ordered_dimensions")
            if ordered_dims and len(ordered_dims) >= 2:
                group_col = ordered_dims[0]    # X-axis (grupo principal)
                subgroup_col = ordered_dims[1]  # Stack (subgrupo)

                if group_col in dimension_infos:
                    dimension_infos[group_col].role = "primary"
                if subgroup_col in dimension_infos:
                    dimension_infos[subgroup_col].role = "series"

                logger.info(
                    f"[DimensionHierarchyManager] NESTED RANKING roles: "
                    f"primary={group_col}, series={subgroup_col}"
                )
                return dimension_infos

            if dim_structure.get("primary") == "temporal":
                # Primary e temporal
                temporal_dims = [name for name, info in dimension_infos.items() if info.is_temporal]
                entity_dims = [name for name, info in dimension_infos.items() if not info.is_temporal]

                if temporal_dims and entity_dims:
                    dimension_infos[temporal_dims[0]].role = "primary"
                    dimension_infos[entity_dims[0]].role = "series"
                    return dimension_infos

        # Padrao: primeira = primary, segunda = series
        dims_list = list(dimension_infos.keys())
        if len(dims_list) >= 1:
            dimension_infos[dims_list[0]].role = "primary"
        if len(dims_list) >= 2:
            dimension_infos[dims_list[1]].role = "series"

        return dimension_infos

    @staticmethod
    def _assign_for_line(
        dimension_infos: Dict[str, DimensionInfo],
        intent_config: Optional[Dict] = None
    ) -> Dict[str, DimensionInfo]:
        """
        Atribui roles para line chart.

        Regra:
        - primary = dimensao temporal (eixo X)
        """
        if len(dimension_infos) != 1:
            logger.warning(
                f"[DimensionHierarchyManager] line requires 1 dimension, "
                f"got {len(dimension_infos)}"
            )

        # A unica dimensao e primary
        if dimension_infos:
            first_dim = list(dimension_infos.keys())[0]
            dimension_infos[first_dim].role = "primary"

        return dimension_infos

    @staticmethod
    def _assign_for_line_composed(
        dimension_infos: Dict[str, DimensionInfo],
        intent_config: Optional[Dict] = None
    ) -> Dict[str, DimensionInfo]:
        """
        Atribui roles para line_composed.

        Regra:
        - primary = dimensao temporal (eixo X)
        - series = dimensao categorica (diferentes linhas)
        """
        if len(dimension_infos) != 2:
            logger.warning(
                f"[DimensionHierarchyManager] line_composed requires 2 dimensions, "
                f"got {len(dimension_infos)}"
            )

        # Separar temporal vs entity
        temporal_dims = [name for name, info in dimension_infos.items() if info.is_temporal]
        entity_dims = [name for name, info in dimension_infos.items() if not info.is_temporal]

        if temporal_dims and entity_dims:
            dimension_infos[temporal_dims[0]].role = "primary"
            dimension_infos[entity_dims[0]].role = "series"
        else:
            # Padrao: primeira = primary, segunda = series
            dims_list = list(dimension_infos.keys())
            if len(dims_list) >= 1:
                dimension_infos[dims_list[0]].role = "primary"
            if len(dims_list) >= 2:
                dimension_infos[dims_list[1]].role = "series"

        return dimension_infos

    @staticmethod
    def _assign_simple(
        dimension_infos: Dict[str, DimensionInfo]
    ) -> Dict[str, DimensionInfo]:
        """
        Atribui roles simples (primeira = primary).

        Usado para chart types que tem 1 dimensao (bar_horizontal, bar_vertical, pie).
        """
        if dimension_infos:
            first_dim = list(dimension_infos.keys())[0]
            dimension_infos[first_dim].role = "primary"

        return dimension_infos


# =============================================================================
# DIMENSION STRUCTURE VALIDATOR
# =============================================================================

class DimensionStructureValidator:
    """
    Validador de estrutura de dimensoes para graficos compostos.

    Este validador verifica se a estrutura de dimensoes e apropriada
    para o tipo de grafico especificado.
    """

    @staticmethod
    def validate(
        dimensions: List[str],
        chart_type: str,
        intent_config: Optional[Dict] = None
    ) -> Tuple[bool, List[str]]:
        """
        Valida estrutura de dimensoes.

        Args:
            dimensions: Lista de nomes de colunas das dimensoes
            chart_type: Tipo de grafico
            intent_config: Configuracao do intent (opcional)

        Returns:
            (is_valid, errors) - tupla com flag de validacao e lista de erros
        """
        errors = []

        # Analisar dimensoes
        dimension_infos = {}
        for dim_name in dimensions:
            dim_info = TemporalDimensionDetector.analyze(dim_name)
            dimension_infos[dim_name] = dim_info

        # Validacoes especificas por chart_type
        if chart_type == "bar_vertical_composed":
            errors.extend(
                DimensionStructureValidator._validate_composed(dimension_infos, intent_config)
            )
        elif chart_type == "bar_vertical_stacked":
            errors.extend(
                DimensionStructureValidator._validate_stacked(dimension_infos, intent_config)
            )
        elif chart_type == "line":
            errors.extend(
                DimensionStructureValidator._validate_line(dimension_infos)
            )
        elif chart_type == "line_composed":
            errors.extend(
                DimensionStructureValidator._validate_line_composed(dimension_infos)
            )

        is_valid = len(errors) == 0

        if not is_valid:
            logger.warning(
                f"[DimensionStructureValidator] Validation failed for {chart_type}: {errors}"
            )

        return is_valid, errors

    @staticmethod
    def _validate_composed(
        dimension_infos: Dict[str, DimensionInfo],
        intent_config: Optional[Dict]
    ) -> List[str]:
        """Valida estrutura para bar_vertical_composed."""
        errors = []

        if len(dimension_infos) != 2:
            errors.append(
                f"bar_vertical_composed requires exactly 2 dimensions, got {len(dimension_infos)}"
            )
            return errors

        # Verificar se tem pelo menos 1 dimensao temporal
        temporal_dims = [name for name, info in dimension_infos.items() if info.is_temporal]

        if len(temporal_dims) == 0:
            errors.append(
                "bar_vertical_composed should have at least 1 temporal dimension for comparison"
            )

        return errors

    @staticmethod
    def _validate_stacked(
        dimension_infos: Dict[str, DimensionInfo],
        intent_config: Optional[Dict]
    ) -> List[str]:
        """Valida estrutura para bar_vertical_stacked."""
        errors = []

        if len(dimension_infos) != 2:
            errors.append(
                f"bar_vertical_stacked requires exactly 2 dimensions, got {len(dimension_infos)}"
            )

        return errors

    @staticmethod
    def _validate_line(dimension_infos: Dict[str, DimensionInfo]) -> List[str]:
        """Valida estrutura para line."""
        errors = []

        if len(dimension_infos) != 1:
            errors.append(
                f"line requires exactly 1 dimension, got {len(dimension_infos)}"
            )
            return errors

        # Verificar que a dimensao e temporal
        first_dim_info = list(dimension_infos.values())[0]
        if not first_dim_info.is_temporal:
            errors.append(
                f"line chart requires a temporal dimension, got: {first_dim_info.name}"
            )

        return errors

    @staticmethod
    def _validate_line_composed(dimension_infos: Dict[str, DimensionInfo]) -> List[str]:
        """Valida estrutura para line_composed."""
        errors = []

        if len(dimension_infos) != 2:
            errors.append(
                f"line_composed requires exactly 2 dimensions, got {len(dimension_infos)}"
            )
            return errors

        # Verificar que primeira dimensao e temporal
        first_dim_info = list(dimension_infos.values())[0]
        if not first_dim_info.is_temporal:
            errors.append(
                f"line_composed requires first dimension to be temporal, got: {first_dim_info.name}"
            )

        return errors


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def analyze_dimensions(
    dimensions: List[str],
    chart_type: str,
    intent_config: Optional[Dict] = None
) -> Dict[str, DimensionInfo]:
    """
    Analisa dimensoes e atribui roles.

    Args:
        dimensions: Lista de nomes de colunas das dimensoes
        chart_type: Tipo de grafico
        intent_config: Configuracao do intent (opcional)

    Returns:
        Dicionario mapeando nome da dimensao para DimensionInfo
    """
    return DimensionHierarchyManager.assign_roles(dimensions, chart_type, intent_config)


def validate_dimension_structure(
    dimensions: List[str],
    chart_type: str,
    intent_config: Optional[Dict] = None
) -> Tuple[bool, List[str]]:
    """
    Valida estrutura de dimensoes.

    Args:
        dimensions: Lista de nomes de colunas das dimensoes
        chart_type: Tipo de grafico
        intent_config: Configuracao do intent (opcional)

    Returns:
        (is_valid, errors)
    """
    return DimensionStructureValidator.validate(dimensions, chart_type, intent_config)
