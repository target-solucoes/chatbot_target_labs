"""
Configuração centralizada para parâmetros de visualização.

Este módulo define limites e parâmetros de visualização aplicados
de forma consistente em todos os tipos de gráficos do sistema.

Características:
- Limite máximo de categorias configurável
- Parâmetros específicos por tipo de gráfico
- Fácil modificação sem alterações no código
"""

from dataclasses import dataclass
from typing import Dict, Optional
import os


@dataclass
class VisualizationConfig:
    """
    Configuração global para visualizações.

    Attributes:
        max_categories: Limite máximo de categorias exibidas em gráficos.
                       Categorias além deste limite são agregadas em "OUTROS"
                       para gráficos de pizza, ou simplesmente excluídas para
                       gráficos de barras.

        enable_others_category: Se True, cria categoria "OUTROS" em gráficos
                               de pizza. Se False, apenas limita categorias.

        others_label: Nome da categoria agregada (padrão: "OUTROS")

    Exemplo:
        >>> config = VisualizationConfig()
        >>> print(config.max_categories)
        15
        >>> config.max_categories = 20  # Alterar dinamicamente
    """

    # Limite máximo de categorias exibidas
    max_categories: int = 15

    # Habilitar categoria "OUTROS" para agregação
    enable_others_category: bool = True

    # Label da categoria agregada
    others_label: str = "OUTROS"

    def __post_init__(self):
        """Valida e carrega overrides de variáveis de ambiente."""
        # Permitir override via variável de ambiente
        env_max_categories = os.getenv("VIZ_MAX_CATEGORIES")
        if env_max_categories:
            try:
                self.max_categories = int(env_max_categories)
            except ValueError:
                pass  # Ignora valor inválido e mantém padrão

        # Validar limite mínimo
        if self.max_categories < 1:
            raise ValueError(
                f"max_categories deve ser >= 1, recebeu {self.max_categories}"
            )

        # Validar limite máximo razoável
        if self.max_categories > 100:
            raise ValueError(
                f"max_categories muito alto ({self.max_categories}), "
                f"limite máximo recomendado: 100"
            )

    def get_limit_for_chart_type(self, chart_type: str) -> int:
        """
        Retorna o limite de categorias para um tipo específico de gráfico.

        Por padrão, todos os tipos usam max_categories, mas este método
        permite customização por tipo de gráfico quando necessário.

        Args:
            chart_type: Tipo de gráfico (ex: "bar_horizontal", "pie", etc)

        Returns:
            Número máximo de categorias para este tipo de gráfico

        Exemplo:
            >>> config = VisualizationConfig(max_categories=15)
            >>> config.get_limit_for_chart_type("bar_horizontal")
            15
            >>> config.get_limit_for_chart_type("pie")
            10
            >>> config.get_limit_for_chart_type("bar_vertical_composed")
            5
        """
        # Override específico para bar_vertical_composed
        # Comparações temporais funcionam melhor com menos categorias
        if chart_type == "bar_vertical_composed":
            return 5

        # Override específico para pie charts
        # Gráficos de pizza funcionam melhor com Top 10 + OUTROS
        # para melhor legibilidade e visualização de proporções
        if chart_type == "pie":
            return 10

        # Outros tipos usam padrão global
        return self.max_categories

    def should_create_others_category(self, chart_type: str) -> bool:
        """
        Determina se deve criar categoria "OUTROS" para o tipo de gráfico.

        Atualmente, apenas gráficos de pizza (pie) agregam em "OUTROS".
        Outros tipos apenas limitam o número de categorias.

        Args:
            chart_type: Tipo de gráfico

        Returns:
            True se deve criar categoria "OUTROS", False caso contrário

        Exemplo:
            >>> config = VisualizationConfig()
            >>> config.should_create_others_category("pie")
            True
            >>> config.should_create_others_category("bar_horizontal")
            False
        """
        # Apenas pie charts agregam em "OUTROS"
        # Outros tipos simplesmente mostram top N
        return self.enable_others_category and chart_type == "pie"


# ============================================================================
# CONFIGURAÇÃO GLOBAL (Singleton)
# ============================================================================

# Usar um dicionário mutável em vez de uma variável global simples
# Isso evita problemas com o Python resetando a variável em re-imports
_CONFIG_STORE = {"config": None}


def get_visualization_config() -> VisualizationConfig:
    """
    Retorna a configuração global de visualização (singleton).

    Esta função garante que apenas uma instância da configuração
    exista em todo o sistema, permitindo modificações centralizadas.

    Returns:
        Instância global de VisualizationConfig

    Exemplo:
        >>> config = get_visualization_config()
        >>> config.max_categories = 20  # Afeta todo o sistema
        >>> config2 = get_visualization_config()
        >>> assert config is config2  # Mesma instância
    """
    if _CONFIG_STORE["config"] is None:
        _CONFIG_STORE["config"] = VisualizationConfig()
    return _CONFIG_STORE["config"]


def set_visualization_config(config: VisualizationConfig) -> None:
    """
    Define uma nova configuração global de visualização.

    Use esta função para substituir a configuração padrão por uma
    customizada, tipicamente em testes ou configurações específicas.

    Args:
        config: Nova instância de VisualizationConfig

    Exemplo:
        >>> custom_config = VisualizationConfig(max_categories=10)
        >>> set_visualization_config(custom_config)
    """
    _CONFIG_STORE["config"] = config


def reset_visualization_config() -> None:
    """
    Reseta a configuração global para os valores padrão.

    Útil principalmente em testes para garantir estado limpo.

    Exemplo:
        >>> reset_visualization_config()
        >>> config = get_visualization_config()
        >>> assert config.max_categories == 15  # Valor padrão
    """
    _CONFIG_STORE["config"] = None


# ============================================================================
# CONFIGURAÇÕES ESPECÍFICAS POR TIPO DE GRÁFICO
# ============================================================================


@dataclass
class ChartTypeConfig:
    """
    Configuração específica para um tipo de gráfico.

    Permite definir comportamentos diferentes por tipo de gráfico,
    se necessário no futuro.
    """

    chart_type: str
    max_categories: Optional[int] = None  # None = usar padrão global
    enable_others: Optional[bool] = None  # None = usar padrão global

    def get_effective_max_categories(self, global_config: VisualizationConfig) -> int:
        """Retorna o limite efetivo, considerando override local."""
        return self.max_categories or global_config.max_categories

    def get_effective_enable_others(self, global_config: VisualizationConfig) -> bool:
        """Retorna se deve criar OUTROS, considerando override local."""
        if self.enable_others is not None:
            return self.enable_others
        return global_config.should_create_others_category(self.chart_type)


# ============================================================================
# CONSTANTES E EXPORTS
# ============================================================================

# Valor padrão do limite de categorias (para referência)
DEFAULT_MAX_CATEGORIES = 15

# Label padrão da categoria agregada
DEFAULT_OTHERS_LABEL = "OUTROS"


__all__ = [
    "VisualizationConfig",
    "ChartTypeConfig",
    "get_visualization_config",
    "set_visualization_config",
    "reset_visualization_config",
    "DEFAULT_MAX_CATEGORIES",
    "DEFAULT_OTHERS_LABEL",
]
