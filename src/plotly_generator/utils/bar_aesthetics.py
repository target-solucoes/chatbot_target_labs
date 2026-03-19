"""
BarAesthetics - Configurações estéticas centralizadas para gráficos de barras.

Este módulo centraliza todas as configurações visuais estéticas para
gráficos de barras (horizontais e verticais), garantindo consistência
e facilidade de manutenção.

Principais recursos:
- Tamanho de fonte otimizado para rótulos categóricos
- Espaçamento adequado entre rótulos e barras
- Margens configuráveis
- Configuração de padding e gap
"""

from typing import Dict, Any, Optional
import plotly.graph_objects as go

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class BarAestheticsConfig:
    """
    Configurações estéticas para gráficos de barras.

    Todas as configurações podem ser ajustadas centralmente aqui.
    """

    # ==================== CONFIGURAÇÕES DE FONTE ====================

    # Tamanho de fonte para rótulos categóricos (eixo Y em horizontal, eixo X em vertical)
    CATEGORY_LABEL_FONT_SIZE = 11  # Base font size (adaptive logic may override)

    # Tamanho de fonte para valores numéricos (eixo X em horizontal, eixo Y em vertical)
    VALUE_LABEL_FONT_SIZE = 13  # Aumentado de 12 para 13

    # Tamanho de fonte para valores nas barras (quando show_values=True)
    BAR_VALUE_FONT_SIZE = 13  # Aumentado de 11 para 13

    # ==================== CONFIGURAÇÕES DE ESPAÇAMENTO ====================

    # Margem entre rótulos e barras (em pixels)
    # Para barras horizontais: margem esquerda (antes dos rótulos Y)
    # Para barras verticais: margem inferior (antes dos rótulos X)
    LABEL_TO_BAR_GAP = 30  # Aumentado para afastar rótulos das barras

    # Margem base para o eixo categórico
    CATEGORY_AXIS_BASE_MARGIN = (
        30  # Reduzido de 150 para 120 para reduzir espaço branco
    )

    # Fator multiplicador para cálculo dinâmico de margem baseado no tamanho do texto
    MARGIN_CHAR_MULTIPLIER = 8  # Pixels por caractere

    # ==================== CONFIGURAÇÕES DE BARRAS ====================

    # Gap entre barras (0.0 = sem gap, 1.0 = gap máximo)
    BAR_GAP = 0.2  # Gap padrão de 20%

    # Gap entre grupos de barras (para gráficos compostos)
    BAR_GROUP_GAP = 0.1

    # ==================== CONFIGURAÇÕES DE GRID ====================

    # Exibir grid no eixo de valores
    SHOW_VALUE_GRID = True

    # Exibir grid no eixo categórico
    SHOW_CATEGORY_GRID = False

    # Cor do grid
    GRID_COLOR = "rgba(128, 128, 128, 0.2)"

    # ==================== CONFIGURAÇÕES DE AUTOMARGIN ====================

    # Permitir que Plotly ajuste margens automaticamente
    ENABLE_AUTOMARGIN = True

    # Padding adicional quando automargin está ativo
    AUTOMARGIN_PADDING = 10


class BarAesthetics:
    """
    Aplicador de configurações estéticas para gráficos de barras.

    Usa BarAestheticsConfig para aplicar configurações consistentes
    em todos os gráficos de barras.

    Exemplo de uso:
        >>> aesthetics = BarAesthetics()
        >>> fig = go.Figure(...)
        >>> aesthetics.apply_horizontal_bar_style(fig, categories)
    """

    def __init__(self, config: Optional[BarAestheticsConfig] = None):
        """
        Inicializa o aplicador de estética.

        Args:
            config: Configuração customizada (usa padrão se None)
        """
        self.config = config or BarAestheticsConfig()
        logger.debug("BarAesthetics inicializado com configurações centralizadas")

    @staticmethod
    def calculate_dynamic_height(num_categories: int) -> int:
        """
        Calcula a altura ideal do gráfico baseada na quantidade de categorias.

        Garante espaçamento adequado entre barras independente da quantidade.

        Args:
            num_categories: Número de categorias/barras no gráfico

        Returns:
            Altura recomendada em pixels
        """
        # Minimum 400px, with ~35px per bar + overhead for axes/margins
        height = max(400, num_categories * 35 + 120)
        # Cap at a reasonable maximum
        height = min(height, 1200)
        logger.debug(f"Dynamic height calculated: {height}px for {num_categories} categories")
        return height

    def _get_adaptive_font_size(self, num_categories: int) -> int:
        """
        Calcula tamanho de fonte adaptativo baseado na quantidade de categorias.

        Menos itens = fonte maior (até 13), muitos itens = fonte menor (até 9).

        Args:
            num_categories: Número de categorias no gráfico

        Returns:
            Tamanho de fonte em pontos
        """
        if num_categories <= 5:
            return 13
        elif num_categories <= 10:
            return 12
        elif num_categories <= 15:
            return 11
        elif num_categories <= 20:
            return 10
        else:
            return 9

    def apply_horizontal_bar_style(
        self, fig: go.Figure, categories: list, show_grid: bool = True
    ) -> None:
        """
        Aplica estilo estético para gráficos de barras horizontais.

        Configurações aplicadas:
        - Tamanho de fonte adaptativo para rótulos Y (categorias)
        - Automargin para ajuste automático de espaçamento
        - Altura dinâmica baseada na quantidade de categorias
        - Grid configurado

        Args:
            fig: Figure Plotly
            categories: Lista de categorias (para cálculo de margem)
            show_grid: Se deve mostrar grid no eixo X (valores)
        """
        num_categories = len(categories) if categories else 0
        adaptive_font = self._get_adaptive_font_size(num_categories)
        dynamic_height = self.calculate_dynamic_height(num_categories)

        # Aplicar configurações ao eixo Y (categorias)
        # Let automargin handle the left margin dynamically
        fig.update_yaxes(
            tickfont=dict(
                size=adaptive_font, family="Arial, sans-serif"
            ),
            showgrid=self.config.SHOW_CATEGORY_GRID,
            automargin=True,
        )

        # Aplicar configurações ao eixo X (valores)
        fig.update_xaxes(
            tickfont=dict(size=self.config.VALUE_LABEL_FONT_SIZE),
            showgrid=show_grid and self.config.SHOW_VALUE_GRID,
            gridcolor=self.config.GRID_COLOR,
        )

        # Use automargin + sensible base margins; avoid hardcoded large l margin
        fig.update_layout(
            height=dynamic_height,
            margin=dict(
                l=10,   # minimal base; automargin will expand as needed
                r=40,
                t=60,
                b=60,
                pad=self.config.AUTOMARGIN_PADDING
                if self.config.ENABLE_AUTOMARGIN
                else 0,
            ),
            bargap=self.config.BAR_GAP,
            bargroupgap=self.config.BAR_GROUP_GAP,
        )

        logger.info(
            f"Configuração estética aplicada em bar_horizontal: "
            f"height={dynamic_height}px, font_size={adaptive_font}, "
            f"categories={num_categories}"
        )

    def apply_vertical_bar_style(
        self,
        fig: go.Figure,
        categories: list,
        show_grid: bool = True,
        rotate_labels: bool = False,
        rotation_angle: int = -45,
    ) -> None:
        """
        Aplica estilo estético para gráficos de barras verticais.

        Configurações aplicadas:
        - Tamanho de fonte aumentado para rótulos X (categorias)
        - Espaçamento adequado entre rótulos e barras
        - Margem inferior otimizada
        - Opção de rotação de rótulos
        - Grid configurado

        Args:
            fig: Figure Plotly
            categories: Lista de categorias (para cálculo de margem)
            show_grid: Se deve mostrar grid no eixo Y (valores)
            rotate_labels: Se deve rotacionar rótulos do eixo X
            rotation_angle: Ângulo de rotação (negativo = anti-horário)
        """
        # Calcular margem ideal baseada no tamanho máximo das categorias
        max_category_len = (
            max(len(str(cat)) for cat in categories) if categories else 20
        )

        # Para barras verticais, se houver rotação, precisamos de mais margem
        if rotate_labels:
            optimal_margin = max(
                self.config.CATEGORY_AXIS_BASE_MARGIN,
                int(
                    max_category_len * self.config.MARGIN_CHAR_MULTIPLIER * 0.7
                ),  # Ajuste para rotação
            )
        else:
            optimal_margin = self.config.CATEGORY_AXIS_BASE_MARGIN

        # Aplicar configurações ao eixo X (categorias)
        fig.update_xaxes(
            tickfont=dict(
                size=self.config.CATEGORY_LABEL_FONT_SIZE, family="Arial, sans-serif"
            ),
            tickangle=rotation_angle if rotate_labels else 0,
            showgrid=self.config.SHOW_CATEGORY_GRID,
            automargin=self.config.ENABLE_AUTOMARGIN,
        )

        # Aplicar configurações ao eixo Y (valores)
        fig.update_yaxes(
            tickfont=dict(size=self.config.VALUE_LABEL_FONT_SIZE),
            showgrid=show_grid and self.config.SHOW_VALUE_GRID,
            gridcolor=self.config.GRID_COLOR,
        )

        # Atualizar layout com margens otimizadas
        fig.update_layout(
            margin=dict(
                l=80,
                r=40,
                t=100,  # Aumentado de 60 para 100 para evitar corte de valores nas barras
                b=optimal_margin + self.config.LABEL_TO_BAR_GAP,
                pad=self.config.AUTOMARGIN_PADDING
                if self.config.ENABLE_AUTOMARGIN
                else 0,
            ),
            bargap=self.config.BAR_GAP,
            bargroupgap=self.config.BAR_GROUP_GAP,
        )

        logger.info(
            f"Configuração estética aplicada em bar_vertical: "
            f"margem_inferior={optimal_margin}, font_size={self.config.CATEGORY_LABEL_FONT_SIZE}, "
            f"rotacao={rotation_angle if rotate_labels else 0}°"
        )

    def configure_bar_value_labels(
        self, fig: go.Figure, orientation: str = "h"
    ) -> None:
        """
        Configura a aparência dos valores nas barras (quando show_values=True).

        Args:
            fig: Figure Plotly
            orientation: 'h' para horizontal, 'v' para vertical
        """
        fig.update_traces(
            textfont=dict(
                size=self.config.BAR_VALUE_FONT_SIZE, family="Arial, sans-serif"
            ),
            textposition="outside" if orientation == "h" else "outside",
        )

        logger.debug(
            f"Rótulos de valores configurados: font_size={self.config.BAR_VALUE_FONT_SIZE}"
        )
