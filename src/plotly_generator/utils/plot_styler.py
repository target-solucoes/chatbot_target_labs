"""
PlotStyler - Gerenciador de estilos visuais para graficos Plotly.

Centraliza toda a logica de styling para garantir consistencia visual entre
todos os tipos de graficos.
"""

import logging
from typing import List, Dict, Any, Tuple, Optional
import plotly.graph_objects as go
import plotly.express as px

from src.shared_lib.utils.logger import get_logger
from src.plotly_generator.utils.color_manager import ColorManager
from src.plotly_generator.utils.text_label_handler import TextLabelHandler

logger = get_logger(__name__)


class PlotStyler:
    """
    Gerenciador de estilos visuais para graficos Plotly.

    Centraliza logica de:
    - Paletas de cores (com filtragem automatica de cores claras)
    - Configuracao de eixos
    - Estilos de legenda
    - Anotacoes de valores

    Exemplo:
        >>> styler = PlotStyler()
        >>> colors = styler.get_color_sequence("Blues", 5)
        >>> styler.apply_axis_config(fig, "Produto", "Vendas")
    """

    # Paletas Plotly suportadas
    PALETTES: Dict[str, List[str]] = {
        "Blues": px.colors.sequential.Blues,
        "Reds": px.colors.sequential.Reds,
        "Greens": px.colors.sequential.Greens,
        "Oranges": px.colors.sequential.Oranges,
        "Purples": px.colors.sequential.Purples,
        "Greys": px.colors.sequential.Greys,
        "Viridis": px.colors.sequential.Viridis,
        "Plasma": px.colors.sequential.Plasma,
        "Inferno": px.colors.sequential.Inferno,
        "Magma": px.colors.sequential.Magma,
        "Cividis": px.colors.sequential.Cividis,
        "Turbo": px.colors.sequential.Turbo,
        "Set1": px.colors.qualitative.Set1,
        "Set2": px.colors.qualitative.Set2,
        "Set3": px.colors.qualitative.Set3,
        "Pastel": px.colors.qualitative.Pastel,
        "Pastel1": px.colors.qualitative.Pastel1,
        "Pastel2": px.colors.qualitative.Pastel2,
        "Dark2": px.colors.qualitative.Dark2,
        "Vivid": px.colors.qualitative.Vivid,
    }

    def __init__(self):
        """Inicializa o PlotStyler com ColorManager e TextLabelHandler integrados."""
        self.color_manager = ColorManager()
        self.text_handler = TextLabelHandler()
        logger.debug("PlotStyler inicializado com ColorManager e TextLabelHandler")

    def get_color_sequence(
        self, palette: str = "Blues", n_colors: int = 10, skip_light_colors: bool = True
    ) -> List[str]:
        """
        Retorna sequencia de cores VISIVEIS da paleta especificada.

        Automaticamente evita cores muito claras para melhor visualizacao.
        Para paletas sequenciais, pula cores claras do inicio.
        Para paletas qualitativas, filtra cores com baixa visibilidade.

        Args:
            palette: Nome da paleta (ex: "Blues", "Set1")
            n_colors: Numero de cores desejadas
            skip_light_colors: Se True, evita cores muito claras (recomendado)

        Returns:
            Lista de cores visiveis em formato hex/rgb

        Exemplo:
            >>> styler = PlotStyler()
            >>> colors = styler.get_color_sequence("Greens", 3)
            >>> len(colors)
            3
            >>> # Retorna cores escuras/medias, pulando as muito claras
        """
        # Delegar para ColorManager que tem logica de filtragem de cores claras
        result = self.color_manager.get_visible_palette_colors(
            palette=palette, n_colors=n_colors, skip_light_colors=skip_light_colors
        )

        logger.debug(
            f"Retornando {len(result)} cores visiveis da paleta '{palette}' "
            f"(skip_light_colors={skip_light_colors})"
        )
        return result

    def apply_axis_config(
        self,
        fig: go.Figure,
        x_title: str,
        y_title: str,
        x_type: str = "linear",
        y_type: str = "linear",
        x_labels: Optional[List[str]] = None,
        y_labels: Optional[List[str]] = None,
        force_vertical_labels: bool = False,
    ) -> None:
        """
        Aplica configuracao consistente de eixos X e Y.

        Configuracoes aplicadas:
        - Titulos dos eixos
        - Gridlines sutis
        - Linha zero destacada
        - Tipo de escala (linear, log, date, category)
        - **NOVO**: Tratamento automático de rótulos longos em eixos categóricos

        Args:
            fig: Figure Plotly a ser configurada
            x_title: Titulo do eixo X
            y_title: Titulo do eixo Y
            x_type: Tipo do eixo X ("linear", "log", "date", "category")
            y_type: Tipo do eixo Y ("linear", "log", "date", "category")
            x_labels: Lista de rótulos do eixo X (para tratamento de textos longos)
            y_labels: Lista de rótulos do eixo Y (para tratamento de textos longos)
            force_vertical_labels: Se True, força rótulos na vertical (-90°) no eixo X

        Exemplo:
            >>> styler = PlotStyler()
            >>> fig = go.Figure()
            >>> labels = ["Produto A com nome muito longo", "Produto B", ...]
            >>> styler.apply_axis_config(fig, "Categoria", "Valor", x_labels=labels)
        """
        # Configuração básica dos eixos
        fig.update_xaxes(
            title=x_title,
            type=x_type,
            showgrid=True,
            gridcolor="lightgray",
            gridwidth=0.5,
            zeroline=True,
            zerolinecolor="gray",
            zerolinewidth=1,
        )

        fig.update_yaxes(
            title=y_title,
            type=y_type,
            showgrid=True,
            gridcolor="lightgray",
            gridwidth=0.5,
            zeroline=True,
            zerolinecolor="gray",
            zerolinewidth=1,
        )

        # Aplicar tratamento de rótulos longos para eixos categóricos
        if x_type == "category" and x_labels:
            logger.debug(
                f"Aplicando tratamento de rótulos longos no eixo X ({len(x_labels)} rótulos)"
            )
            # Usar rotação vertical (-90°) se força vertical ou se há muitos rótulos
            rotation = -90 if (force_vertical_labels or len(x_labels) > 5) else -45
            self.text_handler.apply_categorical_axis_config(
                fig,
                x_labels,
                axis="x",
                force_rotation=force_vertical_labels or len(x_labels) > 3,
                rotation_angle=rotation,
            )

        if y_type == "category" and y_labels:
            logger.debug(
                f"Aplicando tratamento de rótulos longos no eixo Y ({len(y_labels)} rótulos)"
            )
            self.text_handler.apply_categorical_axis_config(fig, y_labels, axis="y")

        logger.debug(
            f"Eixos configurados: X='{x_title}' ({x_type}), Y='{y_title}' ({y_type})"
        )

    def apply_legend_style(self, fig: go.Figure, position: str = "top") -> None:
        """
        Aplica estilo consistente de legenda.

        Posicoes suportadas:
        - "top": Legenda horizontal acima do grafico
        - "right": Legenda vertical a direita do grafico
        - "bottom": Legenda horizontal abaixo do grafico

        Args:
            fig: Figure Plotly a ser configurada
            position: Posicao da legenda ("top", "right", "bottom")

        Exemplo:
            >>> styler = PlotStyler()
            >>> fig = go.Figure()
            >>> styler.apply_legend_style(fig, "right")
        """
        positions = {
            "top": {
                "orientation": "h",
                "yanchor": "bottom",
                "y": 1.02,
                "xanchor": "center",
                "x": 0.5,
            },
            "right": {
                "orientation": "v",
                "yanchor": "top",
                "y": 1,
                "xanchor": "left",
                "x": 1.02,
            },
            "bottom": {
                "orientation": "h",
                "yanchor": "top",
                "y": -0.2,
                "xanchor": "center",
                "x": 0.5,
            },
        }

        config = positions.get(position, positions["top"])
        fig.update_layout(legend=config)

        logger.debug(f"Legenda configurada na posicao '{position}'")

    def add_value_annotations(
        self,
        fig: go.Figure,
        values: List[float],
        positions: List[Tuple[float, float]],
        format_str: str = ".2f",
    ) -> None:
        """
        Adiciona anotacoes de valores no grafico.

        Util quando visual.show_values=True. Adiciona texto formatado
        nas posicoes especificadas.

        Args:
            fig: Figure Plotly
            values: Lista de valores a exibir
            positions: Lista de tuplas (x, y) com posicoes das anotacoes
            format_str: String de formatacao Python (ex: ".2f", ".0f", ".1%")

        Exemplo:
            >>> styler = PlotStyler()
            >>> fig = go.Figure()
            >>> values = [100.5, 200.3, 150.7]
            >>> positions = [(0, 100.5), (1, 200.3), (2, 150.7)]
            >>> styler.add_value_annotations(fig, values, positions, ".1f")
        """
        if len(values) != len(positions):
            logger.warning(
                f"Numero de valores ({len(values)}) difere de posicoes ({len(positions)})"
            )
            return

        annotations = []
        for val, (x, y) in zip(values, positions):
            annotations.append(
                {
                    "x": x,
                    "y": y,
                    "text": f"{val:{format_str}}",
                    "showarrow": False,
                    "font": {"size": 10},
                }
            )

        fig.update_layout(annotations=annotations)
        logger.debug(f"Adicionadas {len(annotations)} anotacoes de valores")

    def get_available_palettes(self) -> List[str]:
        """
        Retorna lista de paletas disponiveis.

        Returns:
            Lista com nomes das paletas suportadas

        Exemplo:
            >>> styler = PlotStyler()
            >>> palettes = styler.get_available_palettes()
            >>> "Blues" in palettes
            True
        """
        return list(self.PALETTES.keys())
