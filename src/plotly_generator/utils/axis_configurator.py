"""
AxisConfigurator - Configurador de eixos baseado em axis_patterns.md.

Garante que os eixos seguem as convencoes estabelecidas:
- bar_horizontal: X = metrica, Y = categoria
- bar_vertical: X = categoria, Y = metrica
- line: X = tempo, Y = metrica
- etc.
"""

from typing import Dict, Any, Optional
import plotly.graph_objects as go

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class AxisConfigurator:
    """
    Configurador de eixos baseado em axis_patterns.md.

    Garante que os eixos seguem as convencoes estabelecidas para cada
    tipo de grafico, incluindo:
    - Tipos de eixos (linear, log, date, category)
    - Titulos apropriados
    - Formatacao temporal
    - Grid e estilos

    Exemplo:
        >>> configurator = AxisConfigurator()
        >>> configurator.configure_axes(fig, "bar_horizontal", chart_spec)
    """

    # Mapeamento de chart_type para configuracao de eixos
    AXIS_PATTERNS: Dict[str, Dict[str, Dict[str, str]]] = {
        "bar_horizontal": {
            "x": {"type": "linear", "title_from": "metric"},
            "y": {"type": "category", "title_from": "dimension"},
        },
        "bar_vertical": {
            "x": {"type": "category", "title_from": "dimension"},
            "y": {"type": "linear", "title_from": "metric"},
        },
        "bar_vertical_composed": {
            "x": {"type": "category", "title_from": "dimension"},
            "y": {"type": "linear", "title_from": "metric"},
        },
        "bar_vertical_stacked": {
            "x": {"type": "category", "title_from": "dimension"},
            "y": {"type": "linear", "title_from": "metric"},
        },
        "line": {
            "x": {"type": "date", "title_from": "dimension"},
            "y": {"type": "linear", "title_from": "metric"},
        },
        "line_composed": {
            "x": {"type": "date", "title_from": "dimension"},
            "y": {"type": "linear", "title_from": "metric"},
        },
        "pie": {
            "x": None,  # Pie charts nao tem eixos tradicionais
            "y": None,
        },
        "histogram": {
            "x": {"type": "linear", "title_from": "metric"},
            "y": {"type": "linear", "title_from": "count"},
        },
    }

    def __init__(self):
        """Inicializa o AxisConfigurator."""
        logger.debug("AxisConfigurator inicializado")

    def configure_axes(
        self, fig: go.Figure, chart_type: str, chart_spec: Dict[str, Any]
    ) -> None:
        """
        Configura eixos X e Y conforme chart_type e axis_patterns.md.

        Args:
            fig: Figure Plotly a configurar
            chart_type: Tipo de grafico (ex: "bar_horizontal")
            chart_spec: ChartOutput original

        Exemplo:
            >>> configurator = AxisConfigurator()
            >>> fig = go.Figure()
            >>> chart_spec = {"dimensions": [...], "metrics": [...]}
            >>> configurator.configure_axes(fig, "bar_horizontal", chart_spec)
        """
        pattern = self.AXIS_PATTERNS.get(chart_type)

        if not pattern:
            logger.warning(f"Sem pattern de eixos para '{chart_type}', usando default")
            return

        # Configurar eixo X
        if pattern["x"]:
            x_config = self._build_axis_config(pattern["x"], chart_spec, axis_name="x")
            fig.update_xaxes(**x_config)
            logger.debug(f"Eixo X configurado: {x_config}")

        # Configurar eixo Y
        if pattern["y"]:
            y_config = self._build_axis_config(pattern["y"], chart_spec, axis_name="y")
            fig.update_yaxes(**y_config)
            logger.debug(f"Eixo Y configurado: {y_config}")

    def _build_axis_config(
        self, pattern: Dict[str, str], chart_spec: Dict[str, Any], axis_name: str
    ) -> Dict[str, Any]:
        """
        Constroi configuracao de eixo a partir do pattern.

        Args:
            pattern: Pattern do eixo (tipo, title_from)
            chart_spec: ChartOutput
            axis_name: Nome do eixo ("x" ou "y")

        Returns:
            Dicionario com configuracao do eixo
        """
        config: Dict[str, Any] = {
            "type": pattern["type"],
            "showgrid": True,
            "gridcolor": "lightgray",
            "zeroline": True,
            "zerolinewidth": 1,
            "zerolinecolor": "gray",
        }

        # Determinar titulo do eixo
        title = self._get_axis_title(pattern["title_from"], chart_spec)
        if title:
            config["title"] = title

        # Formatacao especial para eixos temporais
        if pattern["type"] == "date":
            config.update(
                {
                    "tickformat": "%b %Y",  # Ex: Jan 2015
                    "dtick": "M1",  # Tick mensal
                    "tickangle": -45,
                }
            )

        # Formatacao especial para eixos de categoria
        if pattern["type"] == "category":
            config["categoryorder"] = "total ascending"

        return config

    def _get_axis_title(
        self, title_from: str, chart_spec: Dict[str, Any]
    ) -> Optional[str]:
        """
        Extrai titulo do eixo baseado em title_from.

        Args:
            title_from: Fonte do titulo ("metric", "dimension", "count")
            chart_spec: ChartOutput

        Returns:
            Titulo do eixo ou None
        """
        if title_from == "metric":
            metrics = chart_spec.get("metrics", [])
            if metrics:
                return metrics[0].get("alias") or metrics[0].get("name", "")

        elif title_from == "dimension":
            dimensions = chart_spec.get("dimensions", [])
            if dimensions:
                return dimensions[0].get("alias") or dimensions[0].get("name", "")

        elif title_from == "count":
            return "Frequência"

        return None

    def apply_temporal_formatting(
        self, fig: go.Figure, axis: str = "x", date_format: str = "%b %Y"
    ) -> None:
        """
        Aplica formatacao temporal a um eixo especifico.

        Args:
            fig: Figure Plotly
            axis: Eixo a formatar ("x" ou "y")
            date_format: Formato de data (ex: "%b %Y" para "Jan 2015")

        Exemplo:
            >>> configurator.apply_temporal_formatting(fig, "x", "%d/%m/%Y")
        """
        update_method = fig.update_xaxes if axis == "x" else fig.update_yaxes

        update_method(type="date", tickformat=date_format, dtick="M1", tickangle=-45)

        logger.debug(f"Formatacao temporal aplicada ao eixo {axis}")
