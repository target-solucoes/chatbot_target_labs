"""
LineGenerator - Generator para graficos de linha.

Conforme axis_patterns.md:
- Eixo X: Tempo/periodo (dimension temporal)
- Eixo Y: Metrica quantitativa
- Uso tipico: Tendencias temporais, series temporais
"""

import logging
from typing import Dict, Any, List
import plotly.graph_objects as go

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class LineGenerator(BasePlotlyGenerator):
    """
    Generator para graficos de linha.

    Requisitos (conforme axis_patterns.md):
    - Eixo X: Tempo/periodo (dimension temporal)
    - Eixo Y: Metrica quantitativa
    - Uso tipico: Tendencias temporais, series temporais

    Validacao:
    - Exatamente 1 dimension (temporal)
    - Pelo menos 1 metric
    - Dados nao vazios

    Exemplo de Uso:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> generator = LineGenerator(PlotStyler())
        >>> chart_spec = {
        ...     "chart_type": "line",
        ...     "title": "Vendas Mensais 2015",
        ...     "dimensions": [{"name": "Mes", "alias": "Mes"}],
        ...     "metrics": [{"name": "Vendas", "alias": "Total Vendas"}],
        ...     "visual": {"palette": "Viridis", "show_values": False}
        ... }
        >>> data = [
        ...     {"Mes": "2015-01", "Total Vendas": 10000},
        ...     {"Mes": "2015-02", "Total Vendas": 12000},
        ...     {"Mes": "2015-03", "Total Vendas": 11500}
        ... ]
        >>> fig = generator.generate(chart_spec, data)
        >>> type(fig)
        <class 'plotly.graph_objs._figure.Figure'>
    """

    def validate(self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]) -> None:
        """
        Valida requisitos de line chart.

        Validacoes:
        1. Exatamente 1 dimension (eixo temporal)
        2. Pelo menos 1 metric
        3. Dados nao vazios

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Raises:
            ValueError: Se validacao falhar
        """
        # Validar dimensions
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) != 1:
            raise ValueError(
                f"line requer exatamente 1 dimension (eixo temporal), "
                f"recebeu {len(dimensions)}"
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError("line requer pelo menos 1 metric")

        # Validar dados nao vazios
        if not data:
            raise ValueError("Dados vazios - nao e possivel gerar grafico")

        self.logger.debug(
            f"Validacao OK: {len(dimensions)} dimension, "
            f"{len(metrics)} metric(s), {len(data)} pontos"
        )

    def generate(
        self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> go.Figure:
        """
        Gera grafico de linha.

        Processo:
        1. Validar dados
        2. Extrair dimension (tempo) e metric aliases
        3. Extrair valores das colunas
        4. Criar trace go.Scatter com mode='lines+markers'
        5. Aplicar cor da paleta
        6. Configurar markers e linha
        7. Aplicar layout comum
        8. Configurar eixos (X como temporal)

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Returns:
            go.Figure pronto para renderizacao

        Raises:
            ValueError: Se validacao falhar
            Exception: Se geracao falhar
        """
        # Validar
        self.validate(chart_spec, data)

        # Extrair aliases
        dimension_alias = self._get_dimension_alias(chart_spec, 0)
        metric_alias = self._get_metric_alias(chart_spec, 0)

        self.logger.debug(f"Gerando line: X='{dimension_alias}', Y='{metric_alias}'")

        # Extrair dados das colunas
        time_values = self._extract_column(data, dimension_alias)
        metric_values = self._extract_column(data, metric_alias)

        # Obter configuracoes visuais
        palette = self._get_visual_config(chart_spec, "palette", "Viridis")
        show_values = self._get_visual_config(chart_spec, "show_values", False)

        # Obter cor visivel da paleta (evita cores muito claras automaticamente)
        from src.plotly_generator.utils.color_manager import ColorManager

        color_manager = ColorManager()
        colors = color_manager.get_visible_palette_colors(palette, n_colors=1)
        line_color = colors[0] if colors else "#1f77b4"  # Fallback azul

        # Configurar mode baseado em show_values
        mode = "lines+markers+text" if show_values else "lines+markers"

        # Criar trace de linha
        trace = go.Scatter(
            x=time_values,
            y=metric_values,
            mode=mode,
            name=metric_alias,
            line={"color": line_color, "width": 2},
            marker={
                "size": 8,
                "color": line_color,
                "line": {"width": 1, "color": "white"},
            },
            text=metric_values if show_values else None,
            textposition="top center" if show_values else None,
            texttemplate="%{text:,.0f}" if show_values else None,
            hovertemplate=(
                f"<b>{dimension_alias}</b>: %{{x}}<br>"
                f"<b>{metric_alias}</b>: %{{y:,.0f}}<br>"
                "<extra></extra>"
            ),
        )

        # Criar figure
        fig = go.Figure(data=[trace])

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configurar eixos
        # Tentar detectar se e temporal para usar type='date'
        x_type = "linear"  # Default
        if time_values and isinstance(time_values[0], str):
            # Se for string, pode ser data/periodo
            if any(char in str(time_values[0]) for char in ["-", "/"]):
                x_type = "date"

        self.styler.apply_axis_config(
            fig,
            x_title=dimension_alias,
            y_title=metric_alias,
            x_type=x_type,
            y_type="linear",
            x_labels=time_values
            if x_type == "category"
            else None,  # Passar labels se categórico
        )

        # Ajustar configuracoes especificas de line charts
        xaxis_config = {"showgrid": True, "gridcolor": "lightgray"}

        # Se eixo X for temporal, configurar formato de exibição
        if x_type == "date":
            xaxis_config["tickformat"] = "%b %Y"  # Ex: Jan 2015, Feb 2015
            xaxis_config["dtick"] = "M1"  # Tick a cada mês

        fig.update_xaxes(**xaxis_config)

        fig.update_yaxes(showgrid=True, gridcolor="lightgray", zeroline=True)

        # Nao mostrar legenda para graficos de linha simples (1 serie)
        fig.update_layout(showlegend=False)

        self.logger.info(f"Grafico line gerado com sucesso: {len(data)} pontos")

        return fig
