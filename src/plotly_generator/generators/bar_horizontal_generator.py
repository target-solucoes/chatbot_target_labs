"""
BarHorizontalGenerator - Generator para graficos de barras horizontais.

Conforme axis_patterns.md:
- Eixo X: Metrica quantitativa
- Eixo Y: Categoria
- Uso tipico: Rankings, top-N
- Ordenacao: DESC (maiores no topo)
"""

import logging
from typing import Dict, Any, List
import plotly.graph_objects as go

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.plotly_generator.utils.bar_aesthetics import BarAesthetics
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class BarHorizontalGenerator(BasePlotlyGenerator):
    """
    Generator para graficos de barras horizontais.

    Requisitos (conforme axis_patterns.md):
    - Eixo X: Metrica quantitativa
    - Eixo Y: Categoria
    - Uso tipico: Rankings, top-N
    - Ordenacao: DESC (maiores no topo)

    Validacao:
    - Exatamente 1 dimension
    - Pelo menos 1 metric
    - Dados nao vazios

    Exemplo de Uso:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> generator = BarHorizontalGenerator(PlotStyler())
        >>> chart_spec = {
        ...     "chart_type": "bar_horizontal",
        ...     "title": "Top 5 Produtos",
        ...     "dimensions": [{"name": "Produto", "alias": "Produto"}],
        ...     "metrics": [{"name": "Vendas", "alias": "Total Vendas"}],
        ...     "visual": {"palette": "Blues", "show_values": True}
        ... }
        >>> data = [
        ...     {"Produto": "A", "Total Vendas": 1000},
        ...     {"Produto": "B", "Total Vendas": 800}
        ... ]
        >>> fig = generator.generate(chart_spec, data)
        >>> type(fig)
        <class 'plotly.graph_objs._figure.Figure'>
    """

    def validate(self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]) -> None:
        """
        Valida requisitos de bar_horizontal.

        Validacoes:
        1. Exatamente 1 dimension
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
                f"bar_horizontal requer exatamente 1 dimension, "
                f"recebeu {len(dimensions)}"
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError("bar_horizontal requer pelo menos 1 metric")

        # Validar dados nao vazios
        if not data:
            raise ValueError("Dados vazios - nao e possivel gerar grafico")

        self.logger.debug(
            f"Validacao OK: {len(dimensions)} dimension, "
            f"{len(metrics)} metric(s), {len(data)} linhas"
        )

    def generate(
        self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> go.Figure:
        """
        Gera grafico de barras horizontais.

        Processo:
        1. Validar dados
        2. Aplicar limitação de categorias (top N)
        3. Extrair dimension e metric aliases
        4. Extrair valores das colunas
        5. Wrap long labels for readability
        6. Criar trace go.Bar horizontal
        7. Aplicar paleta de cores
        8. Configurar show_values se habilitado
        9. Aplicar layout comum
        10. Configurar eixos com wrapped labels
        11. Aplicar configurações estéticas (dynamic height, adaptive font)

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

        # APLICAR LIMITAÇÃO DE CATEGORIAS
        # Para bar_horizontal, limitamos a top N categorias
        limited_data, limit_metadata = self._apply_category_limit(
            data=data,
            chart_type="bar_horizontal",
            category_column=dimension_alias,
            metric_column=metric_alias,
        )

        # Usar dados limitados daqui em diante
        data = limited_data

        self.logger.debug(
            f"Gerando bar_horizontal: Y='{dimension_alias}', X='{metric_alias}' "
            f"({len(data)} categorias)"
        )

        # Extrair dados das colunas
        categories = self._extract_column(data, dimension_alias)
        values = self._extract_column(data, metric_alias)

        # Wrap long labels for readability on the Y-axis
        from src.plotly_generator.utils.text_label_handler import TextLabelHandler
        text_handler = TextLabelHandler()
        wrapped_categories = text_handler.wrap_labels(
            [str(c) for c in categories], max_width=25
        )

        # Obter configuracoes visuais
        show_values = self._get_visual_config(chart_spec, "show_values", False)

        # Usar cor azul-claro unica para todas as barras
        light_blue = "#87CEEB"  # Sky blue - tom de azul-claro
        colors = [light_blue] * len(categories)

        # Formatar valores para exibição (K/M) se show_values estiver habilitado
        text_values = self._format_numbers_compact(values) if show_values else None

        # Criar trace de barras horizontais usando wrapped labels
        trace = go.Bar(
            x=values,
            y=wrapped_categories,
            orientation="h",
            marker={"color": colors},
            text=text_values,
            textposition="outside" if show_values else None,
            texttemplate="%{text}" if show_values else None,
        )

        # Criar figure
        fig = go.Figure(data=[trace])

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configurar eixos — simplified, only set titles and types
        fig.update_xaxes(
            title=metric_alias,
            type="linear",
            showgrid=True,
            gridcolor="lightgray",
            gridwidth=0.5,
            zeroline=True,
            zerolinecolor="gray",
            zerolinewidth=1,
        )

        fig.update_yaxes(
            title=dimension_alias,
            type="category",
            showgrid=False,
            autorange="reversed",  # Mantem rankings em ordem decrescente visualmente
            automargin=True,
        )

        # APLICAR CONFIGURAÇÕES ESTÉTICAS CENTRALIZADAS
        # This sets dynamic height, adaptive font, automargin, and grid
        aesthetics = BarAesthetics()
        aesthetics.apply_horizontal_bar_style(fig, wrapped_categories, show_grid=True)

        # Configurar rótulos de valores se show_values estiver ativo
        if show_values:
            aesthetics.configure_bar_value_labels(fig, orientation="h")

        self.logger.info(
            f"Grafico bar_horizontal gerado com sucesso: {len(data)} barras"
        )

        return fig
