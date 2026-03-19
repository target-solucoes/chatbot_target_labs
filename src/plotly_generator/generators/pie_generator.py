"""
PieGenerator - Generator para graficos de pizza.

Conforme axis_patterns.md:
- Sem eixos tradicionais (X/Y)
- Labels: Categoria
- Values: Metrica quantitativa
- Uso tipico: Distribuicao, percentuais, proporcao relativa
"""

import logging
from typing import Dict, Any, List
import plotly.graph_objects as go

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class PieGenerator(BasePlotlyGenerator):
    """
    Generator para graficos de pizza.

    Requisitos (conforme axis_patterns.md):
    - Sem eixos tradicionais (pie charts usam labels + values)
    - Labels: Categoria (dimension)
    - Values: Metrica quantitativa
    - Uso tipico: Distribuicao, percentuais, proporcao relativa

    Validacao:
    - Exatamente 1 dimension
    - Exatamente 1 metric
    - Dados nao vazios

    Exemplo de Uso:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> generator = PieGenerator(PlotStyler())
        >>> chart_spec = {
        ...     "chart_type": "pie",
        ...     "title": "Distribuicao de Vendas",
        ...     "dimensions": [{"name": "Categoria", "alias": "Categoria"}],
        ...     "metrics": [{"name": "Vendas", "alias": "Total"}],
        ...     "visual": {"palette": "Set3", "show_values": True}
        ... }
        >>> data = [
        ...     {"Categoria": "Eletronicos", "Total": 45000},
        ...     {"Categoria": "Roupas", "Total": 30000},
        ...     {"Categoria": "Alimentos", "Total": 25000}
        ... ]
        >>> fig = generator.generate(chart_spec, data)
        >>> type(fig)
        <class 'plotly.graph_objs._figure.Figure'>
    """

    def validate(self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]) -> None:
        """
        Valida requisitos de pie chart.

        Validacoes:
        1. Exatamente 1 dimension (labels)
        2. Exatamente 1 metric (values)
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
                f"pie requer exatamente 1 dimension (labels), recebeu {len(dimensions)}"
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) != 1:
            raise ValueError(
                f"pie requer exatamente 1 metric (values), recebeu {len(metrics)}"
            )

        # Validar dados nao vazios
        if not data:
            raise ValueError("Dados vazios - nao e possivel gerar grafico")

        self.logger.debug(
            f"Validacao OK: {len(dimensions)} dimension, "
            f"{len(metrics)} metric, {len(data)} fatias"
        )

    def generate(
        self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> go.Figure:
        """
        Gera grafico de pizza.

        Processo:
        1. Validar dados
        2. Aplicar limitação de categorias (top N + OUTROS)
        3. Extrair dimension (labels) e metric (values) aliases
        4. Extrair valores das colunas
        5. Criar trace go.Pie
        6. Aplicar paleta de cores
        7. Configurar textinfo (percentuais + valores)
        8. Aplicar layout comum

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

        # APLICAR LIMITAÇÃO DE CATEGORIAS COM AGREGAÇÃO EM "OUTROS"
        # Para pie charts, limitamos a top N e agregamos o resto em "OUTROS"
        limited_data, limit_metadata = self._apply_category_limit(
            data=data,
            chart_type="pie",  # Importante: "pie" ativa criação de "OUTROS"
            category_column=dimension_alias,
            metric_column=metric_alias,
        )

        # Usar dados limitados daqui em diante
        data = limited_data

        self.logger.debug(
            f"Gerando pie: labels='{dimension_alias}', values='{metric_alias}' "
            f"({len(data)} fatias"
            f"{' incluindo OUTROS' if limit_metadata['others_created'] else ''})"
        )

        # Extrair dados das colunas
        labels = self._extract_column(data, dimension_alias)
        values = self._extract_column(data, metric_alias)

        # Aplicar quebra de linha em labels longos para melhor visualização
        wrapped_labels = self.styler.text_handler.wrap_labels(labels, max_width=25)

        # Obter configuracoes visuais
        palette = self._get_visual_config(chart_spec, "palette", "Set3")
        show_values = self._get_visual_config(chart_spec, "show_values", True)

        # Obter sequencia de cores
        colors = self.styler.get_color_sequence(palette=palette, n_colors=len(labels))

        # Configurar textinfo baseado em show_values
        if show_values:
            textinfo = "label+percent+value"
            texttemplate = "%{label}<br>%{percent}<br>%{value:,.0f}"
        else:
            textinfo = "label+percent"
            texttemplate = "%{label}<br>%{percent}"

        # Criar trace de pizza
        trace = go.Pie(
            labels=wrapped_labels,  # Usar labels com quebra de linha
            values=values,
            marker={"colors": colors},
            textinfo=textinfo,
            texttemplate=texttemplate,
            hovertemplate=(
                "<b>%{label}</b><br>"
                f"{metric_alias}: %{{value:,.0f}}<br>"
                "Percentual: %{percent}<br>"
                "<extra></extra>"
            ),
            hole=0,  # 0 = pie chart completo, > 0 = donut chart
            pull=0.02,  # Leve separacao entre fatias para melhor visualizacao
        )

        # Criar figure
        fig = go.Figure(data=[trace])

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configuracoes adicionais para pie charts
        fig.update_layout(
            showlegend=True,
            legend={
                "orientation": "v",
                "yanchor": "middle",
                "y": 0.5,
                "xanchor": "left",
                "x": 1.05,
            },
        )

        self.logger.info(f"Grafico pie gerado com sucesso: {len(data)} fatias")

        return fig
