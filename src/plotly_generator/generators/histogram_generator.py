"""
HistogramGenerator - Generator para histogramas (analise de distribuicao).

Conforme axis_patterns.md:
- Eixo X: Faixas de valores numericos (bins)
- Eixo Y: Frequencia (contagem)
- Uso tipico: Analisar distribuicao de valores numericos
- Nenhuma dimension necessaria (apenas metrica)
"""

from typing import Dict, Any, List
import plotly.graph_objects as go

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class HistogramGenerator(BasePlotlyGenerator):
    """
    Generator para histogramas.

    Requisitos (conforme axis_patterns.md):
    - Eixo X: Faixas de valores (bins) da metrica
    - Eixo Y: Frequencia (contagem automatica)
    - Nenhuma dimension necessaria
    - Pelo menos 1 metric (valores a distribuir)

    Validacao:
    - 0 dimensions (histograma nao usa categorias)
    - Exatamente 1 metric
    - Dados nao vazios

    Exemplo de Uso:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> generator = HistogramGenerator(PlotStyler())
        >>> chart_spec = {
        ...     "chart_type": "histogram",
        ...     "title": "Distribuicao de Valores de Vendas",
        ...     "dimensions": [],
        ...     "metrics": [{"name": "Valor_Venda", "alias": "Valor da Venda"}],
        ...     "visual": {"palette": "Blues", "bins": 20}
        ... }
        >>> data = [
        ...     {"Valor da Venda": 100},
        ...     {"Valor da Venda": 150},
        ...     {"Valor da Venda": 200},
        ...     {"Valor da Venda": 180},
        ...     {"Valor da Venda": 120}
        ... ]
        >>> fig = generator.generate(chart_spec, data)
        >>> type(fig)
        <class 'plotly.graph_objs._figure.Figure'>
    """

    def validate(self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]) -> None:
        """
        Valida requisitos de histogram.

        Validacoes:
        1. 0 dimensions (histograma nao usa categorias)
        2. Exatamente 1 metric
        3. Dados nao vazios

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Raises:
            ValueError: Se validacao falhar
        """
        # Validar dimensions (deve ser vazio)
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) != 0:
            raise ValueError(
                f"histogram nao deve ter dimensions, recebeu {len(dimensions)}"
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) != 1:
            raise ValueError(
                f"histogram requer exatamente 1 metric, recebeu {len(metrics)}"
            )

        # Validar dados nao vazios
        if not data:
            raise ValueError("Dados vazios - nao e possivel gerar grafico")

        self.logger.debug(
            f"Validacao OK: {len(dimensions)} dimensions, "
            f"{len(metrics)} metric, {len(data)} valores"
        )

    def generate(
        self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> go.Figure:
        """
        Gera histograma.

        Logica:
        1. Extrair valores da metrica
        2. Determinar numero de bins (do visual.bins ou automatico)
        3. Criar histograma com Plotly
        4. Aplicar cor da paleta
        5. Configurar eixos (X = faixas, Y = frequencia)

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Returns:
            go.Figure com histograma
        """
        self.logger.info("Gerando histograma")

        # Extrair alias da metrica
        metric_alias = self._get_metric_alias(chart_spec, 0)

        # Extrair valores numericos
        values = self._extract_column(data, metric_alias)

        # Filtrar valores None/null
        values = [v for v in values if v is not None]

        if not values:
            raise ValueError(
                f"Nenhum valor valido encontrado para metrica '{metric_alias}'"
            )

        self.logger.debug(f"Extraidos {len(values)} valores para histograma")

        # Determinar numero de bins
        n_bins = chart_spec.get("visual", {}).get("bins", None)
        if n_bins is None:
            # Auto: usar regra de Sturges
            import math

            n_bins = int(1 + 3.322 * math.log10(len(values)))

        self.logger.debug(f"Usando {n_bins} bins para histograma")

        # Obter cor da paleta
        palette = chart_spec.get("visual", {}).get("palette", "Blues")
        colors = self.styler.get_color_sequence(palette, n_bins)

        # Usar a cor mais escura da paleta
        color = colors[-1] if colors else "#1f77b4"

        # Criar histograma
        trace = go.Histogram(
            x=values,
            nbinsx=n_bins,
            marker=dict(color=color, line=dict(color="white", width=1)),
            name=metric_alias,
            hovertemplate=("Faixa: %{x}<br>Frequência: %{y}<br><extra></extra>"),
        )

        # Criar figure
        fig = go.Figure(data=[trace])

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configurar layout especifico
        fig.update_layout(
            xaxis_title=metric_alias,
            yaxis_title="Frequência",
            bargap=0.05,
            showlegend=False,
        )

        # Configurar eixos
        self.styler.apply_axis_config(
            fig,
            x_title=metric_alias,
            y_title="Frequência",
            x_type="linear",
            y_type="linear",
        )

        self.logger.info(f"Histograma gerado com {n_bins} bins, {len(values)} valores")

        return fig
