"""
LineComposedGenerator - Generator para graficos de linhas compostas (multiplas).

Conforme axis_patterns.md:
- Eixo X: Tempo (Mes, Data, Ano)
- Eixo Y: Metrica quantitativa
- Agrupamento: Categoria (linha separada para cada categoria)
- Uso tipico: Comparar tendencias entre regioes ou produtos ao longo do tempo
"""

from typing import Dict, Any, List
import plotly.graph_objects as go
import pandas as pd

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.plotly_generator.utils.color_manager import ColorManager
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class LineComposedGenerator(BasePlotlyGenerator):
    """
    Generator para graficos de linhas compostas (multiplas linhas).

    Requisitos (conforme axis_patterns.md):
    - Eixo X: Tempo (primeira dimension - Mes, Data, Ano)
    - Eixo Y: Metrica quantitativa
    - Agrupamento: Categoria (segunda dimension) - uma linha por categoria
    - Mostra multiplas linhas para comparar tendencias

    Validacao:
    - Exatamente 2 dimensions (tempo + categoria)
    - Pelo menos 1 metric
    - Dados nao vazios
    - Primeira dimension deve ser temporal

    Exemplo de Uso:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> generator = LineComposedGenerator(PlotStyler())
        >>> chart_spec = {
        ...     "chart_type": "line_composed",
        ...     "title": "Vendas Mensais por Regiao",
        ...     "dimensions": [
        ...         {"name": "Mes", "alias": "Mes"},
        ...         {"name": "Regiao", "alias": "Regiao"}
        ...     ],
        ...     "metrics": [{"name": "Vendas", "alias": "Total Vendas"}],
        ...     "visual": {"palette": "Viridis", "show_values": False}
        ... }
        >>> data = [
        ...     {"Mes": "2015-01", "Regiao": "Norte", "Total Vendas": 5000},
        ...     {"Mes": "2015-02", "Regiao": "Norte", "Total Vendas": 5500},
        ...     {"Mes": "2015-01", "Regiao": "Sul", "Total Vendas": 7000},
        ...     {"Mes": "2015-02", "Regiao": "Sul", "Total Vendas": 7500}
        ... ]
        >>> fig = generator.generate(chart_spec, data)
        >>> type(fig)
        <class 'plotly.graph_objs._figure.Figure'>
    """

    def __init__(self, styler):
        """Inicializa o generator com styler e color manager."""
        super().__init__(styler)
        self.color_manager = ColorManager()

    def validate(self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]) -> None:
        """
        Valida requisitos de line_composed.

        Validacoes:
        1. 1 ou 2 dimensions (1 = single_line, 2 = multi_line)
        2. Pelo menos 1 metric
        3. Dados nao vazios

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Raises:
            ValueError: Se validacao falhar
        """
        # Validar dimensions
        # LAYER 6 FIX: Accept 1 dimension for single_line variant (temporal_trend with series=None)
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) < 1 or len(dimensions) > 2:
            raise ValueError(
                f"line_composed requer 1 ou 2 dimensions, recebeu {len(dimensions)}"
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError("line_composed requer pelo menos 1 metric")

        # Validar dados nao vazios
        if not data:
            raise ValueError("Dados vazios - nao e possivel gerar grafico")

        self.logger.debug(
            f"Validacao OK: {len(dimensions)} dimensions, "
            f"{len(metrics)} metric(s), {len(data)} linhas"
        )

    def generate(
        self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> go.Figure:
        """
        Gera grafico de linhas compostas (multiplas linhas) ou linha unica.

        Logica:
        1. Extrair aliases das dimensions e metric
        2. Se 2 dimensions: criar multiplas linhas (uma por categoria)
        3. Se 1 dimension: criar uma unica linha (single_line variant)
        4. Aplicar cores distintas para cada linha
        5. Ordenar dados temporalmente
        6. Configurar eixo X como temporal

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Returns:
            go.Figure com uma ou multiplas linhas
        """
        self.logger.info("Gerando grafico line_composed")

        # Extrair aliases
        dimensions = chart_spec.get("dimensions", [])
        time_alias = self._get_dimension_alias(chart_spec, 0)  # Eixo X (tempo)
        metric_alias = self._get_metric_alias(chart_spec, 0)  # Eixo Y

        # LAYER 6: Route to single_line or multi_line based on dimension count
        if len(dimensions) == 1:
            # Single line variant - no categorical grouping
            return self._generate_single_line(
                chart_spec, data, time_alias, metric_alias
            )
        else:
            # Multi-line variant - group by category
            category_alias = self._get_dimension_alias(chart_spec, 1)  # Agrupamento
            return self._generate_multi_line(
                chart_spec, data, time_alias, category_alias, metric_alias
            )

    def _generate_single_line(
        self,
        chart_spec: Dict[str, Any],
        data: List[Dict[str, Any]],
        time_alias: str,
        metric_alias: str,
    ) -> go.Figure:
        """
        Gera grafico de linha unica (single_line variant).

        LAYER 6: temporal_trend com dimension_structure.series=None produz
        apenas 1 dimensao temporal, resultando em uma unica linha continua.

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados
            time_alias: Alias da dimensao temporal
            metric_alias: Alias da metrica

        Returns:
            go.Figure com uma linha
        """
        self.logger.info("LAYER 6: Gerando single_line (1 dimensao)")
        self.logger.debug(f"Tempo='{time_alias}', Metrica='{metric_alias}'")

        # Converter para DataFrame
        df = pd.DataFrame(data)

        # Tentar converter coluna temporal para datetime
        try:
            df[time_alias] = pd.to_datetime(df[time_alias])
            self.logger.debug(f"Coluna '{time_alias}' convertida para datetime")
        except Exception as e:
            self.logger.warning(
                f"Nao foi possivel converter '{time_alias}' para datetime: {e}"
            )

        # Ordenar por tempo
        df = df.sort_values(by=time_alias)

        # Extrair valores
        x_values = df[time_alias].tolist()
        y_values = df[metric_alias].tolist()

        # Configuracao visual
        show_values = chart_spec.get("visual", {}).get("show_values", False)

        # Criar trace unico
        trace = go.Scatter(
            x=x_values,
            y=y_values,
            mode="lines+markers",
            name=metric_alias,
            line=dict(color="#636EFA", width=2),  # Azul padrao
            marker=dict(size=6, color="#636EFA"),
            text=y_values if show_values else None,
            textposition="top center" if show_values else None,
            texttemplate="%{text:.2f}" if show_values else None,
            hovertemplate=(
                f"{time_alias}: %{{x}}<br>{metric_alias}: %{{y:.2f}}<br><extra></extra>"
            ),
        )

        # Criar figure
        fig = go.Figure(data=[trace])

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configurar layout especifico
        fig.update_layout(
            xaxis_title=time_alias,
            yaxis_title=metric_alias,
            showlegend=False,  # Nao precisa de legenda para linha unica
            hovermode="x unified",
        )

        # Configurar eixos
        self.styler.apply_axis_config(
            fig,
            x_title=time_alias,
            y_title=metric_alias,
            x_type="date",
            y_type="linear",
        )

        # Configurar formato de exibição para eixo temporal
        fig.update_xaxes(
            tickformat="%b %Y",  # Ex: Jan 2015, Feb 2015
            dtick="M1",  # Tick a cada mês
        )

        self.logger.info(
            f"Grafico single_line gerado com 1 linha, {len(x_values)} pontos"
        )

        return fig

    def _generate_multi_line(
        self,
        chart_spec: Dict[str, Any],
        data: List[Dict[str, Any]],
        time_alias: str,
        category_alias: str,
        metric_alias: str,
    ) -> go.Figure:
        """
        Gera grafico de linhas compostas (multiplas linhas).

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados
            time_alias: Alias da dimensao temporal
            category_alias: Alias da dimensao categorial
            metric_alias: Alias da metrica

        Returns:
            go.Figure com multiplas linhas
        """
        self.logger.info("Gerando multi_line (2 dimensions)")

        # Converter para DataFrame
        df = pd.DataFrame(data)

        # Tentar converter coluna temporal para datetime
        try:
            df[time_alias] = pd.to_datetime(df[time_alias])
            self.logger.debug(f"Coluna '{time_alias}' convertida para datetime")
        except Exception as e:
            self.logger.warning(
                f"Nao foi possivel converter '{time_alias}' para datetime: {e}"
            )

        # Ordenar por tempo
        df = df.sort_values(by=time_alias)

        # Identificar categorias unicas
        categories = df[category_alias].unique().tolist()
        n_categories = len(categories)

        self.logger.debug(f"Encontradas {n_categories} categorias: {categories}")

        # PALETA CUSTOMIZADA PARA LINE_COMPOSED - Cores GARANTIDAMENTE distintas
        # Ordem otimizada para máximo contraste visual entre linhas adjacentes
        DISTINCT_COLORS = [
            "#EF553B",  # Vermelho vibrante
            "#636EFA",  # Azul royal
            "#00CC96",  # Verde esmeralda
            "#AB63FA",  # Roxo intenso
            "#FFA15A",  # Laranja brilhante
            "#19D3F3",  # Ciano vivo
            "#FF6692",  # Rosa choque
            "#B6E880",  # Verde lima
            "#FF97FF",  # Magenta claro
            "#FECB52",  # Amarelo ouro
            "#8B4513",  # Marrom saddle
            "#00CED1",  # Turquesa escuro
        ]

        # Criar mapeamento direto categoria -> cor distinta
        color_mapping = {}
        for i, category in enumerate(categories):
            # Usar cores distintas em ciclo se houver mais categorias que cores
            color_mapping[category] = DISTINCT_COLORS[i % len(DISTINCT_COLORS)]

        self.logger.debug(
            f"Mapeamento de cores DISTINTAS criado para {n_categories} categorias"
        )

        # Criar traces - uma linha para cada categoria
        traces = []
        show_values = chart_spec.get("visual", {}).get("show_values", False)

        for category in categories:
            # Filtrar dados para esta categoria
            category_data = df[df[category_alias] == category]

            # Extrair valores
            x_values = category_data[time_alias].tolist()
            y_values = category_data[metric_alias].tolist()

            # Criar trace (linha)
            trace = go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines+markers",
                name=str(category),
                line=dict(color=color_mapping[category], width=2),
                marker=dict(size=6, color=color_mapping[category]),
                text=y_values if show_values else None,
                textposition="top center" if show_values else None,
                texttemplate="%{text:.2f}" if show_values else None,
                hovertemplate=(
                    f"<b>{category}</b><br>"
                    f"{time_alias}: %{{x}}<br>"
                    f"{metric_alias}: %{{y:.2f}}<br>"
                    "<extra></extra>"
                ),
            )

            traces.append(trace)

            self.logger.debug(
                f"Trace criada para categoria '{category}' com {len(y_values)} pontos"
            )

        # Criar figure
        fig = go.Figure(data=traces)

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configurar layout especifico
        fig.update_layout(
            xaxis_title=time_alias,
            yaxis_title=metric_alias,
            legend=dict(
                title=dict(text=category_alias),
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
            ),
            hovermode="x unified",
        )

        # Configurar eixos
        self.styler.apply_axis_config(
            fig,
            x_title=time_alias,
            y_title=metric_alias,
            x_type="date",
            y_type="linear",
        )

        # Configurar formato de exibição para eixo temporal
        fig.update_xaxes(
            tickformat="%b %Y",  # Ex: Jan 2015, Feb 2015
            dtick="M1",  # Tick a cada mês
        )

        self.logger.info(
            f"Grafico line_composed gerado com {len(traces)} linhas, "
            f"{len(data)} pontos totais"
        )

        return fig
