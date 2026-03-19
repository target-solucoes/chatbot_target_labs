"""
BarVerticalGenerator - Generator para graficos de barras verticais.

Conforme axis_patterns.md:
- Eixo X: Categoria
- Eixo Y: Metrica quantitativa
- Uso tipico: Comparacoes diretas
- Ordenacao: Conforme dados
"""

from typing import Dict, Any, List
import plotly.graph_objects as go

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.plotly_generator.utils.color_manager import ColorManager
from src.plotly_generator.utils.bar_aesthetics import BarAesthetics
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class BarVerticalGenerator(BasePlotlyGenerator):
    """
    Generator para graficos de barras verticais.

    Requisitos (conforme axis_patterns.md):
    - Eixo X: Categoria
    - Eixo Y: Metrica quantitativa
    - Uso tipico: Comparacoes diretas

    Validacao:
    - Exatamente 1 dimension
    - Pelo menos 1 metric
    - Dados nao vazios

    Exemplo de Uso:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> generator = BarVerticalGenerator(PlotStyler())
        >>> chart_spec = {
        ...     "chart_type": "bar_vertical",
        ...     "title": "Vendas por Regiao",
        ...     "dimensions": [{"name": "Regiao", "alias": "Regiao"}],
        ...     "metrics": [{"name": "Vendas", "alias": "Total Vendas"}],
        ...     "visual": {"palette": "Greens", "show_values": True}
        ... }
        >>> data = [
        ...     {"Regiao": "Norte", "Total Vendas": 5000},
        ...     {"Regiao": "Sul", "Total Vendas": 7000}
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
        Valida requisitos de bar_vertical.

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
                f"bar_vertical requer exatamente 1 dimension, recebeu {len(dimensions)}"
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError("bar_vertical requer pelo menos 1 metric")

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
        Gera grafico de barras verticais.

        Processo:
        1. Validar dados
        2. Aplicar limitação de categorias (top N)
        3. Extrair dimension e metric aliases
        4. Extrair valores das colunas
        5. Criar trace go.Bar vertical
        6. Aplicar paleta de cores
        7. Configurar show_values se habilitado
        8. Aplicar layout comum
        9. Configurar eixos

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
        # Para bar_vertical, limitamos a top N categorias
        limited_data, limit_metadata = self._apply_category_limit(
            data=data,
            chart_type="bar_vertical",
            category_column=dimension_alias,
            metric_column=metric_alias,
        )

        # Usar dados limitados daqui em diante
        data = limited_data

        # SORT DATA BY METRIC VALUE DESCENDING
        # This ensures the largest values appear first (left to right)
        # and matches the order used in textual insights
        data = sorted(data, key=lambda row: row.get(metric_alias, 0), reverse=True)

        self.logger.debug(
            f"Gerando bar_vertical: X='{dimension_alias}', Y='{metric_alias}' "
            f"({len(data)} categorias, sorted by value DESC)"
        )

        # Extrair dados das colunas (now in descending value order)
        categories = self._extract_column(data, dimension_alias)
        values = self._extract_column(data, metric_alias)

        # Convert categories to strings for consistent axis handling
        categories_str = [str(c) for c in categories]

        # Obter configuracoes visuais
        palette = self._get_visual_config(chart_spec, "palette", "Blues")
        show_values = self._get_visual_config(chart_spec, "show_values", False)

        # PALETA CUSTOMIZADA PARA BAR_VERTICAL - Cores GARANTIDAMENTE distintas
        # Quando há poucas barras ou paleta sequencial, usar cores distintas
        n_bars = len(categories)
        is_sequential = palette in self.color_manager.SEQUENTIAL_PALETTES

        # Usar cores distintas se: paleta sequencial OU poucas barras (<= 3)
        if is_sequential or n_bars <= 3:
            # Cores distintas otimizadas para máximo contraste visual
            DISTINCT_COLORS = [
                "#636EFA",  # Azul royal
                "#EF553B",  # Vermelho vibrante
                "#00CC96",  # Verde esmeralda
                "#AB63FA",  # Roxo intenso
                "#FFA15A",  # Laranja brilhante
                "#19D3F3",  # Ciano vivo
                "#FF6692",  # Rosa choque
                "#B6E880",  # Verde lima
                "#FF97FF",  # Magenta claro
                "#FECB52",  # Amarelo ouro
            ]

            # Usar cores distintas em ciclo se necessário
            colors = [DISTINCT_COLORS[i % len(DISTINCT_COLORS)] for i in range(n_bars)]

            self.logger.debug(
                f"Usando cores DISTINTAS para {n_bars} barras "
                f"(paleta sequencial ou poucas barras detectadas)"
            )
        else:
            # Para muitas barras com paleta qualitativa, usar método padrão
            colors = self.styler.get_color_sequence(palette=palette, n_colors=n_bars)

        # Formatar valores para exibição (K/M) se show_values estiver habilitado
        text_values = self._format_numbers_compact(values) if show_values else None

        # Criar trace de barras verticais
        trace = go.Bar(
            x=categories_str,
            y=values,
            orientation="v",
            marker={"color": colors},
            text=text_values,
            textposition="outside" if show_values else None,
            texttemplate="%{text}" if show_values else None,
            cliponaxis=False if show_values else True,
        )

        # Criar figure
        fig = go.Figure(data=[trace])

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configurar eixos com tratamento automático de rótulos longos
        self.styler.apply_axis_config(
            fig,
            x_title=dimension_alias,
            y_title=metric_alias,
            x_type="category",
            y_type="linear",
            x_labels=categories_str,  # Passar categorias para tratamento de texto longo
            force_vertical_labels=True,  # Forçar rótulos na vertical para evitar sobreposição
        )

        # FORCE VALUE-BASED CATEGORY ORDER
        # Override Plotly's default alphabetical/numerical sorting
        # with the explicit order from our sorted data (descending by metric)
        fig.update_xaxes(
            categoryorder="array",
            categoryarray=categories_str,  # already sorted by value DESC
            showgrid=False,  # Sem grid no eixo X (categorias)
        )

        fig.update_yaxes(
            showgrid=True,  # Grid no eixo Y (valores)
        )

        # APLICAR CONFIGURAÇÕES ESTÉTICAS CENTRALIZADAS
        aesthetics = BarAesthetics()

        # Determinar se precisa rotacionar labels baseado no tamanho
        needs_rotation = any(len(str(cat)) > 10 for cat in categories)
        aesthetics.apply_vertical_bar_style(
            fig,
            categories,
            show_grid=True,
            rotate_labels=needs_rotation,
            rotation_angle=-45,
        )

        # Configurar rótulos de valores se show_values estiver ativo
        if show_values:
            aesthetics.configure_bar_value_labels(fig, orientation="v")
            self._extend_yaxis_for_value_labels(fig, values)

        self.logger.info(f"Grafico bar_vertical gerado com sucesso: {len(data)} barras")

        return fig

    def _extend_yaxis_for_value_labels(
        self, fig: go.Figure, values: List[float]
    ) -> None:
        """Adiciona folga no eixo Y para evitar corte de rótulos acima das barras."""
        if not values:
            return

        min_value = min(values)
        max_value = max(values)
        padding_ratio = 0.08

        positive_padding = (
            max(abs(max_value) * padding_ratio, 1.0) if max_value >= 0 else 0
        )
        negative_padding = (
            max(abs(min_value) * padding_ratio, 1.0) if min_value < 0 else 0
        )

        lower_bound = min(min_value - negative_padding, 0)
        upper_bound = max(max_value + positive_padding, 0)

        if lower_bound == upper_bound:
            upper_bound = lower_bound + 1

        fig.update_yaxes(range=[lower_bound, upper_bound])
