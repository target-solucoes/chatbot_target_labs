"""
BarVerticalStackedGenerator - Generator para graficos de barras verticais empilhadas.

Conforme axis_patterns.md:
- Eixo X: Categoria principal
- Eixo Y: Metrica quantitativa (empilhada)
- Agrupamento: Subcategoria (empilhamento)
- Uso tipico: Mostrar composicao de subcategorias dentro de cada grupo principal
"""

from typing import Dict, Any, List
import plotly.graph_objects as go
import pandas as pd

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.plotly_generator.utils.color_manager import ColorManager
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class BarVerticalStackedGenerator(BasePlotlyGenerator):
    """
    Generator para graficos de barras verticais empilhadas.

    Requisitos (conforme axis_patterns.md):
    - Eixo X: Categoria principal (primeira dimension)
    - Eixo Y: Metrica quantitativa (empilhada)
    - Agrupamento: Subcategoria (segunda dimension) - cada uma e uma camada
    - Mostra composicao das subcategorias dentro de cada grupo

    Validacao:
    - Exatamente 2 dimensions (principal + subcategoria)
    - Pelo menos 1 metric
    - Dados nao vazios
    - visual.stacked deve ser True (opcional, mas recomendado)

    Exemplo de Uso:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> generator = BarVerticalStackedGenerator(PlotStyler())
        >>> chart_spec = {
        ...     "chart_type": "bar_vertical_stacked",
        ...     "title": "Vendas por Regiao e Produto",
        ...     "dimensions": [
        ...         {"name": "Regiao", "alias": "Regiao"},
        ...         {"name": "Produto", "alias": "Linha de Produto"}
        ...     ],
        ...     "metrics": [{"name": "Vendas", "alias": "Total Vendas"}],
        ...     "visual": {"palette": "Set2", "show_values": True, "stacked": True}
        ... }
        >>> data = [
        ...     {"Regiao": "Norte", "Linha de Produto": "Bikes", "Total Vendas": 5000},
        ...     {"Regiao": "Norte", "Linha de Produto": "Acessorios", "Total Vendas": 2000},
        ...     {"Regiao": "Sul", "Linha de Produto": "Bikes", "Total Vendas": 7000},
        ...     {"Regiao": "Sul", "Linha de Produto": "Acessorios", "Total Vendas": 3000}
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
        Valida requisitos de bar_vertical_stacked.

        Validacoes:
        1. Exatamente 2 dimensions (principal + subcategoria)
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
        if len(dimensions) != 2:
            raise ValueError(
                f"bar_vertical_stacked requer exatamente 2 dimensions, "
                f"recebeu {len(dimensions)}"
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError("bar_vertical_stacked requer pelo menos 1 metric")

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
        Gera grafico de barras verticais empilhadas.

        Logica:
        1. Extrair aliases das dimensions e metric
        2. Aplicar limitação nas categorias principais (primeira dimension)
        3. Identificar subcategorias unicas (dimension secundaria)
        4. Criar uma trace (camada) por subcategoria
        5. Aplicar cores distintas para cada subcategoria
        6. Configurar layout com barmode='stack'

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Returns:
            go.Figure com barras verticais empilhadas
        """
        self.logger.info("Gerando grafico bar_vertical_stacked")

        # Extrair aliases
        main_category_alias = self._get_dimension_alias(chart_spec, 0)  # Eixo X
        subcategory_alias = self._get_dimension_alias(chart_spec, 1)  # Camadas
        metric_alias = self._get_metric_alias(chart_spec, 0)  # Eixo Y

        # APLICAR LIMITAÇÃO NAS CATEGORIAS PRINCIPAIS
        # Para gráficos empilhados, limitamos as categorias principais (eixo X)
        # e mantemos todas as subcategorias para cada categoria principal
        df = pd.DataFrame(data)

        # Calcular total por categoria principal para ordenação
        totals_by_main = (
            df.groupby(main_category_alias)[metric_alias].sum().reset_index()
        )
        totals_by_main.columns = [main_category_alias, "_total_"]

        # Aplicar limitação usando os totais
        limited_totals, limit_metadata = self._apply_category_limit(
            data=totals_by_main.to_dict("records"),
            chart_type="bar_vertical_stacked",
            category_column=main_category_alias,
            metric_column="_total_",
        )

        # Extrair lista de categorias principais a manter
        top_main_categories = [row[main_category_alias] for row in limited_totals]

        # Filtrar dados originais para manter apenas categorias principais selecionadas
        df = df[df[main_category_alias].isin(top_main_categories)]

        if limit_metadata["limit_applied"]:
            self.logger.info(
                f"Limitadas categorias principais de {limit_metadata['original_count']} "
                f"para {len(top_main_categories)} no gráfico empilhado"
            )

        self.logger.debug(
            f"Categorias: principal='{main_category_alias}', "
            f"subcategoria='{subcategory_alias}', metrica='{metric_alias}'"
        )

        # Identificar subcategorias unicas
        subcategories = df[subcategory_alias].unique().tolist()
        n_subcategories = len(subcategories)

        self.logger.debug(
            f"Encontradas {n_subcategories} subcategorias: {subcategories}"
        )

        # Obter paleta de cores
        palette = chart_spec.get("visual", {}).get("palette", "Set2")
        color_mapping = self.color_manager.get_color_sequence(subcategories, palette)

        # Criar traces - uma para cada subcategoria (camada)
        traces = []
        show_values = chart_spec.get("visual", {}).get("show_values", False)

        for subcategory in subcategories:
            # Filtrar dados para esta subcategoria
            subcategory_data = df[df[subcategory_alias] == subcategory]

            # Extrair valores
            x_values = subcategory_data[main_category_alias].tolist()
            y_values = subcategory_data[metric_alias].tolist()

            # Formatar valores para exibição (K/M) se show_values estiver habilitado
            text_values = (
                self._format_numbers_compact(y_values) if show_values else None
            )

            # Criar trace
            trace = go.Bar(
                x=x_values,
                y=y_values,
                name=str(subcategory),
                marker=dict(color=color_mapping[subcategory]),
                text=text_values,
                textposition="inside" if show_values else None,
                texttemplate="%{text}" if show_values else None,
                textfont=dict(color="white"),
            )

            traces.append(trace)

            self.logger.debug(
                f"Trace criada para subcategoria '{subcategory}' "
                f"com {len(y_values)} valores"
            )

        # Criar figure
        fig = go.Figure(data=traces)

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configurar modo de barras empilhadas
        fig.update_layout(
            barmode="stack",
            xaxis_title=main_category_alias,
            yaxis_title=metric_alias,
            legend=dict(
                title=dict(text=subcategory_alias),
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
            ),
        )

        # Configurar eixos com tratamento automático de rótulos longos
        # Extrair categorias únicas do eixo X para tratamento de texto
        unique_x_categories = df[main_category_alias].unique().tolist()
        self.styler.apply_axis_config(
            fig,
            x_title=main_category_alias,
            y_title=metric_alias,
            x_type="category",
            y_type="linear",
            x_labels=unique_x_categories,  # Passar categorias para tratamento de texto longo
            force_vertical_labels=True,  # Forçar rótulos na vertical para evitar sobreposição
        )

        self.logger.info(
            f"Grafico bar_vertical_stacked gerado com {len(traces)} camadas, "
            f"{len(data)} segmentos totais"
        )

        return fig
