"""
BasePlotlyGenerator - Classe abstrata base para todos os generators de graficos.

Define a interface comum e metodos utilitarios reutilizaveis por todos os
generators especificos.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
import plotly.graph_objects as go

from src.plotly_generator.utils.plot_styler import PlotStyler
from src.plotly_generator.utils.category_limiter import CategoryLimiter
from src.plotly_generator.core.visualization_config import get_visualization_config
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class BasePlotlyGenerator(ABC):
    """
    Classe base abstrata para todos os generators de graficos Plotly.

    Define a interface comum que todos os generators devem implementar:
    - validate(): Valida requisitos especificos do tipo de grafico
    - generate(): Gera o objeto Plotly Figure

    Fornece metodos utilitarios reutilizaveis:
    - _extract_column(): Extrai valores de uma coluna
    - _get_dimension_alias(): Retorna alias de uma dimension
    - _get_metric_alias(): Retorna alias de uma metrica
    - _apply_common_layout(): Aplica layout padrao

    Exemplo de Subclasse:
        >>> class BarHorizontalGenerator(BasePlotlyGenerator):
        ...     def validate(self, chart_spec, data):
        ...         # Validacao especifica
        ...         pass
        ...
        ...     def generate(self, chart_spec, data):
        ...         # Geracao especifica
        ...         return go.Figure()
    """

    def __init__(self, styler: PlotStyler):
        """
        Inicializa o generator.

        Args:
            styler: Instancia de PlotStyler para aplicar estilos
        """
        self.styler = styler
        self.logger = get_logger(self.__class__.__name__)
        self._last_limited_data = None  # Armazena últimos dados limitados
        self._last_limit_metadata = None  # Armazena metadata da última limitação

        self.logger.debug(f"{self.__class__.__name__} inicializado")

    def _get_category_limiter(self, chart_type: str = None) -> CategoryLimiter:
        """
        Retorna um CategoryLimiter configurado com as configurações atuais.

        Este método cria o limiter sob demanda, garantindo que sempre
        use a configuração mais recente.

        Args:
            chart_type: Tipo de gráfico para usar limite específico (opcional)

        Returns:
            CategoryLimiter configurado
        """
        viz_config = get_visualization_config()
        # Usar limite específico por tipo se fornecido, senão usar global
        if chart_type:
            max_categories = viz_config.get_limit_for_chart_type(chart_type)
        else:
            max_categories = viz_config.max_categories
        return CategoryLimiter(max_categories=max_categories)

    def get_last_limited_data(self) -> Optional[List[Dict[str, Any]]]:
        """
        Retorna os últimos dados limitados após geração do gráfico.

        Este método permite que o plotly_generator_agent acesse os dados
        que foram efetivamente usados no gráfico (após limitação de categorias),
        para que o formatter_agent possa usá-los corretamente.

        Returns:
            Lista de dicionários com dados limitados, ou None se não houve limitação
        """
        return self._last_limited_data

    def get_last_limit_metadata(self) -> Optional[Dict[str, Any]]:
        """
        Retorna metadata sobre a última limitação aplicada.

        Returns:
            Dicionário com informações sobre a limitação (original_count,
            limited_count, categories_excluded, others_created, etc.)
        """
        return self._last_limit_metadata

    @abstractmethod
    def validate(self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]) -> None:
        """
        Valida requisitos especificos do tipo de grafico.

        Cada generator deve implementar suas proprias regras de validacao:
        - Numero correto de dimensions
        - Numero correto de metrics
        - Tipos de dados apropriados
        - Dados nao vazios

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Raises:
            ValueError: Se validacao falhar com descricao do erro
        """
        pass

    @abstractmethod
    def generate(
        self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> go.Figure:
        """
        Gera objeto Plotly Figure.

        Cada generator implementa a logica especifica para criar o grafico:
        1. Extrair dados das colunas
        2. Criar traces apropriadas
        3. Aplicar estilos e paletas
        4. Configurar layout
        5. Retornar Figure pronta

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Returns:
            go.Figure pronto para renderizacao

        Raises:
            Exception: Se geracao falhar
        """
        pass

    # Metodos utilitarios compartilhados

    def _extract_column(
        self, data: List[Dict[str, Any]], column_name: str
    ) -> List[Any]:
        """
        Extrai valores de uma coluna dos dados.

        Args:
            data: Lista de dicionarios com dados
            column_name: Nome/alias da coluna a extrair

        Returns:
            Lista com valores da coluna

        Exemplo:
            >>> data = [{"Produto": "A", "Vendas": 100}, {"Produto": "B", "Vendas": 200}]
            >>> generator._extract_column(data, "Vendas")
            [100, 200]
        """
        values = [row.get(column_name) for row in data]
        self.logger.debug(f"Extraidos {len(values)} valores da coluna '{column_name}'")
        return values

    def _get_dimension_alias(self, chart_spec: Dict[str, Any], index: int = 0) -> str:
        """
        Retorna alias da dimension no index especificado.

        Se alias nao existir, retorna o nome original da dimension.

        Args:
            chart_spec: ChartOutput
            index: Indice da dimension (0-based)

        Returns:
            Alias ou nome da dimension

        Raises:
            IndexError: Se index fora do range

        Exemplo:
            >>> chart_spec = {"dimensions": [{"name": "Produto", "alias": "Nome do Produto"}]}
            >>> generator._get_dimension_alias(chart_spec, 0)
            'Nome do Produto'
        """
        dims = chart_spec.get("dimensions", [])
        if index >= len(dims):
            raise IndexError(
                f"Dimension index {index} fora do range (total: {len(dims)})"
            )

        dimension = dims[index]
        alias = dimension.get("alias") or dimension["name"]
        self.logger.debug(f"Dimension[{index}] alias: '{alias}'")
        return alias

    def _get_metric_alias(self, chart_spec: Dict[str, Any], index: int = 0) -> str:
        """
        Retorna alias da metric no index especificado.

        Se alias nao existir, retorna o nome original da metrica.

        Args:
            chart_spec: ChartOutput
            index: Indice da metrica (0-based)

        Returns:
            Alias ou nome da metrica

        Raises:
            IndexError: Se index fora do range

        Exemplo:
            >>> chart_spec = {"metrics": [{"name": "Qtd_Vendida", "alias": "Quantidade"}]}
            >>> generator._get_metric_alias(chart_spec, 0)
            'Quantidade'
        """
        metrics = chart_spec.get("metrics", [])
        if index >= len(metrics):
            raise IndexError(
                f"Metric index {index} fora do range (total: {len(metrics)})"
            )

        metric = metrics[index]
        alias = metric.get("alias") or metric["name"]
        self.logger.debug(f"Metric[{index}] alias: '{alias}'")
        return alias

    def _apply_common_layout(self, fig: go.Figure, chart_spec: Dict[str, Any]) -> None:
        """
        Aplica configuracoes de layout comuns a todos os graficos.

        Configuracoes aplicadas:
        - Font family e size
        - Margins
        - Background colors

        Args:
            fig: Figure Plotly a ser configurada
            chart_spec: ChartOutput com titulo e descricao

        Exemplo:
            >>> fig = go.Figure()
            >>> chart_spec = {"title": "Meu Grafico"}
            >>> generator._apply_common_layout(fig, chart_spec)
        """
        fig.update_layout(
            font={"family": "Arial, sans-serif", "size": 12},
            margin={"l": 80, "r": 40, "t": 40, "b": 60},
            plot_bgcolor="white",
            paper_bgcolor="white",
        )

        self.logger.debug("Layout comum aplicado (sem título)")

    def _get_visual_config(
        self, chart_spec: Dict[str, Any], key: str, default: Any = None
    ) -> Any:
        """
        Extrai configuracao visual do chart_spec.

        Args:
            chart_spec: ChartOutput
            key: Chave da configuracao visual (ex: "palette", "show_values")
            default: Valor padrao se nao encontrado

        Returns:
            Valor da configuracao ou default

        Exemplo:
            >>> chart_spec = {"visual": {"palette": "Blues", "show_values": True}}
            >>> generator._get_visual_config(chart_spec, "palette")
            'Blues'
            >>> generator._get_visual_config(chart_spec, "stacked", False)
            False
        """
        visual = chart_spec.get("visual", {})
        value = visual.get(key, default)
        self.logger.debug(f"Visual config '{key}': {value}")
        return value

    def _format_number_compact(self, value: float) -> str:
        """
        Formata numero de forma compacta: K para milhares, M para milhoes.

        Args:
            value: Numero a ser formatado

        Returns:
            String formatada (ex: "1.5M", "2.3K", "500")

        Exemplo:
            >>> generator._format_number_compact(1500000)
            '1.5M'
            >>> generator._format_number_compact(2300)
            '2.3K'
            >>> generator._format_number_compact(500)
            '500'
        """
        abs_value = abs(value)

        if abs_value >= 1_000_000:
            formatted_value = value / 1_000_000
            # Remover zeros desnecessários (usar tolerância para comparação de float)
            if abs(formatted_value - round(formatted_value)) < 0.01:
                formatted = f"{int(round(formatted_value))}M"
            else:
                formatted = f"{formatted_value:.1f}M"
        elif abs_value >= 1_000:
            formatted_value = value / 1_000
            # Remover zeros desnecessários (usar tolerância para comparação de float)
            if abs(formatted_value - round(formatted_value)) < 0.01:
                formatted = f"{int(round(formatted_value))}K"
            else:
                formatted = f"{formatted_value:.1f}K"
        else:
            # Para valores menores que 1000, mostrar como inteiro se possível
            if abs(value - round(value)) < 0.01:
                formatted = f"{int(round(value))}"
            else:
                formatted = f"{value:.0f}"

        return formatted

    def _format_numbers_compact(self, values: List[float]) -> List[str]:
        """
        Formata lista de numeros de forma compacta: K para milhares, M para milhoes.

        Args:
            values: Lista de numeros a serem formatados

        Returns:
            Lista de strings formatadas

        Exemplo:
            >>> generator._format_numbers_compact([1500000, 2300, 500])
            ['1.5M', '2.3K', '500']
        """
        return [self._format_number_compact(v) for v in values]

    def _apply_category_limit(
        self,
        data: List[Dict[str, Any]],
        chart_type: str,
        category_column: str,
        metric_column: str,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Aplica limitação de categorias aos dados baseado na configuração.

        Este método implementa a lógica central de limitação de categorias
        para todos os tipos de gráficos:
        - Gráficos de barras: limita a top N categorias
        - Gráficos de pizza: limita a top N + agrega resto em "OUTROS"

        Args:
            data: Lista de dicionários com os dados
            chart_type: Tipo de gráfico (ex: "bar_horizontal", "pie")
            category_column: Nome da coluna de categoria
            metric_column: Nome da coluna de métrica (para ordenação)

        Returns:
            Tupla contendo:
            - Lista de dicionários com dados limitados
            - Dicionário de metadados sobre a limitação aplicada

        Exemplo:
            >>> data = [{"cat": "A", "val": 100}, ...]  # 50 produtos
            >>> limited, meta = self._apply_category_limit(
            ...     data, "bar_horizontal", "cat", "val"
            ... )
            >>> len(limited)
            15  # Limitado a 15 categorias
            >>> meta["categories_excluded"]
            35  # 35 categorias foram excluídas
        """
        # Obter configuração atual e criar limiter específico para o tipo
        viz_config = get_visualization_config()
        category_limiter = self._get_category_limiter(chart_type)

        # Verificar se precisa limitar
        if not category_limiter.should_limit(data):
            self.logger.debug(
                f"Dataset tem {len(data)} categorias, "
                f"dentro do limite - sem limitação aplicada"
            )
            # Armazenar dados originais (sem limitação)
            self._last_limited_data = data
            self._last_limit_metadata = {
                "original_count": len(data),
                "limited_count": len(data),
                "categories_excluded": 0,
                "others_created": False,
                "limit_applied": False,
            }
            return data, self._last_limit_metadata

        # Determinar se deve criar categoria "OUTROS"
        create_others = viz_config.should_create_others_category(chart_type)
        others_label = viz_config.others_label

        # Aplicar limitação
        limited_data, metadata = category_limiter.limit_categories(
            data=data,
            category_column=category_column,
            metric_column=metric_column,
            create_others=create_others,
            others_label=others_label,
            ascending=False,  # Sempre ordenar descendente (maiores valores primeiro)
        )

        # Adicionar flag de que limitação foi aplicada
        metadata["limit_applied"] = True
        metadata["chart_type"] = chart_type

        # Armazenar dados e metadata para acesso posterior
        self._last_limited_data = limited_data
        self._last_limit_metadata = metadata

        # Log informativo
        if create_others and metadata["others_created"]:
            self.logger.info(
                f"[{chart_type}] Limitado de {metadata['original_count']} para "
                f"{viz_config.max_categories} categorias. "
                f"{metadata['categories_excluded']} categorias agregadas em "
                f"'{others_label}' (total: {metadata['others_value']:,.2f})"
            )
        else:
            self.logger.info(
                f"[{chart_type}] Limitado de {metadata['original_count']} para "
                f"{metadata['limited_count']} categorias. "
                f"{metadata['categories_excluded']} categorias excluídas."
            )

        return limited_data, metadata
