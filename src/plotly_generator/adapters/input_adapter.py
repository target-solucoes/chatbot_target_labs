"""
InputAdapter - Adapter para processar inputs do graphical_classifier e analytics_executor.

Extrai e normaliza informacoes necessarias para geracao do grafico.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import pandas as pd

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PlotParams:
    """
    Parametros extraidos e prontos para plotagem.

    Estrutura interna utilizada pelos generators para receber dados
    ja processados e normalizados.

    Attributes:
        chart_type: Tipo de grafico (ex: "bar_horizontal")
        title: Titulo do grafico
        description: Descricao/subtitulo opcional
        data: DataFrame com dados prontos para plotagem
        dimension_columns: Lista de nomes/aliases das dimensions
        metric_columns: Lista de nomes/aliases das metrics
        palette: Nome da paleta de cores
        show_values: Se deve exibir valores nas barras/pontos
        visual_config: Configuracoes visuais adicionais
    """
    chart_type: str
    title: str
    description: Optional[str]
    data: pd.DataFrame
    dimension_columns: List[str]
    metric_columns: List[str]
    palette: str
    show_values: bool
    visual_config: Dict[str, Any]


class InputAdapter:
    """
    Adapter para processar inputs do graphical_classifier e analytics_executor.

    Responsavel por:
    - Validar consistencia entre ChartOutput e AnalyticsOutput
    - Extrair mapeamento de colunas (nome -> alias)
    - Converter dados para formato interno (PlotParams)
    - Validar existencia de colunas nos dados

    Exemplo:
        >>> adapter = InputAdapter()
        >>> plot_params = adapter.adapt(chart_spec, analytics_result)
        >>> print(plot_params.chart_type)
        'bar_horizontal'
    """

    def __init__(self):
        """Inicializa o InputAdapter."""
        self.logger = get_logger(self.__class__.__name__)
        self.logger.debug("InputAdapter inicializado")

    def adapt(
        self,
        chart_spec: Dict[str, Any],
        analytics_result: Dict[str, Any]
    ) -> PlotParams:
        """
        Adapta inputs para formato interno usado pelos generators.

        Processo:
        1. Validar inputs basicos
        2. Extrair dados do analytics_result
        3. Extrair aliases de dimensions e metrics
        4. Validar consistencia (colunas existem nos dados)
        5. Converter para DataFrame
        6. Montar PlotParams

        Args:
            chart_spec: ChartOutput do graphical_classifier
            analytics_result: AnalyticsOutput do analytics_executor

        Returns:
            PlotParams com dados prontos para plotagem

        Raises:
            ValueError: Se inputs invalidos ou inconsistentes
        """
        self.logger.debug("Iniciando adaptacao de inputs")

        # Validar inputs basicos
        self._validate_inputs(chart_spec, analytics_result)

        # Extrair dados
        data_list = analytics_result.get("data", [])
        if not data_list:
            raise ValueError("analytics_result.data esta vazio")

        # Extrair aliases
        dimension_columns = self._extract_dimension_aliases(chart_spec)
        metric_columns = self._extract_metric_aliases(chart_spec)

        self.logger.debug(
            f"Dimensions: {dimension_columns}, Metrics: {metric_columns}"
        )

        # Validar consistencia
        self.validate_data_consistency(chart_spec, data_list)

        # Converter para DataFrame
        df = pd.DataFrame(data_list)

        # Para line charts, usar valores convertidos do plotly_config se disponível
        chart_type = chart_spec.get("chart_type", "")
        if chart_type in ["line", "line_composed"]:
            df = self._apply_converted_values_from_plotly_config(
                df, analytics_result, dimension_columns
            )

        # Extrair configuracoes visuais
        visual_config = chart_spec.get("visual", {})
        palette = visual_config.get("palette", "Blues")
        show_values = visual_config.get("show_values", False)

        # Montar PlotParams
        plot_params = PlotParams(
            chart_type=chart_spec.get("chart_type", ""),
            title=chart_spec.get("title", ""),
            description=chart_spec.get("description"),
            data=df,
            dimension_columns=dimension_columns,
            metric_columns=metric_columns,
            palette=palette,
            show_values=show_values,
            visual_config=visual_config
        )

        self.logger.info(
            f"Adaptacao concluida: {len(data_list)} linhas, "
            f"{len(dimension_columns)} dimensions, {len(metric_columns)} metrics"
        )

        return plot_params

    def extract_column_mappings(
        self,
        chart_spec: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Extrai mapeamento column_name -> alias.

        Args:
            chart_spec: ChartOutput

        Returns:
            Dicionario com mapeamento {nome_original: alias}

        Exemplo:
            >>> chart_spec = {
            ...     "dimensions": [{"name": "Produto", "alias": "Nome do Produto"}],
            ...     "metrics": [{"name": "Qtd_Vendida", "alias": "Quantidade"}]
            ... }
            >>> adapter.extract_column_mappings(chart_spec)
            {'Produto': 'Nome do Produto', 'Qtd_Vendida': 'Quantidade'}
        """
        mappings = {}

        # Dimensions
        for dim in chart_spec.get("dimensions", []):
            name = dim.get("name")
            alias = dim.get("alias", name)
            if name:
                mappings[name] = alias

        # Metrics
        for metric in chart_spec.get("metrics", []):
            name = metric.get("name")
            alias = metric.get("alias", name)
            if name:
                mappings[name] = alias

        self.logger.debug(f"Mapeamentos extraidos: {len(mappings)} colunas")
        return mappings

    def validate_data_consistency(
        self,
        chart_spec: Dict[str, Any],
        data: List[Dict[str, Any]]
    ) -> None:
        """
        Valida que colunas referenciadas no spec existem nos dados.

        Verifica que todos os aliases de dimensions e metrics estao
        presentes como chaves nos dados.

        Args:
            chart_spec: ChartOutput
            data: Dados do analytics_executor

        Raises:
            ValueError: Se colunas faltando nos dados
        """
        if not data:
            raise ValueError("Dados vazios")

        # Colunas disponiveis nos dados
        available_columns = set(data[0].keys())

        # Colunas requeridas
        dimension_aliases = self._extract_dimension_aliases(chart_spec)
        metric_aliases = self._extract_metric_aliases(chart_spec)
        required_columns = set(dimension_aliases + metric_aliases)

        # Verificar colunas faltantes
        missing_columns = required_columns - available_columns

        if missing_columns:
            raise ValueError(
                f"Colunas faltando nos dados: {sorted(missing_columns)}. "
                f"Colunas disponiveis: {sorted(available_columns)}"
            )

        self.logger.debug(
            f"Validacao de consistencia OK: {len(required_columns)} colunas encontradas"
        )

    def _validate_inputs(
        self,
        chart_spec: Dict[str, Any],
        analytics_result: Dict[str, Any]
    ) -> None:
        """
        Valida inputs basicos.

        Args:
            chart_spec: ChartOutput
            analytics_result: AnalyticsOutput

        Raises:
            ValueError: Se inputs invalidos
        """
        if not chart_spec:
            raise ValueError("chart_spec nao pode ser vazio")

        if not chart_spec.get("chart_type"):
            raise ValueError("chart_spec.chart_type e obrigatorio")

        if not analytics_result:
            raise ValueError("analytics_result nao pode ser vazio")

        if analytics_result.get("status") != "success":
            raise ValueError(
                f"analytics_result.status deve ser 'success', "
                f"recebido: '{analytics_result.get('status')}'"
            )

    def _apply_converted_values_from_plotly_config(
        self,
        df: pd.DataFrame,
        analytics_result: Dict[str, Any],
        dimension_columns: List[str]
    ) -> pd.DataFrame:
        """
        Aplica valores convertidos do plotly_config ao DataFrame.

        Para line charts com valores temporais, o Analytics Executor converte
        valores numéricos (ex: mês 1-12) em strings de data (ex: "2015-01").
        Esses valores convertidos estão em plotly_config.data[].x.

        Args:
            df: DataFrame com dados originais
            analytics_result: Resultado do analytics_executor
            dimension_columns: Lista de colunas de dimensão

        Returns:
            DataFrame com valores convertidos aplicados
        """
        plotly_config = analytics_result.get("plotly_config", {})
        if not plotly_config:
            return df

        data_traces = plotly_config.get("data", [])
        if not data_traces:
            return df

        # Para line chart simples, primeira trace
        if len(data_traces) == 1 and dimension_columns:
            dimension_col = dimension_columns[0]
            converted_x_values = data_traces[0].get("x", [])

            if converted_x_values and len(converted_x_values) == len(df):
                # Substituir valores da coluna de dimensão pelos valores convertidos
                df = df.copy()
                df[dimension_col] = converted_x_values
                self.logger.info(
                    f"Applied converted values from plotly_config to column '{dimension_col}': "
                    f"{df[dimension_col].iloc[0] if len(df) > 0 else 'N/A'}"
                )

        # Para line_composed, múltiplas traces
        elif len(data_traces) > 1 and len(dimension_columns) >= 1:
            # Construir mapeamento de valores convertidos
            temporal_col = dimension_columns[0]

            # Coletar todos os valores únicos de X de todas as traces
            all_converted_x = []
            for trace in data_traces:
                x_values = trace.get("x", [])
                all_converted_x.extend(x_values)

            # Obter valores únicos mantendo ordem
            unique_converted_x = list(dict.fromkeys(all_converted_x))

            if unique_converted_x:
                # Criar mapeamento de índice -> valor convertido
                # Assumindo que os valores originais são sequenciais (1, 2, 3, ...)
                original_values = df[temporal_col].unique()
                if len(original_values) <= len(unique_converted_x):
                    # Mapear valores originais para convertidos
                    value_map = {}
                    for i, orig_val in enumerate(sorted(original_values)):
                        if i < len(unique_converted_x):
                            value_map[orig_val] = unique_converted_x[i]

                    if value_map:
                        df = df.copy()
                        df[temporal_col] = df[temporal_col].map(value_map)
                        self.logger.info(
                            f"Applied converted values from plotly_config to column '{temporal_col}' "
                            f"(line_composed): {len(value_map)} values mapped"
                        )

        return df

    def _extract_dimension_aliases(
        self,
        chart_spec: Dict[str, Any]
    ) -> List[str]:
        """
        Extrai lista de aliases de dimensions.

        Args:
            chart_spec: ChartOutput

        Returns:
            Lista de aliases (ou nomes se alias nao existir)
        """
        dimensions = chart_spec.get("dimensions", [])
        aliases = []

        for dim in dimensions:
            alias = dim.get("alias") or dim.get("name")
            if alias:
                aliases.append(alias)

        return aliases

    def _extract_metric_aliases(
        self,
        chart_spec: Dict[str, Any]
    ) -> List[str]:
        """
        Extrai lista de aliases de metrics.

        Args:
            chart_spec: ChartOutput

        Returns:
            Lista de aliases (ou nomes se alias nao existir)
        """
        metrics = chart_spec.get("metrics", [])
        aliases = []

        for metric in metrics:
            alias = metric.get("alias") or metric.get("name")
            if alias:
                aliases.append(alias)

        return aliases
