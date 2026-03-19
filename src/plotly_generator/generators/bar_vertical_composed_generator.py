"""
BarVerticalComposedGenerator - Generator para graficos de barras verticais compostas.

Conforme axis_patterns.md:
- Eixo X: Categoria ou Periodo
- Eixo Y: Metrica quantitativa
- Agrupamento: Categoria Secundaria (barras agrupadas)
- Uso tipico: Comparacoes entre grupos em periodos distintos
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from numbers import Number
import re
import unicodedata
import plotly.graph_objects as go
import pandas as pd

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.plotly_generator.utils.color_manager import ColorManager
from src.plotly_generator.utils.bar_aesthetics import BarAesthetics
from src.shared_lib.utils.logger import get_logger
from src.shared_lib.utils.temporal_formatter import (
    format_temporal_value,
    is_temporal_dimension,
    get_temporal_sort_key,
)

logger = get_logger(__name__)


class BarVerticalComposedGenerator(BasePlotlyGenerator):
    """
    Generator para graficos de barras verticais compostas (agrupadas).

    Requisitos (conforme axis_patterns.md):
    - Eixo X: Categoria principal ou Periodo (primeira dimension)
    - Eixo Y: Metrica quantitativa
    - Agrupamento: Categoria secundaria (segunda dimension)
    - Cada grupo e uma barra com cor distinta

    Validacao:
    - Exatamente 2 dimensions (categoria principal + categoria secundaria)
    - Pelo menos 1 metric
    - Dados nao vazios

    Exemplo de Uso:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> generator = BarVerticalComposedGenerator(PlotStyler())
        >>> chart_spec = {
        ...     "chart_type": "bar_vertical_composed",
        ...     "title": "Vendas por Regiao e Ano",
        ...     "dimensions": [
        ...         {"name": "Ano", "alias": "Ano"},
        ...         {"name": "Regiao", "alias": "Regiao"}
        ...     ],
        ...     "metrics": [{"name": "Vendas", "alias": "Total Vendas"}],
        ...     "visual": {"palette": "Set1", "show_values": True}
        ... }
        >>> data = [
        ...     {"Ano": "2023", "Regiao": "Norte", "Total Vendas": 5000},
        ...     {"Ano": "2023", "Regiao": "Sul", "Total Vendas": 7000},
        ...     {"Ano": "2015", "Regiao": "Norte", "Total Vendas": 6000},
        ...     {"Ano": "2015", "Regiao": "Sul", "Total Vendas": 8000}
        ... ]
        >>> fig = generator.generate(chart_spec, data)
        >>> type(fig)
        <class 'plotly.graph_objs._figure.Figure'>
    """

    def __init__(self, styler):
        """Inicializa o generator com styler, color manager e bar aesthetics."""
        super().__init__(styler)
        self.color_manager = ColorManager()
        self.bar_aesthetics = BarAesthetics()

    def validate(self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]) -> None:
        """
        Valida requisitos de bar_vertical_composed.

        Validacoes:
        1. Exatamente 2 dimensions (principal + secundaria)
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
                f"bar_vertical_composed requer exatamente 2 dimensions, "
                f"recebeu {len(dimensions)}"
            )

        # Validar metrics
        metrics = chart_spec.get("metrics", [])
        if len(metrics) < 1:
            raise ValueError("bar_vertical_composed requer pelo menos 1 metric")

        # Validar dados nao vazios
        if not data:
            raise ValueError("Dados vazios - nao e possivel gerar grafico")

        self.logger.debug(
            f"Validacao OK: {len(dimensions)} dimensions, "
            f"{len(metrics)} metric(s), {len(data)} linhas"
        )

    def _validate_temporal_periods(
        self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> Optional[str]:
        """
        Valida numero de periodos temporais.

        Se > 2 periodos temporais na primeira dimensao, sugere line_composed
        para melhor visualizacao de tendencias ao longo do tempo.

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Returns:
            Warning message se aplicavel, None caso contrario

        Example:
            >>> warning = self._validate_temporal_periods(chart_spec, data)
            >>> if warning:
            ...     logger.warning(warning)
        """
        dimensions = chart_spec.get("dimensions", [])
        if len(dimensions) < 1:
            return None

        # Verificar se primeira dimensao e temporal
        main_dimension_name = dimensions[0].get("name", "")
        if not is_temporal_dimension(main_dimension_name):
            return None

        # Contar periodos unicos
        df = pd.DataFrame(data)
        filter_year = self._extract_filter_year(chart_spec)
        filter_year = self._extract_filter_year(chart_spec)
        main_alias = self._get_dimension_alias(chart_spec, 0)

        if main_alias not in df.columns:
            return None

        unique_periods = df[main_alias].nunique()

        # Se > 2 periodos, sugerir line_composed
        if unique_periods > 2:
            return (
                f"SUGESTAO: Detectados {unique_periods} periodos temporais na dimensao '{main_dimension_name}'. "
                f"Para mais de 2 periodos, considere usar 'line_composed' "
                f"para melhor visualizacao de tendencias ao longo do tempo. "
                f"Graficos de barras compostas sao otimos para comparar 2 periodos discretos, "
                f"mas graficos de linha sao mais eficientes para visualizar multiplos periodos."
            )

        return None

    def generate(
        self, chart_spec: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> go.Figure:
        """
        Gera grafico de barras verticais compostas (agrupadas).

        Logica:
        1. Extrair aliases das dimensions e metric
        2. Aplicar limitação nas categorias principais (primeira dimension)
        3. Identificar categorias unicas da dimension secundaria
        4. Criar uma trace (conjunto de barras) por categoria secundaria
        5. Aplicar cores distintas para cada grupo
        6. Configurar layout e eixos

        Args:
            chart_spec: ChartOutput do graphical_classifier
            data: Dados processados do analytics_executor

        Returns:
            go.Figure com barras verticais agrupadas
        """
        self.logger.info("Gerando grafico bar_vertical_composed")

        # Validar numero de periodos e avisar se necessario
        period_warning = self._validate_temporal_periods(chart_spec, data)
        if period_warning:
            self.logger.warning(period_warning)
            # Continua gerando o grafico (apenas aviso, nao bloqueia)

        # Extrair aliases
        main_category_alias = self._get_dimension_alias(
            chart_spec, 0
        )  # Período (eixo X)
        secondary_category_alias = self._get_dimension_alias(
            chart_spec, 1
        )  # Categoria (barras agrupadas)
        metric_alias = self._get_metric_alias(chart_spec, 0)  # Métrica (eixo Y)

        df = pd.DataFrame(data)
        filter_year = self._extract_filter_year(chart_spec)

        # DETECTAR SE PRIMEIRA DIMENSÃO É TEMPORAL
        main_dimension_name = chart_spec["dimensions"][0].get(
            "name", main_category_alias
        )
        is_temporal = is_temporal_dimension(main_dimension_name)

        if is_temporal:
            self.logger.info(
                f"Dimensão temporal detectada: '{main_dimension_name}'. "
                f"Aplicando formatação e ordenação cronológica."
            )

        # APLICAR LIMITAÇÃO NA SEGUNDA DIMENSÃO (categorias a comparar)
        # NÃO na primeira dimensão (períodos temporais)
        # Calcular total por categoria SECUNDÁRIA para ordenação
        totals_by_secondary = (
            df.groupby(secondary_category_alias)[metric_alias].sum().reset_index()
        )
        totals_by_secondary.columns = [secondary_category_alias, "_total_"]

        # Limitar categorias secundárias (NÃO os períodos!)
        limited_totals, limit_metadata = self._apply_category_limit(
            data=totals_by_secondary.to_dict("records"),
            chart_type="bar_vertical_composed",
            category_column=secondary_category_alias,  # Limitar categorias
            metric_column="_total_",
        )

        # Extrair categorias secundárias a manter
        top_secondary_categories = [
            row[secondary_category_alias] for row in limited_totals
        ]

        # Filtrar dados para manter apenas categorias selecionadas
        df = df[df[secondary_category_alias].isin(top_secondary_categories)]

        if limit_metadata["limit_applied"]:
            self.logger.info(
                f"Limitadas categorias secundárias de {limit_metadata['original_count']} "
                f"para {len(top_secondary_categories)} (segunda dimensão preservando todos os períodos)"
            )

        self.logger.debug(
            f"Categorias: principal='{main_category_alias}', "
            f"secundaria='{secondary_category_alias}', metrica='{metric_alias}'"
        )

        # Identificar categorias unicas da dimension secundaria
        secondary_categories = df[secondary_category_alias].unique().tolist()
        n_categories = len(secondary_categories)

        self.logger.debug(
            f"Encontradas {n_categories} categorias secundarias: {secondary_categories}"
        )

        # CRIAR MAPEAMENTO DE VALORES TEMPORAIS PARA EIXO X
        # Extrair valores únicos da primeira dimensão (eixo X - períodos)
        x_values_raw = df[main_category_alias].unique()

        if is_temporal:
            # ORDENAR CRONOLOGICAMENTE
            x_values_sorted = sorted(
                x_values_raw,
                key=lambda v: get_temporal_sort_key(v, main_dimension_name),
            )

            # Detectar coluna de ano com heurísticas (ex: Ano_Venda, year)
            year_column = self._find_year_column(df)
            parsed_year_values: List[Optional[int]] = []
            if year_column:
                parsed_year_values = [
                    self._parse_year_value(value) for value in df[year_column].tolist()
                ]
                if not any(val is not None for val in parsed_year_values):
                    self.logger.warning(
                        f"Coluna '{year_column}' identificada como ano, mas sem valores válidos; ignorando."
                    )
                    year_column = None
                    parsed_year_values = []

            # FORMATAR PARA NOMES LEGÍVEIS (incluindo ano se disponível)
            # Verificar se os valores são timestamps (já contêm ano embutido)
            first_value = x_values_sorted[0] if len(x_values_sorted) > 0 else None
            is_timestamp = isinstance(first_value, (pd.Timestamp, datetime))
            dimension_accepts_year = self._dimension_supports_year_suffix(
                main_dimension_name
            )

            if is_timestamp:
                # Valores são timestamps: formato já inclui ano automaticamente
                unique_x_categories = [
                    format_temporal_value(val, main_dimension_name)
                    for val in x_values_sorted
                ]
                self.logger.info("Formatando timestamps com ano embutido")

            elif year_column and dimension_accepts_year:
                valid_years = [val for val in parsed_year_values if val is not None]

                if len(set(valid_years)) == 1 and valid_years:
                    year_value = valid_years[0]
                    unique_x_categories = [
                        format_temporal_value(val, main_dimension_name, year_value)
                        for val in x_values_sorted
                    ]
                    self.logger.info(f"Formatando com ano único: {year_value}")
                else:
                    # Múltiplos anos: criar mapeamento período -> ano
                    period_to_year = {}
                    for period_val, year_val in zip(
                        df[main_category_alias].tolist(), parsed_year_values
                    ):
                        if year_val is None or period_val in period_to_year:
                            continue
                        period_to_year[period_val] = year_val

                    unique_x_categories = [
                        format_temporal_value(
                            val, main_dimension_name, period_to_year.get(val)
                        )
                        for val in x_values_sorted
                    ]
                    distinct_years = {
                        val for val in period_to_year.values() if val is not None
                    }
                    self.logger.info(
                        f"Formatando com múltiplos anos mapeados: {len(distinct_years)} anos distintos"
                    )

            elif filter_year is not None and dimension_accepts_year:
                unique_x_categories = [
                    format_temporal_value(val, main_dimension_name, filter_year)
                    for val in x_values_sorted
                ]
                self.logger.info(
                    f"Formatando com ano derivado de filtros: {filter_year}"
                )

            else:
                # Sem coluna ano e sem filtro/ timestamp: formatar apenas período
                unique_x_categories = [
                    format_temporal_value(val, main_dimension_name)
                    for val in x_values_sorted
                ]
                self.logger.debug(
                    "Formatando sem ano (coluna dedicada ou filtro de ano não encontrados)"
                )

            # CRIAR MAPEAMENTO: valor_bruto -> valor_formatado
            x_value_mapping = dict(zip(x_values_sorted, unique_x_categories))

            self.logger.info(
                f"Dimensão temporal '{main_dimension_name}': "
                f"ordenados e formatados {len(unique_x_categories)} períodos: {unique_x_categories}"
            )
        else:
            # Não temporal: manter original
            unique_x_categories = x_values_raw.tolist()
            x_value_mapping = {v: v for v in unique_x_categories}

        # Obter paleta de cores
        palette = chart_spec.get("visual", {}).get("palette", "Set1")
        color_mapping = self.color_manager.get_color_sequence(
            secondary_categories, palette
        )

        # Criar traces - uma para cada categoria secundaria
        traces = []
        show_values = chart_spec.get("visual", {}).get("show_values", False)

        for category in secondary_categories:
            # Filtrar dados para esta categoria secundaria
            category_data = df[df[secondary_category_alias] == category]

            # Extrair valores brutos e aplicar mapeamento temporal
            x_values_raw = category_data[main_category_alias].tolist()
            # Aplicar mapeamento de formatação temporal (se aplicável)
            x_values = [x_value_mapping.get(val, val) for val in x_values_raw]
            y_values = category_data[metric_alias].tolist()

            # Formatar valores para exibição (K/M) se show_values estiver habilitado
            text_values = (
                self._format_numbers_compact(y_values) if show_values else None
            )

            # Criar trace
            trace = go.Bar(
                x=x_values,
                y=y_values,
                name=str(category),
                marker=dict(color=color_mapping[category]),
                text=text_values,
                textposition="outside" if show_values else None,
                texttemplate="%{text}" if show_values else None,
                cliponaxis=False,  # Evitar corte de valores nas barras
            )

            traces.append(trace)

            self.logger.debug(
                f"Trace criada para categoria '{category}' com {len(y_values)} barras"
            )

        # Criar figure
        fig = go.Figure(data=traces)

        # Aplicar layout comum
        self._apply_common_layout(fig, chart_spec)

        # Configurar modo de barras agrupadas
        fig.update_layout(
            barmode="group",
            xaxis_title=main_category_alias,
            yaxis_title=metric_alias,
            legend=dict(
                title=dict(text=secondary_category_alias),
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
            ),
        )

        # Configurar eixos com tratamento automático de rótulos longos
        # (unique_x_categories e x_value_mapping já foram criados antes do loop de traces)

        # Determinar se precisa forçar rótulos verticais
        # Apenas forçar se houver muitos labels ou se são longos
        max_label_length = (
            max(len(str(label)) for label in unique_x_categories)
            if unique_x_categories
            else 0
        )
        force_vertical = len(unique_x_categories) > 5 or max_label_length > 15

        self.styler.apply_axis_config(
            fig,
            x_title=main_category_alias,
            y_title=metric_alias,
            x_type="category",
            y_type="linear",
            x_labels=unique_x_categories,  # Passar categorias formatadas para tratamento de texto longo
            force_vertical_labels=force_vertical,  # Forçar vertical apenas se necessário
        )

        # APLICAR ESTÉTICA PADRONIZADA (tamanhos de fonte consistentes com bar_horizontal)
        self.bar_aesthetics.apply_vertical_bar_style(
            fig,
            categories=unique_x_categories,
            show_grid=True,
            rotate_labels=force_vertical,
            rotation_angle=-45 if force_vertical else 0,
        )

        # Configurar fonte dos valores nas barras (se show_values=True)
        if show_values:
            self.bar_aesthetics.configure_bar_value_labels(fig, orientation="v")

        self.logger.info(
            f"Grafico bar_vertical_composed gerado com {len(traces)} grupos, "
            f"{len(data)} barras totais"
        )

        return fig

    @staticmethod
    def _normalize_column_name(column_name: str) -> str:
        """Normaliza nome de coluna removendo acentos e inserindo separadores."""
        normalized = unicodedata.normalize("NFKD", str(column_name))
        ascii_only = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        ascii_only = re.sub(r"(?<=[a-z])([A-Z])", r"_\1", ascii_only)
        return ascii_only.lower()

    @classmethod
    def _tokenize_column_name(cls, column_name: str) -> List[str]:
        """Divide nome de coluna em tokens alfanuméricos normalizados."""
        normalized = cls._normalize_column_name(column_name)
        return [token for token in re.split(r"[^a-z0-9]+", normalized) if token]

    def _find_year_column(self, df: pd.DataFrame) -> Optional[str]:
        """Tenta localizar coluna relacionada a ano (ex: Ano, Ano_Venda, Year)."""
        for column in df.columns:
            tokens = self._tokenize_column_name(column)
            if any(token in {"ano", "year"} for token in tokens):
                return column
        return None

    @staticmethod
    def _flatten_filter_payload(payload: Any) -> List[Any]:
        """Flattens nested filter payloads into a simple list of values."""
        if isinstance(payload, dict):
            flattened: List[Any] = []
            for value in payload.values():
                flattened.extend(
                    BarVerticalComposedGenerator._flatten_filter_payload(value)
                )
            return flattened

        if isinstance(payload, (list, tuple, set)):
            flattened: List[Any] = []
            for value in payload:
                flattened.extend(
                    BarVerticalComposedGenerator._flatten_filter_payload(value)
                )
            return flattened

        return [payload]

    @staticmethod
    def _parse_year_value(value: Any) -> Optional[int]:
        """Converte valores potencialmente numéricos em ano inteiro."""
        if value is None or value == "":
            return None

        if isinstance(value, bool):
            return None

        if isinstance(value, Number):
            number_value = float(value)
            if number_value.is_integer():
                return int(number_value)
            return None

        if isinstance(value, str):
            digits_match = re.search(r"(\d{4})", value)
            if digits_match:
                return int(digits_match.group(1))

        return None

    def _extract_filter_year(self, chart_spec: Dict[str, Any]) -> Optional[int]:
        """Obtém ano único explicitado em filtros (ex: Ano = 2015)."""
        filters = chart_spec.get("filters") or {}
        for raw_key, raw_value in filters.items():
            tokens = self._tokenize_column_name(raw_key)
            if not any(token in {"ano", "year"} for token in tokens):
                continue

            flattened_values = self._flatten_filter_payload(raw_value)
            parsed_years = {
                self._parse_year_value(value) for value in flattened_values
            } - {None}

            if len(parsed_years) == 1:
                return parsed_years.pop()

        return None

    def _dimension_supports_year_suffix(self, dimension_name: str) -> bool:
        """Indica se a dimensão temporal aceita complemento de ano (mes, trimestre, semestre)."""
        tokens = self._tokenize_column_name(dimension_name)
        supported = {"mes", "month", "trimestre", "quarter", "semestre", "semester"}
        return any(token in supported for token in tokens)
