"""
LLM-based intent analyzer for non_graph_executor.

This module implements the IntentAnalyzer class, which uses an LLM to
understand the complete semantic intent of a user's query and produce
a structured QueryIntent specification.

The IntentAnalyzer replaces keyword-based classification with genuine
semantic comprehension, enabling the system to correctly handle queries
involving GROUP BY, ORDER BY, temporal functions, rankings, and other
analytical patterns that the old keyword approach could not support.

Key responsibilities:
- Receive user query + schema context (alias.yaml, column types)
- Use LLM (gemini-2.5-flash) to interpret the full intent
- Produce a validated QueryIntent with columns, aggregations, grouping, etc.
- Resolve virtual columns (Ano → YEAR("Data"), Mes → MONTH("Data"))
"""

import json
import logging
import re
from typing import Any, Dict, Optional

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI

from src.non_graph_executor.models.intent_schema import (
    AggregationSpec,
    ColumnSpec,
    OrderSpec,
    QueryIntent,
)
from src.shared_lib.core.config import (
    get_metric_columns,
    get_dimension_columns,
    get_temporal_columns,
    get_default_metric,
    load_alias_data,
    build_keyword_to_column_map,
)

logger = logging.getLogger(__name__)


# =============================================================================
# SCHEMA CONTEXT BUILDER
# =============================================================================


def build_schema_context(alias_mapper) -> str:
    """
    Constrói o contexto de schema compacto para inclusão no prompt do LLM.

    Extrai do AliasMapper:
    - Tipos de colunas (numeric, categorical, temporal)
    - Mapeamento de aliases principais (termos comuns → colunas reais)
    - Registro de colunas virtuais e suas expressões SQL

    Args:
        alias_mapper: AliasMapper instance com aliases carregados

    Returns:
        String formatada com o contexto do schema para o prompt
    """
    lines = []

    # 1. Column types
    lines.append("## Tipos de Colunas do Dataset")
    column_types = getattr(alias_mapper, "column_types", {})
    for col_type, columns in column_types.items():
        lines.append(f"- {col_type}: {', '.join(columns)}")

    # 2. Virtual columns (only if they exist)
    virtual_map = getattr(alias_mapper, "VIRTUAL_COLUMN_MAP", {})
    if virtual_map:
        lines.append("\n## Colunas Virtuais (NAO existem fisicamente no dataset)")
        temporal_col = None
        try:
            from src.shared_lib.core.dataset_config import DatasetConfig

            temporal_col = DatasetConfig.get_instance().temporal_column_name
        except Exception:
            pass
        if temporal_col:
            lines.append(
                f"Estas colunas sao derivadas da coluna '{temporal_col}' via expressoes SQL:"
            )
        else:
            lines.append("Estas colunas sao derivadas via expressoes SQL:")
        for col_name, expression in virtual_map.items():
            lines.append(f"- {col_name} -> {expression}")

    # 3. Key alias mappings (most common terms)
    lines.append("\n## Mapeamento de Aliases (termos do usuário → colunas reais)")
    aliases = getattr(alias_mapper, "aliases", {})
    columns_section = aliases.get("columns", {})

    for col_name, alias_list in columns_section.items():
        if isinstance(alias_list, list) and alias_list:
            # Show up to 5 most relevant aliases
            preview = alias_list[:5]
            lines.append(f"- {col_name}: {', '.join(str(a) for a in preview)}")

    return "\n".join(lines)


# =============================================================================
# SYSTEM PROMPT (construido dinamicamente)
# =============================================================================


def _build_system_prompt() -> str:
    """
    Constroi o system prompt dinamicamente com base nas colunas do alias.yaml.

    Regras temporais (colunas virtuais Ano/Mes, temporal_analysis) sao incluidas
    apenas se existirem colunas temporais no dataset.

    Returns:
        String com o system prompt completo
    """
    try:
        temporal_cols = get_temporal_columns()
        has_temporal = len(temporal_cols) > 0

        parts = []
        parts.append(
            "Voce e um analisador semantico especializado em interpretar "
            "perguntas sobre dados do dataset."
        )
        parts.append("")
        parts.append(
            "Sua tarefa e analisar a query do usuario e produzir uma "
            "especificacao JSON estruturada descrevendo EXATAMENTE o que "
            "o usuario quer saber."
        )
        parts.append("")
        parts.append("## Regras Fundamentais")
        parts.append("")
        parts.append(
            "1. **Entenda a INTENCAO REAL**: Identifique o que o usuario "
            "realmente quer como resposta."
        )
        parts.append(
            "2. **Identifique TODAS as dimensoes**: Se a query menciona "
            "agrupamento, identifique a coluna de agrupamento e a agregacao."
        )
        if has_temporal:
            parts.append(
                '3. **Colunas virtuais**: "Ano" e "Mes" NAO sao colunas '
                'fisicas. Use os NOMES das colunas virtuais (ex: "Ano", '
                '"Mes", "Nome_Mes"). O sistema resolve automaticamente as '
                "expressoes SQL correspondentes. NUNCA inclua expressoes SQL "
                'no campo "expression", sempre use null.'
            )
        parts.append(
            '4. **Rankings**: "top 5 X por Y" = agrupar por X, agregar Y, '
            "ordenar DESC, limit 5."
        )
        if has_temporal:
            parts.append(
                '5. **Temporal**: "ultimo/mais recente" aplicado a uma dimensao '
                'temporal (ex: "qual o ultimo ano com dados?") = MAX sobre '
                'dimensao temporal (temporal_analysis). Porem, "ultimo mes" '
                'como FILTRO (ex: "vendas no ultimo mes") NAO e MAX — e uma '
                "simple_aggregation com SUM e filtro temporal ja aplicado."
            )
        parts.append(
            '6. **"qual X teve maior/menor Y?"** = grouped_aggregation: '
            "agrupar por X, SUM/agregar Y, ordenar DESC/ASC, limit 1."
        )

        # Add metric and aggregation rules from alias.yaml
        default_metric = get_default_metric()
        metric_cols = get_metric_columns()
        if default_metric or metric_cols:
            parts.append("")
            parts.append("## Regras de Selecao de Metrica e Agregacao")
            parts.append("")
            if metric_cols:
                alias_data = load_alias_data()
                columns_section = alias_data.get("columns", {})
                for mc in metric_cols:
                    aliases = columns_section.get(mc, [])
                    if aliases:
                        parts.append(
                            f'- Se o usuario mencionar algum destes termos: '
                            f'{", ".join(str(a) for a in aliases[:8])} '
                            f'→ usar coluna "{mc}"'
                        )
            if default_metric:
                parts.append(
                    f'- Se o usuario NAO especificar qual metrica quer, '
                    f'usar "{default_metric}" como metrica padrao'
                )
            parts.append(
                "- Para perguntas sobre totais, sell-out, faturamento, vendas: "
                "usar SUM como agregacao padrao (NAO usar MAX ou MIN)"
            )
            parts.append(
                "- MAX e MIN so devem ser usados quando o usuario pede "
                'explicitamente "maior", "menor", "maximo", "minimo" '
                "sobre um VALOR INDIVIDUAL, ou em temporal_analysis"
            )

        parts.append("")
        parts.append("## Tipos de Intencao")
        parts.append("")
        parts.append(
            "- **simple_aggregation**: Agregacao sem agrupamento. "
            'Ex: "total de vendas", "quantos clientes temos?"'
        )
        parts.append(
            "- **grouped_aggregation**: Agregacao COM GROUP BY. "
            'Ex: "vendas por categoria", "qual grupo teve mais vendas?"'
        )
        parts.append(
            "- **ranking**: Top N com agrupamento e ordenacao. "
            'Ex: "top 5 categorias por valor total"'
        )
        if has_temporal:
            parts.append(
                "- **temporal_analysis**: Consulta sobre dimensao temporal. "
                'Ex: "ultimo ano com dados", "quando ocorreu o primeiro registro?"'
            )
        parts.append(
            '- **comparison**: Comparacao entre grupos. Ex: "vendas grupo A vs grupo B"'
        )
        parts.append(
            '- **lookup**: Busca de registro especifico. Ex: "dados do cliente 123"'
        )
        parts.append(
            "- **metadata**: Informacao sobre o dataset. "
            'Ex: "quantas linhas", "quais colunas"'
        )
        parts.append(
            '- **tabular**: Dados brutos. Ex: "mostre a tabela", "mostrar registros"'
        )
        parts.append(
            '- **conversational**: Saudacao ou ajuda. Ex: "ola", "como funciona?"'
        )
        parts.append("")
        parts.append("## Funcoes de Agregacao Disponiveis")
        parts.append("- sum, avg, count, min, max, median, std")
        parts.append("")
        parts.append("## REGRAS CRITICAS DE FORMATO JSON")
        parts.append("")
        parts.append("1. Retorne APENAS JSON puro, sem markdown, sem texto adicional.")
        parts.append(
            '2. O campo "expression" deve ser SEMPRE null. '
            "O sistema resolve expressoes automaticamente."
        )
        if has_temporal:
            parts.append(
                "3. Para colunas virtuais (Ano, Mes, Nome_Mes), marque "
                '"is_virtual": true e "expression": null.'
            )
        parts.append(
            "4. Use apenas aspas duplas no JSON. Nao inclua aspas "
            "escapadas dentro de valores string."
        )
        parts.append("")
        parts.append("## Formato de Resposta")
        parts.append("")
        parts.append("{")
        parts.append('  "intent_type": "string",')
        parts.append(
            '  "select_columns": [{"name": "string", "is_virtual": false, '
            '"expression": null, "alias": "string|null"}],'
        )
        parts.append(
            '  "aggregations": [{"function": "string", "column": '
            '{"name": "string", "is_virtual": false, "expression": null, '
            '"alias": null}, "distinct": false, "alias": "string|null"}],'
        )
        parts.append(
            '  "group_by": [{"name": "string", "is_virtual": false, '
            '"expression": null, "alias": "string|null"}],'
        )
        parts.append(
            '  "order_by": {"column": "string", "direction": "ASC|DESC"} | null,'
        )
        parts.append('  "limit": null,')
        parts.append('  "additional_filters": {},')
        parts.append('  "confidence": 0.0-1.0,')
        parts.append('  "reasoning": "string"')
        parts.append("}")

        return "\n".join(parts)

    except Exception as e:
        logger.warning(
            "[_build_system_prompt] Erro ao construir prompt dinamico: %s. "
            "Usando versao generica.",
            e,
        )
        return (
            "Voce e um analisador semantico especializado em interpretar "
            "perguntas sobre dados do dataset.\n\n"
            "Sua tarefa e analisar a query do usuario e produzir uma "
            "especificacao JSON estruturada.\n\n"
            "Retorne APENAS JSON puro com os campos: intent_type, "
            "select_columns, aggregations, group_by, order_by, limit, "
            "additional_filters, confidence, reasoning."
        )


# =============================================================================
# FEW-SHOT EXAMPLES (construidos dinamicamente)
# =============================================================================


def _get_first_alias(columns_section: dict, col_name: str) -> str:
    """Retorna o primeiro alias de uma coluna, ou o proprio nome."""
    aliases = columns_section.get(col_name, [])
    if isinstance(aliases, list) and aliases:
        return str(aliases[0])
    return col_name


def _build_few_shot_examples() -> str:
    """
    Constroi exemplos few-shot dinamicamente com base nas colunas do alias.yaml.

    Gera exemplos contextualizados usando nomes e aliases REAIS do dataset.
    Exemplos temporais sao incluidos apenas se existirem colunas temporais.

    Returns:
        String com os exemplos formatados para o prompt
    """
    try:
        metric_cols = get_metric_columns()
        dimension_cols = get_dimension_columns()
        temporal_cols = get_temporal_columns()
        alias_data = load_alias_data()
        columns_section = alias_data.get("columns", {})
        has_temporal = len(temporal_cols) > 0

        parts = []
        parts.append("")
        parts.append("## Exemplos de Analise")
        example_num = 1

        # Exemplo: Agregacao simples com primeira metrica
        if metric_cols:
            m1 = metric_cols[0]
            a1 = _get_first_alias(columns_section, m1)
            parts.append("")
            parts.append(f"### Exemplo {example_num}: Agregacao simples")
            parts.append(f'Query: "qual o total de {a1}?"')
            parts.append("Resposta:")
            parts.append(
                '{"intent_type": "simple_aggregation", "select_columns": [], '
                '"aggregations": [{"function": "sum", "column": '
                '{"name": "' + m1 + '", "is_virtual": false, '
                '"expression": null, "alias": null}, "distinct": false, '
                '"alias": "total_' + m1.lower() + '"}], '
                '"group_by": [], "order_by": null, "limit": null, '
                '"additional_filters": {}, "confidence": 0.95, '
                '"reasoning": "Total de ' + a1 + ": SUM " + m1 + '"}'
            )
            example_num += 1

        # Exemplo: Agregacao agrupada
        if metric_cols and dimension_cols:
            m1 = metric_cols[0]
            am = _get_first_alias(columns_section, m1)
            # Escolher coluna de agrupamento adequada
            group_col = None
            for candidate in [
                "Contract",
                "PaymentMethod",
                "InternetService",
                "gender",
            ]:
                if candidate in dimension_cols:
                    group_col = candidate
                    break
            if not group_col:
                group_col = dimension_cols[0]
            ag = _get_first_alias(columns_section, group_col)

            parts.append("")
            parts.append(f"### Exemplo {example_num}: Agregacao agrupada")
            parts.append(f'Query: "{am} por {ag}"')
            parts.append("Resposta:")
            parts.append(
                '{"intent_type": "grouped_aggregation", "select_columns": [], '
                '"aggregations": [{"function": "sum", "column": '
                '{"name": "' + m1 + '", "is_virtual": false, '
                '"expression": null, "alias": null}, "distinct": false, '
                '"alias": "total_' + m1.lower() + '"}], '
                '"group_by": [{"name": "' + group_col + '", '
                '"is_virtual": false, "expression": null, '
                '"alias": "' + group_col + '"}], '
                '"order_by": null, "limit": null, '
                '"additional_filters": {}, "confidence": 0.95, '
                '"reasoning": "' + am + " agrupado por " + ag + ": "
                "GROUP BY " + group_col + ", SUM " + m1 + '"}'
            )
            example_num += 1

        # Exemplo: Ranking
        if metric_cols and len(dimension_cols) > 1:
            m2 = metric_cols[1] if len(metric_cols) > 1 else metric_cols[0]
            am2 = _get_first_alias(columns_section, m2)
            rank_col = None
            for candidate in [
                "PaymentMethod",
                "Contract",
                "InternetService",
            ]:
                if candidate in dimension_cols:
                    rank_col = candidate
                    break
            if not rank_col:
                rank_col = (
                    dimension_cols[1] if len(dimension_cols) > 1 else dimension_cols[0]
                )
            ar = _get_first_alias(columns_section, rank_col)

            parts.append("")
            parts.append(f"### Exemplo {example_num}: Ranking")
            parts.append(f'Query: "top 5 {ar} por {am2}"')
            parts.append("Resposta:")
            parts.append(
                '{"intent_type": "ranking", "select_columns": [], '
                '"aggregations": [{"function": "sum", "column": '
                '{"name": "' + m2 + '", "is_virtual": false, '
                '"expression": null, "alias": null}, "distinct": false, '
                '"alias": "total_' + m2.lower() + '"}], '
                '"group_by": [{"name": "' + rank_col + '", '
                '"is_virtual": false, "expression": null, '
                '"alias": "' + rank_col + '"}], '
                '"order_by": {"column": "total_' + m2.lower() + '", '
                '"direction": "DESC"}, "limit": 5, '
                '"additional_filters": {}, "confidence": 0.95, '
                '"reasoning": "Top 5: ranking com limit=5, agrupar por '
                + rank_col
                + ", SUM "
                + m2
                + ', ORDER DESC"}'
            )
            example_num += 1

        # Exemplo: Contagem distinta
        id_col = None
        for candidate in ["customerID", "Cod_Cliente", "id"]:
            if candidate in dimension_cols:
                id_col = candidate
                break
        if not id_col and dimension_cols:
            id_col = dimension_cols[0]

        if id_col:
            aid = _get_first_alias(columns_section, id_col)
            parts.append("")
            parts.append(f"### Exemplo {example_num}: Contagem distinta")
            parts.append(f'Query: "quantos {aid} temos?"')
            parts.append("Resposta:")
            parts.append(
                '{"intent_type": "simple_aggregation", "select_columns": [], '
                '"aggregations": [{"function": "count", "column": '
                '{"name": "' + id_col + '", "is_virtual": false, '
                '"expression": null, "alias": null}, "distinct": true, '
                '"alias": "total_' + id_col.lower() + '"}], '
                '"group_by": [], "order_by": null, "limit": null, '
                '"additional_filters": {}, "confidence": 0.95, '
                '"reasoning": "Contagem de ' + aid + " unicos: "
                "COUNT DISTINCT " + id_col + '"}'
            )
            example_num += 1

        # Exemplo: Segunda metrica (se existir)
        if len(metric_cols) > 1:
            m2 = metric_cols[1]
            a2 = _get_first_alias(columns_section, m2)
            parts.append("")
            parts.append(f"### Exemplo {example_num}: Agregacao simples")
            parts.append(f'Query: "qual o total de {a2}?"')
            parts.append("Resposta:")
            parts.append(
                '{"intent_type": "simple_aggregation", "select_columns": [], '
                '"aggregations": [{"function": "sum", "column": '
                '{"name": "' + m2 + '", "is_virtual": false, '
                '"expression": null, "alias": null}, "distinct": false, '
                '"alias": "total_' + m2.lower() + '"}], '
                '"group_by": [], "order_by": null, "limit": null, '
                '"additional_filters": {}, "confidence": 0.95, '
                '"reasoning": "Total de ' + a2 + ": SUM " + m2 + '"}'
            )
            example_num += 1

        # Exemplo: Metadata (sempre presente)
        parts.append("")
        parts.append(f"### Exemplo {example_num}: Metadata")
        parts.append('Query: "quantas linhas tem o dataset?"')
        parts.append("Resposta:")
        parts.append(
            '{"intent_type": "metadata", "select_columns": [], '
            '"aggregations": [], "group_by": [], "order_by": null, '
            '"limit": null, "additional_filters": {}, "confidence": 0.95, '
            '"reasoning": "Pergunta sobre estrutura do dataset: metadata"}'
        )
        example_num += 1

        # Exemplos temporais - APENAS se houver colunas temporais
        if has_temporal:
            parts.append("")
            parts.append(f"### Exemplo {example_num}: Temporal - ultimo ano")
            parts.append('Query: "qual o ultimo ano com dados?"')
            parts.append("Resposta:")
            parts.append(
                '{"intent_type": "temporal_analysis", "select_columns": [], '
                '"aggregations": [{"function": "max", "column": '
                '{"name": "Ano", "is_virtual": true, "expression": null, '
                '"alias": "Ano"}, "distinct": false, '
                '"alias": "ultimo_ano"}], '
                '"group_by": [], "order_by": null, "limit": null, '
                '"additional_filters": {}, "confidence": 0.95, '
                '"reasoning": "Usuario quer o ultimo (mais recente) ano, '
                'MAX sobre Ano virtual"}'
            )
            example_num += 1

            default_metric = metric_cols[0] if metric_cols else "valor"
            parts.append("")
            parts.append(f"### Exemplo {example_num}: Agrupamento temporal")
            parts.append('Query: "dados por mes"')
            parts.append("Resposta:")
            parts.append(
                '{"intent_type": "grouped_aggregation", '
                '"select_columns": [], '
                '"aggregations": [{"function": "sum", "column": '
                '{"name": "' + default_metric + '", "is_virtual": false, '
                '"expression": null, "alias": null}, "distinct": false, '
                '"alias": "total"}], '
                '"group_by": [{"name": "Mes", "is_virtual": true, '
                '"expression": null, "alias": "Mes"}], '
                '"order_by": null, "limit": null, '
                '"additional_filters": {}, "confidence": 0.90, '
                '"reasoning": "Dados agrupados por mes: GROUP BY Mes"}'
            )
            example_num += 1

            # Exemplo: "ultimo mes" como filtro temporal (SUM, NAO MAX)
            # This is critical: when "ultimo" qualifies a time period used as
            # a FILTER, the aggregation must remain SUM (not MAX).
            # Use the first metric (unidade) to demonstrate non-monetary metric.
            second_metric = metric_cols[0]
            a_second = _get_first_alias(columns_section, second_metric)
            parts.append("")
            parts.append(
                f"### Exemplo {example_num}: Filtro temporal com 'ultimo' (SUM, NAO MAX)"
            )
            parts.append(
                f'Query: "qual o sell-out em {a_second} no ultimo mes?"'
            )
            parts.append("Resposta:")
            parts.append(
                '{"intent_type": "simple_aggregation", "select_columns": [], '
                '"aggregations": [{"function": "sum", "column": '
                '{"name": "' + second_metric + '", "is_virtual": false, '
                '"expression": null, "alias": null}, "distinct": false, '
                '"alias": "total_' + second_metric.lower() + '"}], '
                '"group_by": [], "order_by": null, "limit": null, '
                '"additional_filters": {}, "confidence": 0.95, '
                '"reasoning": "Sell-out em ' + a_second + ' no ultimo mes: '
                "SUM (filtro temporal ja aplicado pelo sistema, NAO usar MAX)\"}"
            )
            example_num += 1

        parts.append("")
        return "\n".join(parts)

    except Exception as e:
        logger.warning(
            "[_build_few_shot_examples] Erro ao construir exemplos: %s. "
            "Usando exemplos genericos.",
            e,
        )
        return (
            "\n## Exemplos de Analise\n\n"
            "### Exemplo 1: Agregacao simples\n"
            'Query: "qual o total?"\n'
            "Resposta:\n"
            '{"intent_type": "simple_aggregation", "select_columns": [], '
            '"aggregations": [], "group_by": [], "order_by": null, '
            '"limit": null, "additional_filters": {}, '
            '"confidence": 0.80, "reasoning": "Agregacao simples"}\n\n'
            "### Exemplo 2: Metadata\n"
            'Query: "quantas linhas tem o dataset?"\n'
            "Resposta:\n"
            '{"intent_type": "metadata", "select_columns": [], '
            '"aggregations": [], "group_by": [], "order_by": null, '
            '"limit": null, "additional_filters": {}, '
            '"confidence": 0.95, '
            '"reasoning": "Pergunta sobre estrutura do dataset"}\n'
        )


class IntentAnalyzer:
    """
    Motor de compreensão de intenção baseado em LLM.

    Recebe a query do usuário e o contexto semântico (alias.yaml, metadata
    do dataset) e produz uma especificação estruturada (QueryIntent) do que
    o usuário realmente quer saber.

    Substitui a classificação keyword-based por compreensão semântica real,
    permitindo ao sistema capturar dimensões de agrupamento, ordenação,
    funções temporais e rankings que o classificador antigo não conseguia.

    Attributes:
        llm: ChatGoogleGenerativeAI instance (gemini-2.5-flash, temperature=0.1)
        alias_mapper: AliasMapper instance com aliases e column_types
        _schema_context: Contexto de schema pré-computado para o prompt
    """

    def __init__(
        self,
        llm: ChatGoogleGenerativeAI,
        alias_mapper,
    ):
        """
        Inicializa o IntentAnalyzer.

        Args:
            llm: LLM instance configurado para análise de intenção
                (recomendado: gemini-2.5-flash, temperature=0.1)
            alias_mapper: AliasMapper instance com aliases e tipos de colunas
        """
        self.llm = llm
        self.alias_mapper = alias_mapper
        self._schema_context = build_schema_context(alias_mapper)
        self._system_prompt = _build_system_prompt()
        self._few_shot_examples = _build_few_shot_examples()
        logger.info("IntentAnalyzer initialized with LLM-based intent comprehension")

    def analyze(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        token_accumulator=None,
    ) -> QueryIntent:
        """
        Analisa a intenção completa do usuário.

        Usa o LLM para compreender semanticamente a query e produzir
        uma especificação estruturada (QueryIntent) com todas as dimensões
        necessárias: colunas, agregações, agrupamentos, ordenação, etc.

        Args:
            query: Query em linguagem natural do usuário
            filters: Filtros ativos da sessão (do filter_classifier)
            token_accumulator: TokenAccumulator para tracking de uso de tokens

        Returns:
            QueryIntent validado com a especificação completa da intenção

        Raises:
            ValueError: Se o LLM retornar resposta inválida após retentativas
        """
        logger.info(f"[IntentAnalyzer] Analyzing query: '{query}'")

        # Build the analysis prompt
        prompt = self._build_prompt(query, filters)

        try:
            # Call LLM
            messages = [
                SystemMessage(content=self._system_prompt),
                HumanMessage(content=prompt),
            ]

            response = self.llm.invoke(messages)
            content = (
                response.content if hasattr(response, "content") else str(response)
            )

            # Track token usage
            from src.shared_lib.utils.token_tracker import extract_token_usage

            tokens = extract_token_usage(response, self.llm)
            if token_accumulator is not None:
                token_accumulator.add(tokens)
                logger.debug(f"[IntentAnalyzer] Tokens accumulated: {tokens}")

            # Parse and validate the response
            intent = self._parse_response(content, query)

            # Post-process: enrich virtual columns from alias_mapper
            intent = self._enrich_virtual_columns(intent)

            logger.info(
                f"[IntentAnalyzer] Intent analyzed: type={intent.intent_type}, "
                f"confidence={intent.confidence:.2f}, "
                f"aggregations={len(intent.aggregations)}, "
                f"group_by={len(intent.group_by)}"
            )

            return intent

        except Exception as e:
            logger.error(f"[IntentAnalyzer] Error analyzing intent: {e}", exc_info=True)
            # Return a safe fallback intent
            return self._create_fallback_intent(query, str(e))

    def _build_prompt(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Constrói o prompt completo para análise de intenção.

        Inclui o contexto do schema, os exemplos few-shot e a query do usuário.

        Args:
            query: Query do usuário
            filters: Filtros ativos da sessão

        Returns:
            Prompt completo formatado
        """
        filters_text = ""
        if filters:
            filters_text = (
                f"\nFiltros já aplicados na sessão: {json.dumps(filters, default=str)}"
            )

        return f"""{self._few_shot_examples}

## Contexto do Dataset

{self._schema_context}

## Query para Analisar

Query do usuário: "{query}"{filters_text}

Analise a query acima e retorne APENAS o JSON de resposta (sem markdown, sem código, sem explicações adicionais):"""

    def _parse_response(self, content: str, original_query: str) -> QueryIntent:
        """
        Parse e valida a resposta JSON do LLM.

        Tenta múltiplas estratégias de parsing para máxima robustez:
        1. JSON direto
        2. JSON dentro de code blocks markdown
        3. Primeiro objeto JSON encontrado no texto
        4. JSON com correção de aspas escapadas em expressões SQL

        Args:
            content: Resposta bruta do LLM
            original_query: Query original (para fallback)

        Returns:
            QueryIntent validado

        Raises:
            ValueError: Se não conseguir parsear resposta válida
        """
        # Clean up content
        content = content.strip()

        # Log raw response for debugging
        logger.debug(
            f"[IntentAnalyzer] Raw LLM response ({len(content)} chars): {content[:500]}"
        )

        # Strategy 1: Direct JSON parse
        parsed = self._try_parse_json(content)

        # Strategy 2: Extract from markdown code blocks
        if parsed is None:
            match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", content, re.DOTALL)
            if match:
                parsed = self._try_parse_json(match.group(1))

        # Strategy 3: Find first JSON object in text (greedy match for nested objects)
        if parsed is None:
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if match:
                parsed = self._try_parse_json(match.group(0))

        # Strategy 4: Fix common JSON issues and retry
        if parsed is None:
            fixed_content = self._fix_json_issues(content)
            if fixed_content != content:
                parsed = self._try_parse_json(fixed_content)
                if parsed is None:
                    # Try extracting JSON from fixed content
                    match = re.search(r"\{.*\}", fixed_content, re.DOTALL)
                    if match:
                        parsed = self._try_parse_json(match.group(0))

        if parsed is None:
            logger.error(
                f"[IntentAnalyzer] Failed to parse LLM response after all strategies. "
                f"Content ({len(content)} chars): {content[:300]}"
            )
            raise ValueError(
                f"Could not parse LLM response as JSON. "
                f"Response preview: {content[:200]}"
            )

        # Validate and construct QueryIntent
        try:
            intent = self._build_intent_from_dict(parsed)
            return intent
        except Exception as e:
            logger.error(
                f"[IntentAnalyzer] Failed to build QueryIntent from parsed data: {e}. "
                f"Parsed dict: {parsed}"
            )
            raise ValueError(f"Invalid intent structure: {e}")

    def _try_parse_json(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Tenta parsear texto como JSON de forma segura.

        Args:
            text: Texto a parsear

        Returns:
            Dict parseado ou None se falhar
        """
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return None

    def _fix_json_issues(self, content: str) -> str:
        """
        Tenta corrigir problemas comuns de JSON em respostas de LLM.

        Problemas tratados:
        1. Aspas duplas não-escapadas dentro de valores de expressões SQL
           Ex: "expression": "YEAR("Data")" → "expression": null
        2. Caracteres Unicode inválidos
        3. Trailing commas

        Args:
            content: Conteúdo JSON potencialmente malformado

        Returns:
            Conteúdo com correções aplicadas
        """
        fixed = content

        # Fix 1: Replace expression fields containing unescaped SQL with null
        # Pattern: "expression": "YEAR("Data")" or "expression": "MONTH("Data")"
        # These contain unescaped double quotes that break JSON parsing
        fixed = re.sub(
            r'"expression"\s*:\s*"(?:YEAR|MONTH|MONTHNAME|EXTRACT|DATE_PART)\s*\([^"]*"[^"]*"\s*\)[^"]*"',
            '"expression": null',
            fixed,
        )

        # Fix 2: More aggressive — replace any expression with unescaped quotes
        # Match "expression": "...anything with quotes inside..."
        # by looking for the pattern where expression value contains ("
        fixed = re.sub(
            r'"expression"\s*:\s*"[^"]*\("[^"]*"\)[^"]*"',
            '"expression": null',
            fixed,
        )

        # Fix 3: Remove trailing commas before } or ]
        fixed = re.sub(r",\s*([}\]])", r"\1", fixed)

        return fixed

    def _build_intent_from_dict(self, data: Dict[str, Any]) -> QueryIntent:
        """
        Constrói QueryIntent a partir de um dicionário parseado do LLM.

        Realiza normalização e validação dos campos, incluindo:
        - Normalização de intent_type
        - Construção de ColumnSpec para colunas
        - Construção de AggregationSpec para agregações
        - Construção de OrderSpec para ordenação

        Args:
            data: Dicionário parseado da resposta do LLM

        Returns:
            QueryIntent validado
        """
        # Normalize intent_type
        intent_type = data.get("intent_type", "simple_aggregation")
        valid_types = {
            "simple_aggregation",
            "grouped_aggregation",
            "ranking",
            "temporal_analysis",
            "comparison",
            "lookup",
            "metadata",
            "tabular",
            "conversational",
        }
        if intent_type not in valid_types:
            logger.warning(
                f"[IntentAnalyzer] Invalid intent_type '{intent_type}', "
                f"defaulting to 'simple_aggregation'"
            )
            intent_type = "simple_aggregation"

        # Build select_columns
        select_columns = [
            self._build_column_spec(col)
            for col in data.get("select_columns", [])
            if isinstance(col, dict)
        ]

        # Build aggregations
        aggregations = [
            self._build_aggregation_spec(agg)
            for agg in data.get("aggregations", [])
            if isinstance(agg, dict)
        ]

        # Build group_by
        group_by = [
            self._build_column_spec(col)
            for col in data.get("group_by", [])
            if isinstance(col, dict)
        ]

        # Build order_by
        order_by = None
        order_data = data.get("order_by")
        if isinstance(order_data, dict) and order_data.get("column"):
            direction = order_data.get("direction", "DESC")
            if direction not in ("ASC", "DESC"):
                direction = "DESC"
            order_by = OrderSpec(
                column=order_data["column"],
                direction=direction,
            )

        # Limit
        limit = data.get("limit")
        if limit is not None:
            try:
                limit = int(limit)
                if limit <= 0:
                    limit = None
            except (ValueError, TypeError):
                limit = None

        # Additional filters
        additional_filters = data.get("additional_filters", {})
        if not isinstance(additional_filters, dict):
            additional_filters = {}

        # Confidence
        confidence = data.get("confidence", 0.8)
        try:
            confidence = float(confidence)
            confidence = max(0.0, min(1.0, confidence))
        except (ValueError, TypeError):
            confidence = 0.8

        # Reasoning
        reasoning = str(data.get("reasoning", ""))

        return QueryIntent(
            intent_type=intent_type,
            select_columns=select_columns,
            aggregations=aggregations,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            additional_filters=additional_filters,
            confidence=confidence,
            reasoning=reasoning,
        )

    def _build_column_spec(self, data: Dict[str, Any]) -> ColumnSpec:
        """
        Constrói ColumnSpec a partir de um dicionário.

        Args:
            data: Dicionário com dados da coluna

        Returns:
            ColumnSpec validado
        """
        name = str(data.get("name", ""))

        # Resolve virtual column info from alias_mapper
        is_virtual = bool(data.get("is_virtual", False))
        expression = data.get("expression")
        alias = data.get("alias")

        # Auto-detect virtual columns via alias_mapper
        if name and hasattr(self.alias_mapper, "is_virtual_column"):
            if self.alias_mapper.is_virtual_column(name):
                is_virtual = True
                if not expression:
                    expression = self.alias_mapper.get_virtual_expression(name)

        return ColumnSpec(
            name=name,
            is_virtual=is_virtual,
            expression=expression,
            alias=alias,
        )

    def _build_aggregation_spec(self, data: Dict[str, Any]) -> AggregationSpec:
        """
        Constrói AggregationSpec a partir de um dicionário.

        Args:
            data: Dicionário com dados da agregação

        Returns:
            AggregationSpec validado
        """
        # Validate function
        function = data.get("function", "sum")
        valid_functions = {"sum", "avg", "count", "min", "max", "median", "std"}
        if function not in valid_functions:
            logger.warning(
                f"[IntentAnalyzer] Invalid aggregation function '{function}', "
                f"defaulting to 'sum'"
            )
            function = "sum"

        # Build column
        col_data = data.get("column", {})
        if isinstance(col_data, dict):
            column = self._build_column_spec(col_data)
        else:
            column = ColumnSpec(name=str(col_data))

        return AggregationSpec(
            function=function,
            column=column,
            distinct=bool(data.get("distinct", False)),
            alias=data.get("alias"),
        )

    def _enrich_virtual_columns(self, intent: QueryIntent) -> QueryIntent:
        """
        Enriquece o QueryIntent com informações de colunas virtuais do alias_mapper.

        Garante que todas as colunas virtuais referenciadas tenham suas
        expressões SQL corretas, mesmo que o LLM não as tenha fornecido.

        Args:
            intent: QueryIntent a enriquecer

        Returns:
            QueryIntent com colunas virtuais enriquecidas
        """
        if not hasattr(self.alias_mapper, "is_virtual_column"):
            return intent

        def _enrich_col(col: ColumnSpec) -> ColumnSpec:
            if self.alias_mapper.is_virtual_column(col.name):
                expr = self.alias_mapper.get_virtual_expression(col.name)
                return ColumnSpec(
                    name=col.name,
                    is_virtual=True,
                    expression=expr or col.expression,
                    alias=col.alias or col.name,
                )
            return col

        # Enrich all column references
        enriched_select = [_enrich_col(c) for c in intent.select_columns]
        enriched_group = [_enrich_col(c) for c in intent.group_by]
        enriched_aggs = []
        for agg in intent.aggregations:
            enriched_col = _enrich_col(agg.column)
            enriched_aggs.append(
                AggregationSpec(
                    function=agg.function,
                    column=enriched_col,
                    distinct=agg.distinct,
                    alias=agg.alias,
                )
            )

        return QueryIntent(
            intent_type=intent.intent_type,
            select_columns=enriched_select,
            aggregations=enriched_aggs,
            group_by=enriched_group,
            order_by=intent.order_by,
            limit=intent.limit,
            additional_filters=intent.additional_filters,
            confidence=intent.confidence,
            reasoning=intent.reasoning,
        )

    def _create_fallback_intent(self, query: str, error_msg: str) -> QueryIntent:
        """
        Cria um QueryIntent de fallback seguro em caso de erro.

        Tenta inferir o tipo mais provável da query via heurísticas simples
        para que o sistema possa ao menos tentar processar a query.
        Inclui detecção de agrupamentos e rankings para melhor cobertura.

        Args:
            query: Query original do usuário
            error_msg: Mensagem de erro que motivou o fallback

        Returns:
            QueryIntent de fallback com confiança baixa
        """
        query_lower = query.lower()

        # Heurísticas simples de fallback
        intent_type = "simple_aggregation"
        aggregations = []
        group_by = []
        order_by = None
        limit = None
        reasoning = f"Fallback due to error: {error_msg}"

        # Tentar detectar se é metadata
        metadata_terms = [
            "quantas linhas",
            "quantas colunas",
            "quantos registros",
            "quais colunas",
            "tipos de dados",
            "schema",
            "estrutura",
        ]
        if any(term in query_lower for term in metadata_terms):
            intent_type = "metadata"
        # Tentar detectar se é tabular
        elif any(
            term in query_lower
            for term in ["mostre tabela", "dados brutos", "ver tabela"]
        ):
            intent_type = "tabular"
        # Detectar queries com agrupamento dimensional
        elif self._detect_grouped_intent(query_lower):
            intent_type = "grouped_aggregation"
            try:
                numeric_cols = self.alias_mapper.column_types.get("numeric", [])
                agg_col = numeric_cols[0] if numeric_cols else None

                if agg_col:
                    aggregations = [
                        AggregationSpec(
                            function="sum",
                            column=ColumnSpec(name=agg_col),
                            alias="total",
                        )
                    ]

                # Detectar dimensão de agrupamento
                group_col = self._detect_group_dimension(query_lower)
                if group_col:
                    is_virtual = (
                        self.alias_mapper.is_virtual_column(group_col)
                        if hasattr(self.alias_mapper, "is_virtual_column")
                        else False
                    )
                    group_by = [
                        ColumnSpec(
                            name=group_col,
                            is_virtual=is_virtual,
                            alias=group_col,
                        )
                    ]

                # Detectar se é ranking (maior/menor/top)
                # Note: "ultimo"/"ultima" are NOT ranking keywords — they
                # indicate temporal filter context ("vendas no ultimo mes"),
                # not "which group had the highest/lowest value".
                if any(
                    kw in query_lower
                    for kw in [
                        "maior",
                        "menor",
                        "top",
                        "primeiro",
                    ]
                ):
                    order_by = OrderSpec(column="total", direction="DESC")
                    limit = 1
                    if any(
                        kw in query_lower for kw in ["menor", "primeiro", "primeira"]
                    ):
                        order_by = OrderSpec(column="total", direction="ASC")

            except (AttributeError, KeyError, IndexError):
                pass
        # Default: simple aggregation com coluna numérica
        else:
            try:
                numeric_cols = self.alias_mapper.column_types.get("numeric", [])
                if numeric_cols:
                    # Detectar função de agregação
                    func = "sum"
                    if any(
                        kw in query_lower
                        for kw in [
                            "máximo",
                            "maximo",
                            "maior",
                        ]
                    ):
                        # "ultimo"/"ultima" as filter context (e.g. "vendas no
                        # ultimo mes") should NOT trigger MAX — the temporal
                        # resolver already applies the date filter, so the
                        # aggregation should remain SUM.  MAX is only correct
                        # for explicit "maior"/"maximo" requests on a value.
                        func = "max"
                    elif any(
                        kw in query_lower
                        for kw in [
                            "primeiro",
                            "primeira",
                            "mais antigo",
                            "mínimo",
                            "minimo",
                            "menor",
                        ]
                    ):
                        func = "min"
                    elif any(kw in query_lower for kw in ["media", "média", "average"]):
                        func = "avg"
                    elif any(
                        kw in query_lower for kw in ["quantos", "quantas", "contagem"]
                    ):
                        func = "count"

                    aggregations = [
                        AggregationSpec(
                            function=func,
                            column=ColumnSpec(name=numeric_cols[0]),
                            alias="resultado",
                        )
                    ]
            except (AttributeError, KeyError):
                pass

        logger.warning(
            f"[IntentAnalyzer] Using fallback intent: type={intent_type}, "
            f"aggs={len(aggregations)}, group_by={len(group_by)}, "
            f"reason={error_msg}"
        )

        return QueryIntent(
            intent_type=intent_type,
            aggregations=aggregations,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            confidence=0.3,
            reasoning=reasoning,
        )

    def _detect_grouped_intent(self, query_lower: str) -> bool:
        """
        Detecta se a query provavelmente requer agrupamento (GROUP BY).

        Usa padroes linguisticos genericos ao inves de termos hardcoded
        para funcionar com qualquer dataset.

        Args:
            query_lower: Query em lowercase

        Returns:
            True se a query provavelmente requer agrupamento
        """
        group_patterns = [
            r"por\s+\w+",
            r"qua[il]s?\s+\w+\s+(teve|tem|foi|com|e\b)",
            r"\w+\s+com\s+(maior|menor|mais|menos)",
            r"agrupar\s+por",
            r"cada\s+\w+",
            r"distribui[\xe7c][\xe3a]o\s+(de|por|entre)",
        ]
        return any(re.search(p, query_lower) for p in group_patterns)

    def _detect_group_dimension(self, query_lower: str) -> Optional[str]:
        """
        Detecta a dimensao de agrupamento mais provavel da query.

        Usa resolucao dinamica via aliases do alias.yaml ao inves de
        mapeamento hardcoded, funcionando com qualquer dataset.

        Args:
            query_lower: Query em lowercase

        Returns:
            Nome da coluna de agrupamento ou None
        """
        # 1. Verificar nomes de colunas diretamente (case-insensitive)
        all_cols = []
        try:
            all_cols = list(
                self.alias_mapper.column_types.get("categorical", [])
            ) + list(self.alias_mapper.column_types.get("temporal", []))
        except (AttributeError, KeyError):
            pass

        for col in all_cols:
            if col.lower() in query_lower:
                return col

        # 2. Verificar aliases (termos do usuario -> coluna real)
        try:
            aliases = getattr(self.alias_mapper, "aliases", {})
            columns_section = aliases.get("columns", {})
            for col_name, alias_list in columns_section.items():
                if not isinstance(alias_list, list):
                    continue
                for alias_term in alias_list:
                    if (
                        isinstance(alias_term, str)
                        and alias_term.lower() in query_lower
                    ):
                        return col_name
        except (AttributeError, KeyError):
            pass

        # 3. Verificar colunas virtuais (se existirem)
        try:
            virtual_map = getattr(self.alias_mapper, "VIRTUAL_COLUMN_MAP", {})
            for col_name in virtual_map:
                if col_name.lower() in query_lower:
                    return col_name
        except (AttributeError, KeyError):
            pass

        return None
