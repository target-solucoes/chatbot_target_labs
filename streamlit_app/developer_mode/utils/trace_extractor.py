"""
Trace extractor for Developer Mode.

Extracts per-agent input/output traces and tool-level detail
from the pipeline final state, enabling full data flow traceability.
"""

from typing import Dict, Any, List


# ============================================================================
# AGENT I/O KEY DEFINITIONS
# ============================================================================

AGENT_DEFINITIONS = {
    "filter_classifier": {
        "input_keys": ["query", "current_filters", "filter_history"],
        "output_keys": [
            "filter_final",
            "filter_operations",
            "detected_filter_columns",
            "filter_confidence",
        ],
    },
    "graphic_classifier": {
        "input_keys": ["query", "filter_final", "data_source"],
        "output_keys": [
            "output",
            "intent",
            "confidence",
            "semantic_anchor",
            "semantic_validation",
            "semantic_mapping",
            "parsed_entities",
            "detected_keywords",
            "mapped_columns",
        ],
    },
    "analytics_executor": {
        "input_keys": ["output", "data_source", "filter_final"],
        "output_keys": ["executor_output", "execution_time"],
    },
    "non_graph_executor": {
        "input_keys": ["query", "filter_final", "data_source"],
        "output_keys": ["non_graph_output", "execution_time"],
    },
    "insight_generator": {
        "input_keys": ["output", "executor_output", "query"],
        "output_keys": ["insight_result", "insight_execution_time"],
    },
    "plotly_generator": {
        "input_keys": ["output", "executor_output"],
        "output_keys": ["plotly_output", "plotly_execution_time"],
    },
    "formatter": {
        "input_keys": [
            "query",
            "output",
            "executor_output",
            "insight_result",
            "plotly_output",
            "filter_final",
            "agent_tokens",
        ],
        "output_keys": ["formatter_output"],
    },
}


# ============================================================================
# AGENT-LEVEL I/O EXTRACTION
# ============================================================================


def extract_agent_input(state: Dict[str, Any], agent_name: str) -> Dict[str, Any]:
    """Extract raw input for a specific agent from pipeline state."""
    defn = AGENT_DEFINITIONS.get(agent_name, {})
    return {k: state.get(k) for k in defn.get("input_keys", [])}


def extract_agent_output(state: Dict[str, Any], agent_name: str) -> Dict[str, Any]:
    """Extract raw output for a specific agent from pipeline state."""
    defn = AGENT_DEFINITIONS.get(agent_name, {})
    return {k: state.get(k) for k in defn.get("output_keys", [])}


# ============================================================================
# TOOL-LEVEL EXTRACTION PER AGENT
# ============================================================================


def extract_filter_tools(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract tool-level traces for the Filter Classifier."""
    query = state.get("query", "")
    current_filters = state.get("current_filters", {})
    detected_cols = state.get("detected_filter_columns", [])
    filter_ops = state.get("filter_operations", {})
    filter_final = state.get("filter_final", {})

    tools = []

    # ContextLoader
    tools.append({
        "name": "ContextLoader & Heuristic (Local)",
        "description": "Analisa a query heuristicamente para decidir se necessita de filtro, carrega historico e inicializa contadores.",
        "input": {"query": query},
        "output": {"_filter_needs_detection": state.get("_filter_needs_detection", True)}
    })

    # RelativeTemporalResolver
    if "temporal_resolution" in state:
        tools.append({
            "name": "RelativeTemporalResolver (Regex)",
            "description": "Resolve referencias temporais relativas (ex: 'ultimo mes', 'YTD') para filtros de datas baseados na data atual.",
            "input": {"query": query},
            "output": state.get("temporal_resolution", {})
        })

    # PreMatchEngine
    if "pre_match_candidates" in state:
        tools.append({
            "name": "PreMatchEngine & ValueCatalog (Local)",
            "description": "Executa matching fuzzy e deterministico direto contra os valores reais do dataset indexados no ValueCatalog ANTES do LLM. Tambem avalia value_aliases.",
            "input": {"query": query},
            "output": {"pre_match_candidates": state.get("pre_match_candidates", [])}
        })

    # FilterParser
    tools.append({
        "name": "FilterParser (LLM)",
        "description": "Usa LLM e os candidatos pre-resolvidos para interpretar a query textual e extrair colunas, valores sugeridos e operacoes CRUD.",
        "input": {
            "query": query,
            "filter_history_count": len(state.get("filter_history", [])),
        },
        "output": {
            "detected_filter_columns": detected_cols,
            "filter_confidence": state.get("filter_confidence", 0.0),
        },
    })

    # TemporalPeriodExpander
    if "temporal_expansion_validation" in state:
        tools.append({
            "name": "TemporalPeriodExpander (Regex)",
            "description": "Expande periodos temporais comparativos na query (ex: 'maio a junho') para garantir cobertura completa do range de datas.",
            "input": {"query": query},
            "output": state.get("temporal_expansion_validation", {})
        })

    # FilterValidator
    tools.append({
        "name": "FilterValidator (Local)",
        "description": "Valida se os valores escolhidos pelo LLM existem de fato no ValueCatalog. Sugere correcoes fuzzy deterministicas.",
        "input": {"detected_columns": detected_cols},
        "output": {
            "validation_passed": len(state.get("errors", [])) == 0,
            "warnings": state.get("validation_warnings", [])
        },
    })

    # Legacy operations
    tools.extend([
        {
            "name": "OperationsIdentifier",
            "description": "Compara os filtros atuais com os detectados para identificar quais operacoes CRUD deven ser aplicadas (ADICIONAR, ALTERAR, REMOVER, MANTER).",
            "input": {
                "current_filters": current_filters,
                "detected_columns": detected_cols,
            },
            "output": {"filter_operations": filter_ops},
        },
        {
            "name": "FilterApplicator",
            "description": "Aplica as operacoes CRUD identificadas sobre os filtros atuais, produzindo o estado consolidado (filter_final).",
            "input": {
                "current_filters": current_filters,
                "filter_operations": filter_ops,
            },
            "output": {"filter_final": filter_final},
        },
        {
            "name": "FilterPersistence",
            "description": "Persiste o estado final dos filtros em sessao para reutilizacao.",
            "input": {"filter_final": filter_final},
            "output": {"persisted": True},
        },
    ])

    return tools


def extract_classifier_tools(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract tool-level traces for the Graphic Classifier."""
    query = state.get("query", "")
    anchor = state.get("semantic_anchor")
    validation = state.get("semantic_validation")
    mapping = state.get("semantic_mapping")
    entities = state.get("parsed_entities")
    keywords = state.get("detected_keywords")

    return [
        {
            "name": "SemanticAnchorExtractor (LLM)",
            "description": "Usa LLM para extrair a intencao semantica central da query ('ancora'), normalizando-a para facilitar o mapeamento para tipos de grafico.",
            "input": {"query": query},
            "output": {"semantic_anchor": anchor},
        },
        {
            "name": "SemanticValidator",
            "description": "Verifica se a ancora semantica extraida e coerente e suficientemente especifica para guiar a classificacao, ou se precisa de fallback para a pipeline legada.",
            "input": {"semantic_anchor": anchor},
            "output": {"semantic_validation": validation},
        },
        {
            "name": "SemanticMapper",
            "description": "Mapeia a ancora semantica validada para sugestoes de tipo de grafico e configuracoes visuais, usando regras e padroes pre-definidos.",
            "input": {"semantic_validation": validation},
            "output": {"semantic_mapping": mapping},
        },
        {
            "name": "QueryParser",
            "description": "Extrai entidades estruturadas da query (numeros, datas, termos citados entre aspas, referencias a colunas) para enriquecer a classificacao.",
            "input": {"query": query},
            "output": {"parsed_entities": entities},
        },
        {
            "name": "DatasetMetadataLoader",
            "description": "Carrega os metadados do dataset (colunas disponiveis, tipos de dados) para validar se as colunas mencionadas na query existem.",
            "input": {"data_source": state.get("data_source")},
            "output": {"available_columns": state.get("available_columns")},
        },
        {
            "name": "KeywordDetector",
            "description": "Detecta palavras-chave na query que indicam tipo de grafico ou intencao analitica (ex: 'top', 'evolucao', 'distribuicao', 'comparar').",
            "input": {"query": query},
            "output": {"detected_keywords": keywords},
        },
        {
            "name": "IntentClassifier (DecisionTree)",
            "description": "Aplica uma arvore de decisao deterministica usando as keywords detectadas, o mapeamento semantico e entidades para classificar o intent e o tipo de grafico final.",
            "input": {
                "query": query,
                "detected_keywords": keywords,
                "semantic_mapping": mapping,
                "parsed_entities": entities,
            },
            "output": {
                "intent": state.get("intent"),
                "confidence": state.get("confidence"),
            },
        },
        {
            "name": "ColumnMapper",
            "description": "Mapeia os nomes de colunas mencionados pelo usuario (aliases, variantes, erros ortograficos) para os nomes exatos das colunas no dataset usando alias.yaml.",
            "input": {
                "parsed_entities": entities,
                "available_columns": state.get("available_columns"),
            },
            "output": {
                "mapped_columns": state.get("mapped_columns"),
                "columns_mentioned": state.get("columns_mentioned"),
            },
        },
        {
            "name": "OutputGenerator",
            "description": "Consolida todos os resultados anteriores e gera o ChartOutput estruturado (JSON) com chart_type, metrics, dimensions, filters, top_n, sort e configuracoes visuais.",
            "input": {
                "intent": state.get("intent"),
                "confidence": state.get("confidence"),
                "mapped_columns": state.get("mapped_columns"),
                "semantic_mapping": mapping,
            },
            "output": {"output (ChartOutput)": state.get("output")},
        },
    ]


def extract_executor_tools(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract tool-level traces for the Analytics Executor."""
    executor_output = state.get("executor_output") or {}
    chart_spec = state.get("output") or {}
    chart_type = chart_spec.get("chart_type", "N/A")

    return [
        {
            "name": "InputParser",
            "description": "Le o ChartOutput do Graphic Classifier e resolve o caminho do dataset para preparar o contexto de execucao.",
            "input": {
                "chart_type": chart_type,
                "metrics": chart_spec.get("metrics"),
                "dimensions": chart_spec.get("dimensions"),
                "data_source": state.get("data_source"),
            },
            "output": {
                "parsed_chart_type": chart_type,
                "data_source_resolved": state.get("data_source"),
            },
        },
        {
            "name": "Router",
            "description": "Roteia para o ToolHandler especializado correspondente ao chart_type detectado (ex: bar_horizontal -> BarHorizontalHandler).",
            "input": {"chart_type": chart_type},
            "output": {
                "routed_to": f"tool_handle_{chart_type}",
            },
        },
        {
            "name": f"ToolHandler ({chart_type})",
            "description": f"Handler especializado para o tipo '{chart_type}'. Constroi a query SQL otimizada para esse chart_type, executa via DuckDB e retorna os dados tabulares prontos para plotagem.",
            "input": {
                "chart_type": chart_type,
                "metrics": chart_spec.get("metrics"),
                "dimensions": chart_spec.get("dimensions"),
                "filters": state.get("filter_final", {}),
                "top_n": chart_spec.get("top_n"),
                "sort": chart_spec.get("sort"),
            },
            "output": {
                "sql_query": executor_output.get("sql_query"),
                "row_count": executor_output.get("row_count"),
                "engine_used": executor_output.get("engine_used"),
            },
        },
        {
            "name": "OutputFormatter",
            "description": "Formata o resultado da execucao SQL em um dict padrao com status, dados, plotly_config e metadata, pronto para ser consumido pelo Plotly Generator e Insight Generator.",
            "input": {
                "raw_data_rows": executor_output.get("row_count", 0),
                "chart_type": chart_type,
            },
            "output": {
                "status": executor_output.get("status"),
                "has_plotly_config": executor_output.get("plotly_config") is not None,
                "metadata": executor_output.get("metadata"),
            },
        },
    ]


def extract_non_graph_tools(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract tool-level traces for the Non-Graph Executor."""
    non_graph = state.get("non_graph_output") or {}
    metadata = non_graph.get("metadata") or {}
    query = state.get("query", "")

    tools = [
        {
            "name": "QueryClassifier (LLM)",
            "description": "Classifica o tipo da query nao-grafica usando LLM (metadata, aggregation, lookup, textual, statistical, tabular) e extrai os parametros necessarios para execucao.",
            "input": {"query": query},
            "output": {
                "query_type": non_graph.get("query_type"),
                "execution_path": metadata.get("execution_path"),
            },
        },
    ]

    if metadata.get("execution_path") == "dynamic":
        tools.append(
            {
                "name": "IntentAnalyzer (LLM)",
                "description": "Analisa semanticamente a query para extrair um QueryIntent rico com agregacoes (SUM, AVG, COUNT), GROUP BY, ORDER BY e LIMIT para construcao de SQL dinamico.",
                "input": {"query": query},
                "output": {
                    "intent_type": metadata.get("intent_type"),
                    "aggregations": metadata.get("aggregations"),
                    "group_by": metadata.get("group_by"),
                    "order_by": metadata.get("order_by"),
                    "limit": metadata.get("limit"),
                },
            }
        )
        tools.append(
            {
                "name": "DynamicQueryBuilder",
                "description": "Constroi a query SQL dinamicamente a partir do QueryIntent extraido pelo IntentAnalyzer, aplicando filtros ativos da sessao e validando as colunas via AliasMapper.",
                "input": {
                    "intent_type": metadata.get("intent_type"),
                    "aggregations": metadata.get("aggregations"),
                    "group_by": metadata.get("group_by"),
                    "filters": metadata.get("filters_applied", {}),
                },
                "output": {"sql_query": metadata.get("sql_query")},
            }
        )

    sql = metadata.get("sql_query")
    if sql:
        tools.append(
            {
                "name": "QueryExecutor (DuckDB)",
                "description": "Executa a query SQL gerada diretamente sobre o arquivo Parquet via DuckDB, retornando os resultados como lista de dicionarios.",
                "input": {"sql_query": sql},
                "output": {
                    "row_count": metadata.get("row_count"),
                    "engine": metadata.get("engine", "DuckDB"),
                },
            }
        )

    conv_response = non_graph.get("conversational_response")
    if conv_response:
        tools.append(
            {
                "name": "ConversationalHandler (LLM)",
                "description": "Detecta queries puramente conversacionais (ex: 'ola', 'obrigado') que nao requerem execucao de dados e gera uma resposta textual diretamente via LLM.",
                "input": {"query": query, "data_context": "query_result"},
                "output": {
                    "conversational_response": (
                        conv_response[:300] + "..."
                        if len(conv_response) > 300
                        else conv_response
                    )
                },
            }
        )

    tools.append(
        {
            "name": "OutputFormatter",
            "description": "Formata o resultado final em JSON estruturado com status, query_type, summary, conversational_response, data e performance_metrics.",
            "input": {"query_type": non_graph.get("query_type"), "raw_data": "query_result"},
            "output": {
                "status": non_graph.get("status"),
                "summary": non_graph.get("summary"),
            },
        }
    )

    return tools


def extract_insight_tools(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract tool-level traces for the Insight Generator."""
    insight = state.get("insight_result") or {}
    chart_spec = state.get("output") or {}
    executor = state.get("executor_output") or {}
    insights_list = insight.get("detailed_insights", insight.get("insights", []))

    return [
        {
            "name": "MetricsCalculator",
            "description": "Calcula metricas estatisticas especificas para o tipo de grafico (top/bottom, variacao percentual, medias, outliers) a partir dos dados do executor, para embasar os insights.",
            "input": {
                "chart_type": chart_spec.get("chart_type"),
                "data_rows": len(executor.get("data", [])),
            },
            "output": {
                "metrics_calculated": len(insights_list),
            },
        },
        {
            "name": "InsightSynthesizer (LLM)",
            "description": "Usa LLM para gerar insights estrategicos e narrativa interpretativa a partir das metricas calculadas, enriquecidos com o contexto da query original do usuario.",
            "input": {
                "chart_type": chart_spec.get("chart_type"),
                "data_summary": f"{len(executor.get('data', []))} rows",
                "user_query": state.get("query", ""),
            },
            "output": {
                "status": insight.get("status"),
                "insights_count": len(insights_list),
                "has_formatted_insights": bool(insight.get("formatted_insights")),
            },
        },
    ]


def extract_plotly_tools(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract tool-level traces for the Plotly Generator."""
    plotly = state.get("plotly_output") or {}
    chart_spec = state.get("output") or {}
    executor = state.get("executor_output") or {}
    chart_type = chart_spec.get("chart_type", "N/A")

    return [
        {
            "name": f"PlotlyChartHandler ({chart_type})",
            "description": f"Handler especializado para renderizacao do grafico '{chart_type}'. Consome o plotly_config do executor e os dados para gerar um objeto Plotly Figure interativo, salvo como HTML.",
            "input": {
                "chart_type": chart_type,
                "data_rows": len(executor.get("data", [])),
                "plotly_config_from_executor": executor.get("plotly_config") is not None,
            },
            "output": {
                "status": plotly.get("status"),
                "has_config": plotly.get("config") is not None,
                "file_path": plotly.get("file_path"),
            },
        },
    ]


def extract_formatter_tools(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract tool-level traces for the Formatter Agent."""
    formatter = state.get("formatter_output") or {}

    return [
        {
            "name": "ExecutiveSummaryGenerator (LLM)",
            "description": "Gera via LLM um sumario executivo estruturado (titulo, introducao, destaques) consolidando o resultado analitico em linguagem de negocio.",
            "input": {
                "chart_type": state.get("output", {}).get("chart_type"),
                "query": state.get("query"),
                "data_rows": len(
                    (state.get("executor_output") or {}).get("data", [])
                ),
            },
            "output": {
                "executive_summary": formatter.get("executive_summary"),
            },
        },
        {
            "name": "InsightNarrativeSynthesizer (LLM)",
            "description": "Sintetiza os insights brutos do Insight Generator em uma narrativa coesa e priorizada, removendo redundancias e ordenando por relevancia estrategica.",
            "input": {
                "insight_result_available": state.get("insight_result") is not None,
            },
            "output": {
                "insights": formatter.get("insights"),
            },
        },
        {
            "name": "NextStepsGenerator (LLM)",
            "description": "Gera via LLM recomendacoes de proximos passos acionaveis com base nos insights e no tipo de analise realizada, orientando decisoes de negocio.",
            "input": {
                "chart_type": state.get("output", {}).get("chart_type"),
                "insights_available": formatter.get("insights") is not None,
            },
            "output": {
                "next_steps": formatter.get("next_steps"),
            },
        },
        {
            "name": "DataTableFormatter",
            "description": "Formata os dados tabulares do executor em estrutura padronizada para exibicao no frontend (colunas, linhas, totais, formatacao de valores).",
            "input": {
                "executor_data_available": state.get("executor_output") is not None,
            },
            "output": {
                "data": formatter.get("data") is not None,
            },
        },
    ]


# ============================================================================
# MAIN EXTRACTION FUNCTION
# ============================================================================


def extract_all_traces(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Extract all agent traces from pipeline state.

    Returns a dict keyed by agent name, each containing:
    - agent_input: raw input dict (keys the agent reads)
    - agent_output: raw output dict (keys the agent writes)
    - tools: list of tool-level traces with name, description, input, output per tool
    """
    return {
        "filter_classifier": {
            "agent_input": extract_agent_input(state, "filter_classifier"),
            "agent_output": extract_agent_output(state, "filter_classifier"),
            "tools": extract_filter_tools(state),
        },
        "graphic_classifier": {
            "agent_input": extract_agent_input(state, "graphic_classifier"),
            "agent_output": extract_agent_output(state, "graphic_classifier"),
            "tools": extract_classifier_tools(state),
        },
        "analytics_executor": {
            "agent_input": extract_agent_input(state, "analytics_executor"),
            "agent_output": extract_agent_output(state, "analytics_executor"),
            "tools": extract_executor_tools(state),
        },
        "non_graph_executor": {
            "agent_input": extract_agent_input(state, "non_graph_executor"),
            "agent_output": extract_agent_output(state, "non_graph_executor"),
            "tools": extract_non_graph_tools(state),
        },
        "insight_generator": {
            "agent_input": extract_agent_input(state, "insight_generator"),
            "agent_output": extract_agent_output(state, "insight_generator"),
            "tools": extract_insight_tools(state),
        },
        "plotly_generator": {
            "agent_input": extract_agent_input(state, "plotly_generator"),
            "agent_output": extract_agent_output(state, "plotly_generator"),
            "tools": extract_plotly_tools(state),
        },
        "formatter": {
            "agent_input": extract_agent_input(state, "formatter"),
            "agent_output": extract_agent_output(state, "formatter"),
            "tools": extract_formatter_tools(state),
        },
    }
