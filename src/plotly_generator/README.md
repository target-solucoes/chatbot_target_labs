# Plotly Generator Agent

**Versão:** 1.0  
**Data:** 2025-11-12  
**Status:** ✅ Produção

---

## 📋 Índice

1. [Visão Geral](#-visão-geral)
2. [Arquitetura](#-arquitetura)
3. [Instalação](#-instalação)
4. [Uso Básico](#-uso-básico)
5. [API Pública](#-api-pública)
6. [Tipos de Gráficos Suportados](#-tipos-de-gráficos-suportados)
7. [Integração com Pipeline](#-integração-com-pipeline)
8. [Performance](#-performance)
9. [Exemplos](#-exemplos)
10. [Troubleshooting](#-troubleshooting)

---

## 🎯 Visão Geral

O **Plotly Generator Agent** é o quarto e último agente do pipeline multiagente, responsável por transformar saídas estruturadas dos agentes `graphical_classifier` e `analytics_executor` em **gráficos interativos Plotly**.

### Posição no Pipeline

```
User Query
    ↓
[Agent 1: filter_classifier]
    → Extrai e normaliza filtros
    ↓
[Agent 2: graphical_classifier]
    → Classifica chart_type e extrai especificações
    ↓
[Agent 3: analytics_executor]
    → Executa queries SQL e retorna dados processados
    ↓
[Agent 4: plotly_generator] ← ESTE AGENTE
    → Gera gráficos Plotly interativos
    ↓
Plotly HTML/PNG Output
```

### Características Principais

✅ **8 tipos de gráficos suportados**  
✅ **100% de taxa de sucesso** (conforme benchmark)  
✅ **Performance média: 0.022s** (22ms por gráfico)  
✅ **Zero hardcoding** - totalmente dinâmico baseado em specs  
✅ **LangGraph workflow** para orquestração  
✅ **Salvamento automático** em HTML/PNG  
✅ **Rastreamento de estatísticas** integrado  

---

## 🏗️ Arquitetura

```
src/plotly_generator/
├── plotly_generator_agent.py    # Agente principal
├── adapters/
│   └── input_adapter.py          # Parsers de input
├── generators/
│   ├── base.py                   # Classe base abstrata
│   ├── router.py                 # Seleção de generator
│   ├── bar_horizontal_generator.py
│   ├── bar_vertical_generator.py
│   ├── bar_vertical_composed_generator.py
│   ├── bar_vertical_stacked_generator.py
│   ├── line_generator.py
│   ├── line_composed_generator.py
│   ├── pie_generator.py
│   └── histogram_generator.py
├── utils/
│   ├── plot_styler.py            # Estilos e paletas
│   ├── file_saver.py             # Salvamento de arquivos
│   ├── axis_configurator.py      # Configuração de eixos
│   └── color_manager.py          # Gerenciamento de cores
└── graph/
    ├── state.py                  # Estado do LangGraph
    ├── nodes.py                  # Nodes do workflow
    └── workflow.py               # Definição do workflow
```

### Fluxo de Execução

```
Input (ChartOutput + AnalyticsOutput)
    ↓
[validate_inputs]
    ↓
[adapt_inputs]
    ↓
[generate_plot]
    ↓
[save_output]
    ↓
Output (Plotly Figure + HTML + Metadata)
```

---

## 📦 Instalação

### Dependências

```toml
[tool.poetry.dependencies]
plotly = "^5.18.0"           # Biblioteca Plotly
kaleido = "^0.2.1"           # Para exportar PNG (opcional)
pandas = "^2.1.0"            # Manipulação de dados
```

### Instalação via Poetry

```bash
poetry add plotly kaleido
poetry install
```

### Instalação via pip

```bash
pip install plotly kaleido
```

---

## 🚀 Uso Básico

### Exemplo 1: Uso Standalone

```python
from src.plotly_generator.plotly_generator_agent import PlotlyGeneratorAgent

# Inicializar agente
agent = PlotlyGeneratorAgent(
    save_html=True,
    save_png=False
)

# Inputs dos agentes anteriores
chart_spec = {
    "chart_type": "bar_horizontal",
    "title": "Top 5 Produtos",
    "metrics": [{"name": "Qtd_Vendida", "alias": "Quantidade"}],
    "dimensions": [{"name": "Produto", "alias": "Produto"}],
    "visual": {"palette": "Blues", "show_values": True}
}

analytics_result = {
    "status": "success",
    "data": [
        {"Produto": "Bike A", "Quantidade": 1200},
        {"Produto": "Bike B", "Quantidade": 950},
        {"Produto": "Helmet", "Quantidade": 800}
    ]
}

# Gerar gráfico
result = agent.generate(chart_spec, analytics_result)

if result['status'] == 'success':
    result['figure'].show()  # Exibe no browser
    print(f"Salvo em: {result['file_path']}")
```

### Exemplo 2: Integração com Pipeline Completo

```python
from src.pipeline_orchestrator import run_integrated_pipeline

result = run_integrated_pipeline(
    "top 5 produtos mais vendidos",
    include_plotly_generator=True,
    save_plotly_html=True
)

if result.plotly_output and result.plotly_output['status'] == 'success':
    result.plotly_figure.show()
    print(f"Gráfico salvo: {result.plotly_file_path}")
```

---

## 📚 API Pública

### Classe Principal: `PlotlyGeneratorAgent`

#### Inicialização

```python
agent = PlotlyGeneratorAgent(
    output_dir: Optional[Path] = None,     # Diretório de saída
    save_html: bool = True,                # Salvar como HTML
    save_png: bool = False                 # Salvar como PNG
)
```

#### Método Principal: `generate()`

```python
result = agent.generate(
    chart_spec: Dict[str, Any],            # ChartOutput do graphical_classifier
    analytics_result: Dict[str, Any]       # AnalyticsOutput do analytics_executor
) -> Dict[str, Any]
```

**Retorna:**

```python
{
    "status": "success" | "error",
    "chart_type": str,
    "figure": plotly.graph_objects.Figure,  # Objeto Plotly
    "html": str,                            # HTML renderizado
    "file_path": str,                       # Caminho do arquivo salvo
    "config": Dict[str, Any],               # Configuração utilizada
    "metadata": {
        "rows_plotted": int,
        "render_time": float,
        "generator_used": str
    },
    "error": Optional[Dict]                 # Se status == "error"
}
```

#### Método: `validate_inputs()`

```python
is_valid, error_message = agent.validate_inputs(
    chart_spec: Dict[str, Any],
    analytics_result: Dict[str, Any]
) -> Tuple[bool, Optional[str]]
```

#### Método: `get_statistics()`

```python
stats = agent.get_statistics() -> Dict[str, Any]
```

**Retorna:**

```python
{
    "total_generations": int,
    "successful_generations": int,
    "failed_generations": int,
    "success_rate": float,               # Percentual
    "total_render_time": float,          # Segundos
    "average_render_time": float,        # Segundos
    "charts_by_type": Dict[str, int]     # Contador por tipo
}
```

---

## 📊 Tipos de Gráficos Suportados

| Tipo | Descrição | Eixo X | Eixo Y | Uso Típico |
|------|-----------|--------|--------|------------|
| **bar_horizontal** | Barras horizontais | Métrica | Categoria | Rankings, top-N |
| **bar_vertical** | Barras verticais | Categoria | Métrica | Comparações diretas |
| **bar_vertical_composed** | Barras agrupadas | Categoria | Métrica | Comparações por subcategoria |
| **bar_vertical_stacked** | Barras empilhadas | Categoria | Métrica | Composição empilhada |
| **line** | Linha simples | Tempo | Métrica | Tendências temporais |
| **line_composed** | Múltiplas linhas | Tempo | Métrica | Múltiplas séries temporais |
| **pie** | Pizza | - | - | Proporção relativa |
| **histogram** | Histograma | Faixas (bins) | Frequência | Distribuição de valores |

---

## 🔗 Integração com Pipeline

### Modificações no `pipeline_orchestrator.py`

#### Novo Parâmetro: `include_plotly_generator`

```python
result = run_integrated_pipeline(
    query: str,
    include_filter_classifier: bool = True,
    include_executor: bool = True,
    include_plotly_generator: bool = False,  # NOVO
    save_plotly_html: bool = True,           # NOVO
    save_plotly_png: bool = False            # NOVO
)
```

#### Novas Propriedades em `IntegratedPipelineResult`

```python
result.plotly_output       # Dict com resultado do plotly_generator
result.plotly_figure       # Objeto plotly.graph_objects.Figure
result.plotly_html         # HTML renderizado
result.plotly_file_path    # Caminho do arquivo salvo
```

---

## ⚡ Performance

### Benchmark Results (10 iterações por tipo)

| Chart Type | Tempo Médio | Throughput | Taxa de Sucesso |
|------------|-------------|------------|-----------------|
| **bar_horizontal** | 0.057s | 17.7 charts/s | 100% |
| **bar_vertical** | 0.006s | 167.5 charts/s | 100% |
| **bar_vertical_composed** | 0.022s | 44.8 charts/s | 100% |
| **bar_vertical_stacked** | 0.018s | 55.3 charts/s | 100% |
| **line** | 0.025s | 39.7 charts/s | 100% |
| **line_composed** | 0.020s | 50.1 charts/s | 100% |
| **pie** | 0.006s | 156.1 charts/s | 100% |
| **histogram** | 0.021s | 48.4 charts/s | 100% |
| **MÉDIA GERAL** | **0.022s** | **45.5 charts/s** | **100%** |

### Scaling (bar_horizontal)

| Rows | Tempo Médio | Throughput |
|------|-------------|------------|
| 10 | 0.032s | 31.1 charts/s |
| 50 | 0.020s | 48.9 charts/s |
| 100 | 0.017s | 59.1 charts/s |
| 500 | 0.031s | 31.8 charts/s |
| 1000 | 0.055s | 18.1 charts/s |

### Tamanhos de Arquivo

- **HTML**: ~9.3 KB (com CDN)
- **HTML (inline)**: ~500 KB (Plotly.js embarcado)
- **PNG**: Variável (depende de resolução)

---

## 💡 Exemplos

### Exemplo: Gerar Todos os Tipos de Gráficos

```python
from src.plotly_generator.plotly_generator_agent import PlotlyGeneratorAgent

agent = PlotlyGeneratorAgent(save_html=True)

chart_types = [
    "bar_horizontal", "bar_vertical", "pie", "line",
    "bar_vertical_composed", "bar_vertical_stacked", 
    "line_composed", "histogram"
]

for chart_type in chart_types:
    chart_spec = create_spec_for_type(chart_type)  # Sua função
    analytics_result = get_data_for_type(chart_type)  # Sua função
    
    result = agent.generate(chart_spec, analytics_result)
    print(f"{chart_type}: {result['status']}")
```

### Exemplo: Monitorar Performance

```python
agent = PlotlyGeneratorAgent()

# Gerar vários gráficos...
for i in range(100):
    result = agent.generate(chart_spec, analytics_result)

# Ver estatísticas
stats = agent.get_statistics()
print(f"Taxa de sucesso: {stats['success_rate']:.1f}%")
print(f"Tempo médio: {stats['average_render_time']:.3f}s")
```

### Exemplo: Salvar PNG

```python
# Requer: pip install kaleido

agent = PlotlyGeneratorAgent(
    save_html=True,
    save_png=True  # Ativa salvamento PNG
)

result = agent.generate(chart_spec, analytics_result)
print(f"HTML: {result['file_path']}")
```

---

## 🔧 Troubleshooting

### Erro: "Kaleido package not installed"

**Solução:**
```bash
pip install kaleido
```

### Erro: "Unsupported chart_type"

**Solução:** Verifique se o `chart_type` está entre os 8 suportados:
- bar_horizontal, bar_vertical, bar_vertical_composed, bar_vertical_stacked
- line, line_composed, pie, histogram

### Erro: "Column X not found in data"

**Solução:** Certifique-se de que os aliases em `chart_spec` correspondem aos nomes das colunas em `analytics_result.data`.

### Gráfico Não Exibe Valores

**Solução:** Adicione `"show_values": True` em `visual`:
```python
"visual": {"palette": "Blues", "show_values": True}
```

### Performance Lenta

**Solução:** 
- Reduza o número de linhas de dados (use `top_n`)
- Desabilite `save_png` (PNG é mais lento que HTML)
- Use paletas mais simples

---

## 📝 Logs e Depuração

O agente utiliza logging detalhado:

```python
from src.shared_lib.utils.logger import setup_logger

setup_logger(level="DEBUG")  # Ver logs detalhados
```

Logs incluem:
- Validação de inputs
- Seleção de generator
- Tempo de renderização
- Salvamento de arquivos
- Erros com stack traces

---

## 🧪 Testes

### Executar Testes de Integração

```bash
pytest tests/tests_plotly_generator/test_integration.py -v
```

### Executar Benchmark

```bash
python scripts/benchmark_plotly_generator.py
```

### Executar Demo

```bash
python examples/plotly_generator_demo.py
```

---

## 📄 Licença

Este código faz parte do projeto LangGraph Multi-Agent Pipeline.

---

## 👥 Autores

- **Claude Code** - Implementação Fase 5
- **Equipe Target Labs** - Especificação e Planejamento

---

## 📅 Changelog

### v1.0 (2025-11-12)
- ✅ Implementação completa do agente
- ✅ 8 tipos de gráficos suportados
- ✅ Integração com pipeline
- ✅ Performance otimizada (0.022s média)
- ✅ 100% de taxa de sucesso
- ✅ Testes de integração
- ✅ Benchmark automatizado
- ✅ Documentação completa

---

Para mais informações, consulte:
- **Planejamento:** `planning_plotly_generator.md`
- **Especificação de Eixos:** `axis_patterns.md`
- **Tipos de Gráficos:** `CHART_TYPE_SPECS.md`
