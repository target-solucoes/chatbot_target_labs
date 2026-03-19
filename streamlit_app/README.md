# Streamlit Chatbot - Analytics Pipeline

Aplicacao Streamlit interativa para consultas de dados em linguagem natural com exibicao progressiva em tempo real.

## Estrutura dos Modulos

### `session_state.py`
Gerenciamento de estado da sessao:
- `ChatHistory`: Historico de mensagens
- `FilterStateManager`: Sincronizacao de filtros com backend
- `SessionStateManager`: Gerenciador centralizado de estado

### `pipeline_runner.py`
Execucao do pipeline multi-agente:
- `StreamingPipelineRunner`: Execucao com streaming de estados intermediarios
- `SimplePipelineRunner`: Execucao simples sem streaming
- `PipelineExecutionResult`: Container de resultados

### `progressive_display.py`
Renderizacao progressiva em tempo real:
- `ProgressiveRenderer`: Gerencia containers pre-alocados para cada secao
- `StreamingDisplayManager`: Coordena atualizacoes baseadas em estados do pipeline
- `ProgressiveContainers`: Estrutura de placeholders Streamlit

### `display_components.py`
Funcoes de renderizacao para componentes do JSON formatter:
- `render_executive_summary()`: Titulo e introducao
- `render_filters_badge()`: Badges de filtros ativos
- `render_plotly_chart()`: Grafico Plotly
- `render_insights()`: Narrative + key findings + detailed insights
- `render_next_steps()`: Acoes estrategicas + analises sugeridas
- `render_data_table()`: Tabela de dados + estatisticas
- `render_metadata_debug()`: Informacoes tecnicas e debug
- `render_complete_response()`: Renderizacao completa na ordem correta

## Fluxo de Execucao Progressiva

1. Usuario submete query
2. `app.py` cria `ProgressiveRenderer` e pre-aloca containers
3. `StreamingPipelineRunner.run_with_streaming()` executa workflow com streaming
4. Para cada estado intermediario:
   - `StreamingDisplayManager` processa o estado
   - Atualiza containers relevantes conforme dados chegam
   - Mantem ordem fixa de exibicao
5. Ao finalizar, exibe status de conclusao e limpa loading states
6. Adiciona resposta completa ao historico

## Ordem de Exibicao

A ordem de exibicao e mantida atraves de containers pre-alocados:

1. **Status** (spinner/success)
2. **Titulo + Introducao** (executive_summary)
3. **Filtros Aplicados** (badges)
4. **Grafico Plotly** (visualization.chart)
5. **Insights** (narrative + findings)
6. **Proximos Passos** (actions + suggested analyses)
7. **Tabela de Dados** (data.summary_table)
8. **Debug/Metadata** (expander colapsavel)

## Como Usar

### Executar o App

```bash
streamlit run app.py
```

### Exemplo de Integracao

```python
from streamlit_app.pipeline_runner import StreamingPipelineRunner
from streamlit_app.progressive_display import ProgressiveRenderer, StreamingDisplayManager

# Criar runner
runner = StreamingPipelineRunner()

# Criar renderer
renderer = ProgressiveRenderer()
display_manager = StreamingDisplayManager(renderer)

# Executar com streaming
for state in runner.run_with_streaming("top 5 produtos"):
    display_manager.process_pipeline_state(state)
```

## Funcionalidades Principais

- ✅ **Exibicao Progressiva**: Renderiza cada secao assim que disponivel
- ✅ **Filtros Conversacionais**: Suporta filter_classifier para filtros entre queries
- ✅ **Reset de Filtros**: Botao para limpar filtros sem perder historico
- ✅ **Debug Panel**: Metadata tecnica colapsavel
- ✅ **Chat History**: Mantem historico completo da conversacao
- ✅ **Sincronizacao de Estado**: Integra com `.filter_state.json` do backend
- ✅ **Tratamento de Erros**: Exibe erros sem quebrar outras secoes

## Dependencias

O app utiliza os seguintes modulos do backend:
- `src.pipeline_orchestrator`: Criacao e execucao do workflow
- `src.shared_lib.core.config`: Configuracoes (DATA_PATH, etc.)

Dependencias Python:
- `streamlit`: Framework de interface
- `langgraph`: Workflow streaming
- Todas as dependencias do backend existente

## Notas de Implementacao

### Backend Nao Modificado
O app consome o backend atraves das APIs publicas existentes:
- `create_full_integrated_workflow_with_insights()`
- `initialize_full_pipeline_state()`
- `workflow.stream()` do LangGraph

### Sincronizacao de Filtros
Filtros sao sincronizados via arquivo `.filter_state.json`:
- `FilterStateManager` le/escreve nesse arquivo
- Backend tambem le/escreve no mesmo arquivo
- Garantia de consistencia entre frontend e backend

### Streaming vs Batch
O app suporta ambos os modos:
- **Streaming** (padrao): `run_with_streaming()` para exibicao progressiva
- **Batch**: `run_complete()` para resultado final apenas

### Containers Pre-alocados
A estrategia de pre-alocacao garante:
- Ordem fixa de exibicao (sempre na mesma sequencia)
- Atualizacoes sem re-render de toda a interface
- Performance otimizada (apenas secoes afetadas atualizam)

## Extensoes Futuras

Funcionalidades que podem ser adicionadas:
- [ ] Export de conversas (JSON/PDF)
- [ ] Suggested queries clicaveis que executam nova query
- [ ] Graficos interativos com drill-down
- [ ] Modo escuro
- [ ] Personalizacao de cores/tema
- [ ] Suporte a multiplos datasets
- [ ] Cache de queries recentes
- [ ] Compartilhamento de insights via link
