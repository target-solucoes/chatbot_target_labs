# -*- coding: utf-8 -*-
"""
Alias Manager Component

Componente visual para gerenciar aliases de colunas do dataset.
Permite ao usuário visualizar e editar os aliases semânticos que mapeiam
termos em linguagem natural para colunas reais do banco de dados.
"""

import streamlit as st
from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class AliasManager:
    """
    Gerenciador visual de aliases de colunas.

    Esta primeira versão (mock/frontend-only) permite:
    - Visualizar aliases atuais do alias.yaml
    - Editar aliases por coluna (simulado em memória)
    - Interface organizada com expanders

    Versões futuras incluirão:
    - Salvar modificações no arquivo YAML
    - Validação de aliases duplicados
    - Histórico de mudanças
    """

    def __init__(self, alias_file_path: Optional[str] = None):
        """
        Inicializa o gerenciador de aliases.

        Args:
            alias_file_path: Caminho para o arquivo alias.yaml.
                           Se None, usa o caminho padrão.
        """
        if alias_file_path is None:
            # Caminho padrão relativo ao projeto
            self.alias_file_path = Path("data/mappings/alias.yaml")
        else:
            self.alias_file_path = Path(alias_file_path)

        # Cache de dados do YAML em memória (por sessão)
        self._init_session_state()

    def _init_session_state(self):
        """Inicializa estado da sessão para armazenar aliases modificados."""
        if "alias_manager_data" not in st.session_state:
            st.session_state.alias_manager_data = None

        if "alias_manager_modified" not in st.session_state:
            st.session_state.alias_manager_modified = False

    def load_aliases(self) -> Dict[str, Any]:
        """
        Carrega aliases do arquivo YAML.

        Returns:
            Dicionário com estrutura completa do alias.yaml
        """
        try:
            if not self.alias_file_path.exists():
                logger.error(
                    f"Arquivo de aliases não encontrado: {self.alias_file_path}"
                )
                return {
                    "columns": {},
                    "column_types": {},
                    "metrics": {},
                    "conventions": {},
                }

            with open(self.alias_file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            logger.info(f"Aliases carregados de {self.alias_file_path}")
            return data or {}

        except Exception as e:
            logger.error(f"Erro ao carregar aliases: {e}", exc_info=True)
            return {"columns": {}, "column_types": {}, "metrics": {}, "conventions": {}}

    def get_session_aliases(self) -> Dict[str, Any]:
        """
        Retorna aliases da sessão (modificados ou originais).

        Returns:
            Dicionário com aliases (versão modificada se houver)
        """
        if st.session_state.alias_manager_data is None:
            # Primeira vez: carrega do arquivo
            st.session_state.alias_manager_data = self.load_aliases()

        return st.session_state.alias_manager_data

    def get_all_dataset_columns(self) -> List[str]:
        """
        Retorna lista de todas as colunas do dataset.

        Combina colunas de column_types (numeric, categorical, temporal).

        Returns:
            Lista ordenada de nomes de colunas
        """
        data = self.get_session_aliases()
        column_types = data.get("column_types", {})

        all_columns = set()
        for column_list in column_types.values():
            if isinstance(column_list, list):
                all_columns.update(column_list)

        return sorted(all_columns)

    def get_column_aliases(self, column_name: str) -> List[str]:
        """
        Retorna aliases de uma coluna específica.

        Args:
            column_name: Nome da coluna

        Returns:
            Lista de aliases (vazia se coluna não tiver aliases)
        """
        data = self.get_session_aliases()
        columns = data.get("columns", {})
        return columns.get(column_name, [])

    def get_column_type(self, column_name: str) -> Optional[str]:
        """
        Retorna tipo da coluna (numeric, categorical, temporal).

        Args:
            column_name: Nome da coluna

        Returns:
            Tipo da coluna ou None se não encontrada
        """
        data = self.get_session_aliases()
        column_types = data.get("column_types", {})

        for type_name, columns in column_types.items():
            if isinstance(columns, list) and column_name in columns:
                return type_name

        return None

    def update_column_aliases(self, column_name: str, new_aliases: List[str]):
        """
        Atualiza aliases de uma coluna (apenas em memória).

        Args:
            column_name: Nome da coluna
            new_aliases: Nova lista de aliases
        """
        data = self.get_session_aliases()

        if "columns" not in data:
            data["columns"] = {}

        # Remove aliases vazios ou apenas espaços
        cleaned_aliases = [a.strip() for a in new_aliases if a.strip()]

        data["columns"][column_name] = cleaned_aliases
        st.session_state.alias_manager_data = data
        st.session_state.alias_manager_modified = True

        logger.info(
            f"Aliases atualizados para '{column_name}': {len(cleaned_aliases)} aliases"
        )

    def add_alias(self, column_name: str, new_alias: str):
        """
        Adiciona um novo alias a uma coluna.

        Args:
            column_name: Nome da coluna
            new_alias: Novo alias a adicionar
        """
        current_aliases = self.get_column_aliases(column_name)

        # Evita duplicatas (case-insensitive)
        normalized_aliases = [a.lower() for a in current_aliases]
        if new_alias.lower() not in normalized_aliases:
            current_aliases.append(new_alias)
            self.update_column_aliases(column_name, current_aliases)
            return True

        return False

    def remove_alias(self, column_name: str, alias_to_remove: str):
        """
        Remove um alias de uma coluna.

        Args:
            column_name: Nome da coluna
            alias_to_remove: Alias a remover
        """
        current_aliases = self.get_column_aliases(column_name)

        if alias_to_remove in current_aliases:
            current_aliases.remove(alias_to_remove)
            self.update_column_aliases(column_name, current_aliases)
            return True

        return False

    def reset_to_original(self):
        """Reseta aliases para versão original do arquivo."""
        st.session_state.alias_manager_data = None
        st.session_state.alias_manager_modified = False
        logger.info("Aliases resetados para versão original")

    def render(self):
        """
        Renderiza interface completa do gerenciador de aliases.

        Mostra:
        - Informações sobre o sistema de aliases
        - Lista de colunas com expanders
        - Editor de aliases por coluna
        - Indicadores de modificações
        """
        st.markdown("## 🔤 Gerenciador de Aliases")

        # Info box sobre o sistema
        with st.expander("ℹ️ Sobre o Sistema de Aliases", expanded=False):
            st.markdown("""
            ### O que são Aliases Semânticos?
            
            Aliases são **sinônimos em linguagem natural** que o sistema usa para entender 
            suas perguntas e mapeá-las para as colunas reais do dataset.
            
            **Exemplo:**
            - Quando você pergunta sobre **"vendas"**, o sistema entende que você quer ver a coluna `Valor_Vendido`
            - Termos como **"faturamento"**, **"receita"**, **"valor total"** também mapeiam para `Valor_Vendido`
            
            ### Como Funciona?
            
            1. **Você faz uma pergunta** em linguagem natural
            2. **O sistema extrai os termos-chave** da sua pergunta
            3. **Consulta os aliases** para encontrar as colunas corretas
            4. **Gera a análise** usando as colunas mapeadas
            
            ### Benefícios
            
            - ✅ **Zero alucinações**: Apenas colunas reais são usadas
            - ✅ **Compreensão flexível**: Aceita múltiplas formas de expressar a mesma coisa
            - ✅ **Customizável**: Você pode adicionar seus próprios termos
            - ✅ **Eficiente**: Sem necessidade de processar LLM para cada termo
            
            ---
            
            **📝 Versão Atual:** Frontend Mock (modificações não são salvas permanentemente)
            
            **🚀 Próximas Versões:** Salvamento persistente, validação avançada, histórico de mudanças
            """)

        st.markdown("---")

        # Aviso de modo demonstrativo (sempre visivel) com borda destacada
        st.markdown(
            """
        <style>
        .demo-warning {
            background-color: #fff3cd;
            border-left: 6px solid #ffc107;
            border-radius: 10px;
            padding: 16px;
            margin: 12px 0;
        }
        .demo-warning h4 {
            color: #856404;
            margin-bottom: 6px;
        }
        .demo-warning p {
            color: #856404;
            line-height: 1.5;
        }
        </style>
        """,
            unsafe_allow_html=True,
        )

        st.markdown(
            """
        <div class="demo-warning">
            <h4>⚠️ Modo Demonstrativo</h4>
            <p>
                Este ambiente é destinado exclusivamente à demonstração do sistema de aliases semânticos, simulando o funcionamento esperado da aplicação por meio de um protótipo visual.
            </p>
            <p>
                Todas interações realizadas têm caráter apenas ilustrativo e não representam alterações reais no sistema.
            </p>
            <p>
                A funcionalidade de edição será disponibilizada apenas para usuários internos devidamente autorizados.
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

        # Botões de ação global
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            st.markdown("### 📋 Colunas do Dataset")

        with col2:
            if st.button(
                "🔄 Resetar Tudo",
                help="Descartar modificações e voltar ao original",
                key="reset_all_aliases",
                use_container_width=True,
            ):
                self.reset_to_original()
                st.rerun()

        with col3:
            # Placeholder para futura função de salvar
            if st.button(
                "💾 Salvar",
                help="Funcionalidade em desenvolvimento",
                disabled=True,
                key="save_aliases",
                use_container_width=True,
            ):
                st.warning("Função de salvamento será implementada em versão futura")

        st.markdown("---")

        # Lista de colunas
        all_columns = self.get_all_dataset_columns()

        if not all_columns:
            st.warning("Nenhuma coluna encontrada no arquivo de aliases.")
            return

        # Filtro de busca
        search_term = st.text_input(
            "🔍 Buscar coluna",
            placeholder="Digite para filtrar colunas...",
            help="Filtre colunas pelo nome",
        )

        # Filtra colunas se houver busca
        if search_term:
            filtered_columns = [
                col for col in all_columns if search_term.lower() in col.lower()
            ]
        else:
            filtered_columns = all_columns

        if not filtered_columns:
            st.info(f"Nenhuma coluna encontrada com o termo '{search_term}'")
            return

        st.markdown(f"**{len(filtered_columns)}** colunas encontradas")
        st.markdown("<br>", unsafe_allow_html=True)

        # Renderiza cada coluna com expander
        for column_name in filtered_columns:
            self._render_column_editor(column_name)

    def _render_column_editor(self, column_name: str):
        """
        Renderiza editor de aliases para uma coluna específica.

        Args:
            column_name: Nome da coluna
        """
        # Informações da coluna
        column_type = self.get_column_type(column_name)
        current_aliases = self.get_column_aliases(column_name)

        # Ícone por tipo
        type_icons = {"numeric": "🔢", "categorical": "🏷️", "temporal": "📅"}
        type_icon = type_icons.get(column_type, "❓")

        # Badge de contagem de aliases
        alias_count = len(current_aliases)
        count_emoji = "✅" if alias_count > 0 else "⚪"

        # Expander title - usando apenas texto e emojis (sem HTML)
        expander_title = f"{type_icon} **{column_name}**  •  `{column_type or 'unknown'}`  •  {count_emoji} {alias_count} aliases"

        with st.expander(expander_title, expanded=False):
            # Tipo da coluna
            st.markdown(f"**Tipo:** `{column_type or 'Não classificado'}`")

            if column_type == "numeric":
                st.caption("🔢 Coluna quantitativa - usa SUM() como agregação padrão")
            elif column_type == "categorical":
                st.caption("🏷️ Coluna qualitativa - usa COUNT() ou COUNT DISTINCT")
            elif column_type == "temporal":
                st.caption("📅 Coluna de data/tempo - tratamento temporal especial")

            st.markdown("---")

            # Lista de aliases atuais
            st.markdown("**Aliases Atuais:**")

            if not current_aliases:
                st.info("Nenhum alias definido para esta coluna.")
            else:
                # Renderiza cada alias com botão de remover
                for i, alias in enumerate(current_aliases):
                    col1, col2 = st.columns([0.9, 0.1])

                    with col1:
                        st.markdown(f"• `{alias}`")

                    with col2:
                        if st.button(
                            "🗑️",
                            key=f"remove_{column_name}_{i}",
                            help=f"Remover '{alias}'",
                        ):
                            if self.remove_alias(column_name, alias):
                                st.success(f"Alias '{alias}' removido!")
                                st.rerun()

            st.markdown("---")

            # Adicionar novo alias
            st.markdown("**Adicionar Novo Alias:**")

            new_alias = st.text_input(
                "Novo termo",
                key=f"new_alias_{column_name}",
                placeholder="Digite um novo sinônimo...",
                help=f"Adicione um novo alias para {column_name}",
                label_visibility="collapsed",
            )

            col1, col2 = st.columns([0.7, 0.3])

            with col2:
                if st.button(
                    "➕ Adicionar",
                    key=f"add_btn_{column_name}",
                    use_container_width=True,
                ):
                    if new_alias and new_alias.strip():
                        if self.add_alias(column_name, new_alias.strip()):
                            st.success(f"✅ Alias '{new_alias}' adicionado!")
                            st.rerun()
                        else:
                            st.warning(
                                f"⚠️ Alias '{new_alias}' já existe (case-insensitive)"
                            )
                    else:
                        st.error("Digite um alias válido")

            # Dicas contextuais
            st.markdown("---")
            with st.expander("💡 Dicas para Aliases Eficazes", expanded=False):
                st.markdown("""
                **Boas Práticas:**
                
                1. **Use termos do dia a dia** que você e sua equipe usam naturalmente
                2. **Inclua variações** (singular/plural, com/sem acentos)
                3. **Adicione sinônimos** de diferentes áreas (contábil, comercial, etc.)
                4. **Frases contextuais** também funcionam ("total de vendas", "valor da venda")
                
                **Exemplos para Valor_Vendido:**
                - ✅ "vendas", "venda", "faturamento", "receita"
                - ✅ "valor total", "valor da venda", "total de vendas"
                - ✅ "quanto vendemos", "valor comercializado"
                
                **Evite:**
                - ❌ Aliases muito genéricos ("total", "valor") sem contexto
                - ❌ Termos ambíguos que poderiam se referir a múltiplas colunas
                - ❌ Duplicatas exatas (o sistema detecta automaticamente)
                """)

    def render_summary_stats(self):
        """
        Renderiza estatísticas resumidas sobre os aliases.

        Útil para sidebar ou dashboard.
        """
        data = self.get_session_aliases()
        columns_dict = data.get("columns", {})

        total_columns = len(self.get_all_dataset_columns())
        columns_with_aliases = len([col for col in columns_dict.values() if col])
        total_aliases = sum(len(aliases) for aliases in columns_dict.values())

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("📊 Colunas", total_columns)

        with col2:
            st.metric("✅ Com Aliases", columns_with_aliases)

        with col3:
            st.metric("🔤 Total Aliases", total_aliases)

        # Progress bar
        if total_columns > 0:
            coverage = (columns_with_aliases / total_columns) * 100
            st.progress(coverage / 100)
            st.caption(f"Cobertura: {coverage:.1f}%")


# Singleton instance para uso global
_alias_manager_instance = None


def get_alias_manager() -> AliasManager:
    """
    Retorna instância singleton do AliasManager.

    Returns:
        Instância compartilhada do AliasManager
    """
    global _alias_manager_instance

    if _alias_manager_instance is None:
        _alias_manager_instance = AliasManager()

    return _alias_manager_instance
