"""
Formatador de insights em markdown executivo.

Transforma JSON estruturado em markdown formatado com aparência profissional.
"""

import re
from typing import Dict, List, Any


class ExecutiveMarkdownFormatter:
    """Formata insights estruturados em markdown executivo."""

    # Templates por chart_type - define estrutura de seções
    SECTION_TEMPLATES = {
        "bar_horizontal": [
            "Concentração de Poder",
            "Gap Competitivo",
            "Oportunidades de Diversificação",
            "Riscos de Dependência",
            "Dinâmica Competitiva"
        ],
        "bar_vertical": [
            "Amplitude de Variação",
            "Análise de Extremos",
            "Dispersão Relativa",
            "Padrões de Distribuição"
        ],
        "bar_vertical_composed": [
            "Série Dominante",
            "Variabilidade Entre Séries",
            "Correlações Identificadas",
            "Dinâmica Multi-Série",
            "Oportunidades de Balanceamento"
        ],
        "bar_vertical_stacked": [
            "Composição Total",
            "Contribuição por Componente",
            "Padrões de Empilhamento",
            "Análise de Participação",
            "Recomendações Estratégicas"
        ],
        "line": [
            "Evolução Temporal",
            "Tendência Identificada",
            "Volatilidade e Consistência",
            "Pontos de Inflexão",
            "Projeção Futura"
        ],
        "line_composed": [
            "Evolução Comparativa",
            "Séries Divergentes",
            "Correlações Temporais",
            "Liderança ao Longo do Tempo",
            "Dinâmica Multi-Temporal"
        ],
        "pie": [
            "Concentração Geral",
            "Categoria Dominante",
            "Índice de Diversificação",
            "Fragmentação Identificada",
            "Recomendações de Portfolio"
        ],
        "histogram": [
            "Distribuição de Frequências",
            "Concentração Modal",
            "Assimetria Identificada",
            "Outliers e Extremos",
            "Padrão de Dispersão"
        ]
    }

    def format_insights(self, insights_json: List[Dict[str, str]], chart_type: str) -> str:
        """
        Formata lista de insights JSON em markdown executivo.

        Args:
            insights_json: Lista de dicts com keys: title, formula, interpretation
            chart_type: Tipo de gráfico (bar_horizontal, line, etc.)

        Returns:
            String markdown formatada com H3, marcadores, linhas separadoras
        """
        if not insights_json:
            return ""

        sections = []
        section_titles = self.SECTION_TEMPLATES.get(
            chart_type,
            [insight.get("title", f"Insight {i+1}") for i, insight in enumerate(insights_json)]
        )

        for i, insight in enumerate(insights_json):
            # Usar título do template se disponível, senão usar do JSON
            section_title = section_titles[i] if i < len(section_titles) else insight.get("title", f"Insight {i+1}")

            formula = insight.get("formula", "")
            interpretation = insight.get("interpretation", "")

            # Formatar seção
            section = self._format_section(section_title, formula, interpretation)
            sections.append(section)

        # Juntar seções com linhas separadoras
        return "\n\n---\n\n".join(sections)

    def _format_section(self, title: str, formula: str, interpretation: str) -> str:
        """
        Formata uma seção individual de insight.

        Args:
            title: Título da seção
            formula: Fórmula com cálculos
            interpretation: Interpretação estratégica

        Returns:
            String markdown da seção formatada
        """
        # Título H3
        section_lines = [f"### **{title}**", ""]

        # Formatar fórmula com negrito em valores importantes
        formatted_formula = self._highlight_important_values(formula)
        if formatted_formula:
            section_lines.append(f"* {formatted_formula}")

        # Formatar interpretação (pode ter múltiplas frases/pontos)
        if interpretation:
            interpretation_lines = self._format_interpretation(interpretation)
            section_lines.extend(interpretation_lines)

        return "\n".join(section_lines)

    def _highlight_important_values(self, formula: str) -> str:
        """
        Adiciona negrito em valores numéricos importantes na fórmula.

        Padrões destacados:
        - Valores monetários (R$ X.XXM, X.XXK)
        - Percentuais (X.X%, X%)
        - Operadores importantes (→, =)
        """
        if not formula:
            return ""

        # Destacar valores monetários com M/K
        formula = re.sub(
            r'(R\$\s*[\d.,]+[MKmk])',
            r'**\1**',
            formula
        )

        # Destacar percentuais
        formula = re.sub(
            r'([\d.,]+%)',
            r'**\1**',
            formula
        )

        # Garantir que setas e iguais fiquem visíveis (não em negrito)
        formula = formula.replace('**→**', '→')
        formula = formula.replace('**=**', '=')

        return formula

    def _format_interpretation(self, interpretation: str) -> List[str]:
        """
        Formata texto de interpretação em marcadores.

        Se texto contém múltiplas frases, separa em marcadores.
        Destaca valores importantes em negrito.
        """
        if not interpretation:
            return []

        # Remover emojis (caso existam)
        interpretation = self._remove_emojis(interpretation)

        # Dividir em frases (por ponto final, ponto-e-vírgula ou quebra de linha)
        sentences = re.split(r'[.;]\s+|\n', interpretation.strip())
        sentences = [s.strip() for s in sentences if s.strip()]

        # Formatar como marcadores
        formatted_lines = []
        for sentence in sentences:
            # Adicionar ponto final se não tiver
            if not sentence.endswith('.'):
                sentence += '.'

            # Destacar valores em negrito
            sentence = self._highlight_important_values(sentence)

            formatted_lines.append(f"* {sentence}")

        return formatted_lines

    def _remove_emojis(self, text: str) -> str:
        """Remove todos os emojis do texto."""
        # Padrão para remover emojis Unicode
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # símbolos & pictogramas
            "\U0001F680-\U0001F6FF"  # transporte & símbolos de mapa
            "\U0001F1E0-\U0001F1FF"  # bandeiras (iOS)
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+",
            flags=re.UNICODE
        )
        return emoji_pattern.sub('', text).strip()

    def parse_json_response(self, llm_response: str) -> List[Dict[str, str]]:
        """
        Parseia resposta JSON da LLM.

        Args:
            llm_response: String JSON ou dict da LLM

        Returns:
            Lista de insights estruturados
        """
        import json

        try:
            # Se já é dict, usar diretamente
            if isinstance(llm_response, dict):
                insights_data = llm_response
            else:
                # Tentar parsear como JSON
                insights_data = json.loads(llm_response)

            # Extrair lista de insights
            if "insights" in insights_data:
                return insights_data["insights"]
            elif isinstance(insights_data, list):
                return insights_data
            else:
                return []

        except json.JSONDecodeError as e:
            # Fallback: tentar extrair JSON de texto misto
            return self._extract_json_from_text(llm_response)

    def _extract_json_from_text(self, text: str) -> List[Dict[str, str]]:
        """
        Extrai JSON de texto que pode conter conteúdo adicional.

        Útil para reasoning models que podem adicionar texto antes/depois do JSON.
        """
        import json

        # Procurar por { ou [ no início de JSON
        json_start = -1
        for i, char in enumerate(text):
            if char in ['{', '[']:
                json_start = i
                break

        if json_start == -1:
            return []

        # Procurar fechamento correspondente
        json_end = len(text)
        brace_count = 0
        start_char = text[json_start]
        end_char = '}' if start_char == '{' else ']'

        for i in range(json_start, len(text)):
            if text[i] == start_char:
                brace_count += 1
            elif text[i] == end_char:
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break

        json_str = text[json_start:json_end]

        try:
            data = json.loads(json_str)
            if isinstance(data, dict) and "insights" in data:
                return data["insights"]
            elif isinstance(data, list):
                return data
            return []
        except json.JSONDecodeError:
            return []
