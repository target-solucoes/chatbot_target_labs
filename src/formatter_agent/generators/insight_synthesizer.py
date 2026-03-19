"""
InsightSynthesizer - Synthesizes insights into cohesive narrative via LLM
===========================================================================

Responsible for:
- Transforming individual insights into connected narrative
- Extracting 3-5 key findings as bullet points
- Prioritizing insights by confidence and category
- Providing fallback synthesis when LLM fails
"""

import json
import logging
import time
from typing import Dict, Any, List

from langchain_google_genai import ChatGoogleGenerativeAI

from ..models.formatter_schemas import SynthesizedInsightsOutput
from ..core.settings import get_retry_config

logger = logging.getLogger(__name__)


class InsightSynthesizer:
    """
    Synthesizes individual insights into cohesive narrative via Google Gemini LLM.

    Takes raw insights from insight_generator and creates:
    - Connected narrative paragraph (200-500 chars)
    - 3-5 key findings as concise bullet points
    - Transparency validation tracking
    """

    # Insight categories for classification
    INSIGHT_CATEGORIES = {
        "concentração": ["concentração", "distribuição", "dominância", "dominante"],
        "gap_competitivo": ["gap", "diferença", "distância", "vantagem"],
        "tendência": ["crescimento", "queda", "variação", "tendência", "evolução"],
        "diversidade": ["diversidade", "diversificação", "variedade"],
    }

    def __init__(self, llm: ChatGoogleGenerativeAI):
        """
        Initialize synthesizer with LLM instance.

        Args:
            llm: Configured ChatGoogleGenerativeAI instance
        """
        self.llm = llm
        self.retry_config = get_retry_config()
        logger.info("InsightSynthesizer initialized")

    def synthesize(
        self,
        insights: List[Dict[str, Any]],
        parsed_inputs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Synthesize insights into narrative and key findings.

        Args:
            insights: List of insights from insight_generator
            parsed_inputs: Parsed input data for context

        Returns:
            Dictionary containing:
            {
                "narrative": str (cohesive paragraph),
                "key_findings": List[str] (3-5 bullets),
                "detailed_insights": List[Dict] (processed insights),
                "transparency_validated": bool,
                "_fallback_used": bool (if fallback was triggered)
            }
        """
        start_time = time.time()

        logger.info(f"Synthesizing {len(insights)} insights")

        # Handle empty insights
        if not insights:
            logger.warning("No insights provided for synthesis")
            return self._empty_synthesis()

        # Process insights
        processed_insights = self._process_insights(insights)

        # Build prompt
        prompt = self._build_prompt(processed_insights, parsed_inputs)

        # Try LLM synthesis with retry
        for attempt in range(1, self.retry_config["max_attempts"] + 1):
            try:
                logger.debug(
                    f"LLM call attempt {attempt}/{self.retry_config['max_attempts']}"
                )

                response = self.llm.invoke(prompt)
                result = self._parse_response(response.content)

                # Capture tokens from LLM response
                from src.shared_lib.utils.token_tracker import extract_token_usage

                tokens = extract_token_usage(response, self.llm)
                logger.debug(f"[InsightSynthesizer] Tokens used: {tokens}")

                # Add processed insights and transparency info
                result["detailed_insights"] = processed_insights
                result["transparency_validated"] = all(
                    self._has_formula(ins) for ins in insights
                )
                result["_fallback_used"] = False
                result["_tokens"] = tokens  # Include tokens for aggregation

                execution_time = time.time() - start_time
                logger.info(
                    f"Insights synthesized successfully in {execution_time:.2f}s"
                )
                return result

            except Exception as e:
                logger.warning(
                    f"LLM call attempt {attempt} failed: {e}",
                    exc_info=(attempt == self.retry_config["max_attempts"]),
                )

                if attempt < self.retry_config["max_attempts"]:
                    time.sleep(self.retry_config["delay"])
                else:
                    # All retries exhausted, use fallback
                    logger.error(
                        "All LLM attempts failed for insight synthesis. Using fallback."
                    )
                    result = self._fallback_synthesis(insights)
                    result["detailed_insights"] = processed_insights
                    execution_time = time.time() - start_time
                    logger.info(
                        f"Fallback synthesis generated in {execution_time:.2f}s"
                    )
                    return result

    def _build_prompt(
        self,
        insights: List[Dict[str, Any]],
        parsed_inputs: Dict[str, Any],
    ) -> str:
        """
        Build LLM prompt for insight synthesis.

        Args:
            insights: Processed insights
            parsed_inputs: Parsed input data for context

        Returns:
            Formatted prompt string
        """
        chart_type = parsed_inputs.get("chart_type", "")
        query = parsed_inputs.get("query", "")

        # Format insights for prompt
        insights_text = self._format_insights_for_prompt(insights)

        prompt = f"""Você é um analista sênior de business intelligence com expertise em transformar dados brutos em narrativas estratégicas. Sua missão é sintetizar os insights gerados pelo sistema em uma narrativa executiva clara, fluida e explicativa — como se estivesse explicando os achados para um diretor ou executivo C-level.

═══════════════════════════════════════════════════════════════════
📊 CONTEXTO DA ANÁLISE
═══════════════════════════════════════════════════════════════════

Tipo de análise: {chart_type}
Query original: "{query}"

═══════════════════════════════════════════════════════════════════
🔍 INSIGHTS IDENTIFICADOS (gerados por sistema automatizado)
═══════════════════════════════════════════════════════════════════

{insights_text}

═══════════════════════════════════════════════════════════════════
🎯 SUA TAREFA
═══════════════════════════════════════════════════════════════════

Transforme estes insights técnicos em uma narrativa executiva que seja:
• **Explicativa e contextualizada**: não apenas relate números, mas explique o que eles significam
• **Fluida e natural**: use transições lógicas entre ideias, como um analista contando uma história
• **Estratégica e profissional**: mantenha tom consultivo, destacando implicações de negócio
• **Completa sem ser verborrágica**: seja detalhado onde necessário, mas mantenha foco e clareza

═══════════════════════════════════════════════════════════════════
📝 FORMATO DE SAÍDA (JSON rigoroso)
═══════════════════════════════════════════════════════════════════

{{
  "narrative": "Narrativa executiva fluida e explicativa...",
  "key_findings": [
    "Bullet 1: síntese objetiva do primeiro insight crítico",
    "Bullet 2: síntese objetiva do segundo insight crítico",
    "Bullet 3: síntese objetiva do terceiro insight crítico",
    "(opcional) Bullets 4-5 se houver insights adicionais relevantes"
  ]
}}

═══════════════════════════════════════════════════════════════════
📐 DIRETRIZES DETALHADAS PARA O CAMPO `narrative`
═══════════════════════════════════════════════════════════════════

🔹 **ESTRUTURA E EXTENSÃO**
   → Escreva entre 400-800 caracteres
   → Divida em 2-4 sentenças naturais com transições fluidas
   → Cada sentença deve adicionar valor interpretativo, não apenas descrever
   → Use conectivos lógicos: "ainda assim", "por outro lado", "nesse contexto", "em contraste", "simultaneamente"

🔹 **CONTEÚDO E EXPLICABILIDADE** ⚠️ CRÍTICO
   → **SEMPRE conecte conclusões aos dados**: cada afirmação deve ser seguida ou precedida pelo número que a fundamenta
   → **Formato obrigatório**: "[Conclusão] baseada em [número/métrica específica]"
   → **Exemplo**: "vulnerável a choques nos top 3" deve ser "concentram **68,28%** do total, indicando alta vulnerabilidade a choques no Top 3"
   → Use números concretos extraídos das fórmulas dos insights (percentuais, valores absolutos, proporções, gaps)
   → **Explique o que os números significam** — transforme estatística em insight estratégico
   → Identifique **relações causais** ou **implicações estratégicas**: "o que torna difícil reverter...", "indicando espaço real para..."
   → Destaque **padrões** com evidências: concentração (%), assimetria (valores), gaps (diferenças absolutas/relativas)
   → Contextualize com conectores explicativos: "indicando...", "o que torna...", "apontando para...", "sinalizando..."

🔹 **ELIMINAÇÃO DE REDUNDÂNCIAS** ⚠️ CRÍTICO
   → **NÃO repita** a mesma informação em frases diferentes
   → **Consolide mensagens similares** em uma única sentença mais rica
   → Exemplo INCORRETO: "foco de 3 setores domina 68%" + "concentração eleva exposição: perda de top 3 impacta"
   → Exemplo CORRETO: "os três principais setores concentram **68,28%** do total, indicando alta vulnerabilidade a choques no Top 3"
   → Cada sentença deve trazer um insight DISTINTO (não reformulação do anterior)

🔹 **TOM E ESTILO**
   → Tom consultivo e estratégico (como um analista sênior explicando para executivos)
   → Evite linguagem telegráfica ("foco de 3 setores domina 68%")
   → Prefira frases completas e explicativas ("os três principais setores concentram **68,28%** do total")
   → Use vocabulário preciso, mas acessível
   → Mantenha fluência e coesão entre as sentenças

🔹 **PRIORIZAÇÃO DE INSIGHTS**
   → Dê maior peso a insights com confidence > 0.8
   → Foque em descobertas acionáveis e com impacto estratégico
   → Conecte insights relacionados (ex: concentração + gap competitivo)
   → Identifique tensões ou oportunidades (ex: dominância vs. risco de dependência)

🔹 **EXEMPLO DE EVOLUÇÃO DA NARRATIVA**

   ❌ **Formato INCORRETO (telegráfico, redundante, sem evidências integradas):**
   "A análise revela que foco de 3 setores domina 68% do total; vulnerável a choques nos top 3. Além disso, liderança robusta; vantagem difícil de reverter no curto prazo. Adicionalmente, potencial de crescimento distribuído. foco em diversificação de portfólio. Por fim, concentração eleva exposição: perda de top 3 impacta significativamente."
   
   ⚠️ **Problemas identificados**:
   - Conclusões sem evidências numéricas específicas ("domina 68%" sem conectar ao significado)
   - Redundância: concentração mencionada 2x de formas diferentes
   - Linguagem telegráfica ("foco de 3 setores")
   - Falta de conexão entre dados e interpretação

   ✅ **Formato CORRETO (explicável, não-redundante, baseado em evidências):**
   "A análise mostra que os três principais setores concentram **68,28%** do total, indicando alta vulnerabilidade a choques no Top 3. A liderança mantém uma vantagem sólida de **R$ 2,85M**, equivalente a **103,77%** acima do segundo colocado, o que torna difícil reverter a posição no curto prazo. A cauda representa **31,72%** do total, indicando espaço real para crescimento distribuído e reforçando a necessidade de diversificação."
   
   ✅ **Elementos bem implementados**:
   - Cada conclusão vinculada a um número específico
   - Sem redundâncias (concentração mencionada 1x com evidência clara)
   - Linguagem fluida e profissional
   - Conectores explicativos ("indicando", "o que torna", "reforçando")

═══════════════════════════════════════════════════════════════════
📋 DIRETRIZES PARA `key_findings`
═══════════════════════════════════════════════════════════════════

🔹 **FORMATO**
   → Mínimo 3, máximo 5 bullets
   → Cada bullet: máximo 140 caracteres (anteriormente 120, agora mais espaço)
   → Inclua valores numéricos concretos sempre que possível
   → Use linguagem objetiva e direta

🔹 **CONTEÚDO**
   → Cada bullet deve capturar UM insight específico
   → Seja acionável: o leitor deve entender a implicação
   → Evite redundância com a narrativa (não repita exatamente as mesmas frases)
   → Priorize insights de alta confiança e relevância estratégica

═══════════════════════════════════════════════════════════════════
⚠️ RESTRIÇÕES TÉCNICAS
═══════════════════════════════════════════════════════════════════

✔ Retorne APENAS JSON válido (sem markdown, sem texto antes/depois)
✔ Não use emojis no JSON de saída
✔ Não invente números — use apenas os dados fornecidos nos insights
✔ Mantenha precisão numérica (percentuais com 1-2 casas decimais)
✔ Seja fiel ao contexto do chart_type e da query original

═══════════════════════════════════════════════════════════════════
📘 EXEMPLO DE SAÍDA COMPLETA
═══════════════════════════════════════════════════════════════════

{{
  "narrative": "A análise da distribuição de faturamento revela concentração crítica de **68,3%** nos três principais clientes, sinalizando alta dependência estrutural e risco estratégico significativo. O líder isolado detém vantagem competitiva de **R$ 2,45M** sobre o segundo colocado (equivalente a **60,7%** acima), consolidando posição dominante no segmento. Os dois últimos clientes representam **11,7%** do total, apontando para oportunidades de crescimento na base menos explorada e reforçando a necessidade de diversificação para mitigar riscos de concentração excessiva.",
  "key_findings": [
    "Top 3 clientes concentram 68,3% do faturamento (R$ 8,5M), criando alta dependência estrutural",
    "Líder detém R$ 4,05M (32,1%), com vantagem de 60,7% sobre o segundo (R$ 2,52M)",
    "Base inferior representa 11,7% do total (R$ 1,46M), sinalizando oportunidades de crescimento"
  ]
}}

═══════════════════════════════════════════════════════════════════

Agora, processe os insights fornecidos e gere a narrativa executiva seguindo rigorosamente estas diretrizes."""

        return prompt

    def _parse_response(self, content: str) -> Dict[str, Any]:
        """
        Parse and validate LLM JSON response.

        Args:
            content: Raw JSON string from LLM

        Returns:
            Validated dictionary with narrative and key_findings

        Raises:
            ValueError: If parsing or validation fails
        """
        try:
            data = json.loads(content)
            validated = SynthesizedInsightsOutput(**data)
            return validated.model_dump()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            raise ValueError(f"Invalid JSON from LLM: {e}")
        except Exception as e:
            logger.error(f"Failed to validate LLM response: {e}")
            raise ValueError(f"Validation error: {e}")

    def _fallback_synthesis(self, insights: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate fallback synthesis when LLM fails.
        Builds evidence-based narrative by extracting numbers from formulas.

        Args:
            insights: Raw insights list

        Returns:
            Dictionary with evidence-based synthesis
        """
        # Extract key findings with numerical evidence from formulas
        key_findings = []
        for ins in insights[:5]:  # Max 5
            interpretation = ins.get("interpretation", "")
            formula = ins.get("formula", "")

            # Extract key numbers from formula to enrich interpretation
            numbers = self._extract_numbers_from_formula(formula)

            # Build finding with interpretation + key evidence
            if numbers:
                # Integrate numbers into the interpretation
                finding = (
                    f"{interpretation} ({', '.join(numbers[:2])})"  # Max 2 key numbers
                )
            else:
                finding = interpretation

            key_findings.append(finding[:140])

        # Create evidence-based narrative from insights
        narrative_parts = []
        used_categories = set()  # Track categories to avoid redundancy
        insights_used = 0

        for ins in insights[:4]:
            interpretation = ins.get("interpretation", "")
            formula = ins.get("formula", "")
            category = ins.get("category", "geral")

            if not interpretation:
                continue

            # Skip if category already covered (avoid redundancy)
            if category in used_categories and category != "geral":
                continue

            used_categories.add(category)

            # Extract key metrics from formula
            numbers = self._extract_numbers_from_formula(formula)

            # Build interpretation with numbers integrated
            interp_with_numbers = self._integrate_numbers_into_interpretation(
                interpretation, numbers
            )

            # Build sentence with integrated evidence
            if insights_used == 0:
                # First sentence: set context with data
                sentence = f"A análise mostra que {interp_with_numbers.lower()}"
            elif category in ["diversidade", "tendência"]:
                # Use contrastive connector for diversity/trends
                clean_interp = interp_with_numbers.lower()
                # Check for various cauda patterns
                for prefix in ["a cauda ", "cauda ", "o potencial "]:
                    if clean_interp.startswith(prefix):
                        clean_interp = clean_interp[len(prefix) :]
                        break

                if "cauda" in interpretation.lower():
                    sentence = f"A cauda {clean_interp}"
                elif "potencial" in interpretation.lower():
                    sentence = f"O potencial de crescimento {clean_interp}"
                else:
                    sentence = f"Por outro lado, {interp_with_numbers.lower()}"
            elif category == "gap_competitivo":
                # Leadership sentence
                clean_interp = interp_with_numbers.lower()
                # Check for various leadership patterns
                for prefix in ["a liderança ", "liderança "]:
                    if clean_interp.startswith(prefix):
                        clean_interp = clean_interp[len(prefix) :]
                        break
                sentence = f"A liderança {clean_interp}"
            else:
                # Other sentences
                connectors = ["Além disso", "Adicionalmente", "Por fim"]
                connector = connectors[min(insights_used - 1, len(connectors) - 1)]
                sentence = f"{connector}, {interp_with_numbers.lower()}"

            narrative_parts.append(sentence.rstrip("."))
            insights_used += 1

        # Join sentences with proper punctuation
        narrative = ". ".join(narrative_parts) + "." if narrative_parts else ""

        # Clean up double periods and spaces
        narrative = narrative.replace("..", ".").replace("  ", " ")

        # Ensure reasonable length - add context if needed
        if narrative and len(narrative) < 300:
            # Significantly under target - add analytical context
            narrative = (
                narrative.rstrip(".")
                + ", fornecendo base analítica para decisões estratégicas e identificando oportunidades de otimização."
            )
        elif narrative and len(narrative) < 400:
            # Slightly under target - add lighter context
            narrative = (
                narrative.rstrip(".") + ", fornecendo base para decisões estratégicas."
            )

        # Truncate to max length if needed
        narrative = narrative[:800]

        # Use generated narrative if available, otherwise fallback to generic
        if not narrative or len(narrative.strip()) < 100:
            narrative = "A análise dos dados revelou padrões significativos que merecem atenção estratégica. Os insights identificados fornecem uma visão abrangente da situação atual e podem orientar decisões táticas e operacionais. Recomenda-se análise detalhada dos números apresentados para maximizar oportunidades e mitigar riscos identificados."

        return {
            "narrative": narrative,
            "key_findings": key_findings
            if key_findings
            else ["Nenhum insight detalhado disponível"],
            "transparency_validated": all(self._has_formula(ins) for ins in insights),
            "_fallback_used": True,
            "_tokens": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        }

    def _empty_synthesis(self) -> Dict[str, Any]:
        """
        Return empty synthesis when no insights are provided.

        Returns:
            Dictionary with empty/default values
        """
        return {
            "narrative": "Nenhum insight foi gerado para esta análise. Os dados fornecidos podem não apresentar padrões estatisticamente significativos, ou a análise pode requerer ajustes nos parâmetros de configuração para identificar tendências relevantes. Recomenda-se revisar os critérios de filtragem e a qualidade dos dados de entrada para maximizar a geração de insights acionáveis.",
            "key_findings": ["Nenhum insight disponível para os dados analisados"],
            "detailed_insights": [],
            "transparency_validated": False,
            "_fallback_used": True,
            "_tokens": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        }

    def _process_insights(self, insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process insights for standardized format.

        Args:
            insights: Raw insights from insight_generator

        Returns:
            List of processed insights with additional metadata
        """
        processed = []
        for ins in insights:
            processed.append(
                {
                    "title": ins.get("title", ""),
                    "interpretation": ins.get("interpretation", ""),
                    "formula": ins.get("formula", ""),
                    "confidence": ins.get("confidence", 0.0),
                    "category": self._categorize_insight(ins),
                    "has_formula": self._has_formula(ins),
                }
            )
        return processed

    def _categorize_insight(self, insight: Dict[str, Any]) -> str:
        """
        Categorize insight based on keywords in title.

        Args:
            insight: Insight dictionary

        Returns:
            Category string (concentração, gap_competitivo, tendência, diversidade, geral)
        """
        title_lower = insight.get("title", "").lower()

        for category, keywords in self.INSIGHT_CATEGORIES.items():
            if any(kw in title_lower for kw in keywords):
                return category

        return "geral"

    def _has_formula(self, insight: Dict[str, Any]) -> bool:
        """
        Check if insight has explicit formula.

        Args:
            insight: Insight dictionary

        Returns:
            True if formula contains calculation symbols
        """
        formula = insight.get("formula", "")
        if not formula:
            return False

        # Check for calculation symbols
        return any(symbol in formula for symbol in ["/", "→", "=", "+", "-", "*"])

    def _extract_numbers_from_formula(self, formula: str) -> List[str]:
        """
        Extract key numerical values from formula string.

        Args:
            formula: Formula string with calculations and values

        Returns:
            List of formatted numerical strings (percentages, values, etc.)
        """
        import re

        if not formula:
            return []

        numbers = []

        # Extract percentages (e.g., "68,28%", "7.38%")
        percentages = re.findall(r"\d+[.,]?\d*%", formula)
        numbers.extend(percentages[:2])  # Keep top 2

        # Extract currency values (e.g., "R$ 2.85M", "24.46M")
        currency = re.findall(r"R?\$?\s*\d+[.,]?\d*[MKB]?", formula)
        for val in currency[:2]:
            if val not in numbers:  # Avoid duplicates
                numbers.append(val.strip())

        # If still empty, extract raw numbers
        if not numbers:
            raw_numbers = re.findall(r"\d+[.,]\d+", formula)
            numbers.extend(raw_numbers[:1])

        return numbers[:3]  # Max 3 key numbers

    def _integrate_numbers_into_interpretation(
        self, interpretation: str, numbers: List[str]
    ) -> str:
        """
        Integrate extracted numbers into interpretation text intelligently.

        Args:
            interpretation: Original interpretation text
            numbers: List of extracted numbers from formula

        Returns:
            Interpretation with numbers integrated naturally (WITHOUT markdown bold)
        """
        if not numbers:
            return interpretation

        import re

        # Strategy: append key numbers at logical points without modifying the interpretation
        # This avoids text duplication and maintains original phrasing
        # NOTE: DO NOT add markdown ** here - it will be handled by the frontend rendering

        result = interpretation

        # Check if interpretation already has percentages or values embedded
        has_percent = bool(re.search(r"\d+[.,]\d+%", interpretation))
        has_value = bool(re.search(r"\d+[.,]\d+[MKB]", interpretation, re.IGNORECASE))

        # If already has numbers, just return as-is (they're already in the text)
        if has_percent or has_value:
            return result

        # Otherwise, intelligently append numbers WITHOUT markdown formatting
        # Remove trailing punctuation temporarily
        result = result.rstrip(".;,")

        # Add numbers based on context - plain text only
        if len(numbers) == 1:
            result = f"{result} ({numbers[0]})"
        elif len(numbers) >= 2:
            # Check context to decide which numbers to show
            if "%" in numbers[0]:  # First is percentage
                if "M" in numbers[1] or "m" in numbers[1]:  # Second is monetary value
                    result = f"{result} ({numbers[0]}, com valor de {numbers[1]})"
                else:
                    result = f"{result} ({numbers[0]})"
            else:
                result = f"{result} ({', '.join(numbers[:2])})"

        return result

    def _get_connector_for_category(self, category: str, position: int) -> str:
        """
        Get appropriate connector based on insight category.

        Args:
            category: Insight category
            position: Position in narrative (0-indexed)

        Returns:
            Connector string
        """
        connectors_map = {
            "gap_competitivo": [
                "A liderança mantém",
                "O líder detém",
                "Existe uma vantagem de",
            ],
            "diversidade": ["Por outro lado", "Em contraste", "Simultaneamente"],
            "tendência": ["Adicionalmente", "Neste contexto", "Observa-se que"],
            "geral": ["Além disso", "Também se identifica que", "Adicionalmente"],
        }

        options = connectors_map.get(category, connectors_map["geral"])
        return options[min(position % len(options), len(options) - 1)]

    def _get_contrastive_connector(self, category: str) -> str:
        """
        Get contrastive connector for final sentence.

        Args:
            category: Insight category

        Returns:
            Connector string
        """
        if category in ["diversidade", "tendência"]:
            return "A cauda"
        return "Por fim"

    def _format_insights_for_prompt(self, insights: List[Dict[str, Any]]) -> str:
        """
        Format insights for LLM prompt.

        Args:
            insights: List of insights

        Returns:
            Formatted multi-line string
        """
        lines = []
        for i, ins in enumerate(insights, 1):
            title = ins.get("title", "")
            interpretation = ins.get("interpretation", "")
            formula = ins.get("formula", "")
            confidence = ins.get("confidence", 0.0)

            lines.append(f"INSIGHT {i}:")
            lines.append(f"  Título: {title}")
            lines.append(f"  Interpretação: {interpretation}")
            if formula:
                lines.append(f"  Fórmula: {formula}")
            lines.append(f"  Confiança: {confidence:.2f}")
            lines.append("")

        return "\n".join(lines)
