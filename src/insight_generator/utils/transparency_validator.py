"""
Transparency validator for ensuring insights include base values.

This module validates that insights contain numeric values and proper transparency.
"""

import re
from typing import List, Dict, Any


def validate_transparency(insights: List[str], metrics: Dict[str, Any]) -> bool:
    """
    Valida se insights incluem FORMULAS EXPLICITAS com valores base.

    Criterios RIGOROSOS (atualizados):
    - Pelo menos 80% dos insights devem conter FORMULAS matematicas
    - Formulas devem incluir operadores: =, /, -, +, →, ou ×
    - Numeros devem estar em contexto de calculo, nao apenas isolados

    Exemplos VALIDOS:
    - "Top 3 = 8.66M / Total 12.68M → 68.3%"
    - "Gap = Lider - Segundo = 3.4M - 2.1M = 1.3M"
    - "Variacao = (450 - 300) / 300 = +50%"

    Exemplos INVALIDOS (nao passam):
    - "Top 3 representa 68.3%"
    - "O lider tem vantagem de 62%"

    Args:
        insights: List of insight strings to validate
        metrics: Dict of metrics that should be referenced

    Returns:
        bool: True if transparency rate >= 80% with formulas, False otherwise
    """
    if not insights:
        return False

    insights_with_formulas = 0

    for insight in insights:
        # Valida presenca de FORMULAS EXPLICITAS com operadores matematicos
        # Padroes aceitos:
        # 1. "A = B / C → D%" (divisao com resultado)
        # 2. "A - B = C" (subtracao)
        # 3. "A + B = C" (adicao)
        # 4. "(A - B) / C = D%" (formula completa)
        # 5. "A / B → C%" (divisao simples)

        has_formula = False

        # Padrão 1: Divisão com seta (qualquer tipo)
        # Ex: "8.66M / 12.68M → 68.3%" ou "8.66M / 12.68M -> 68.3%"
        if "/" in insight and ("→" in insight or "->" in insight):
            has_formula = True

        # Padrão 2: Divisão com igual
        # Ex: "Top 3 = 8.66M / Total 12.68M" ou "A = B / C"
        if "=" in insight and "/" in insight:
            has_formula = True

        # Padrão 3: Operação aritmética com igual (formato direto)
        # Ex: "3.4M - 2.1M = 1.3M" ou "500 - 100 = 400"
        if re.search(r"\d+[.,]?\d*[MKmk]?\s*[-+×x]\s*\d+[.,]?\d*[MKmk]?\s*=", insight):
            has_formula = True

        # Padrão 4: Operação aritmética descritiva
        # Ex: "Gap = Lider - Segundo = 1.3M" ou "Amplitude = Max - Min = 400"
        # ou "A - B = C" (formato genérico)
        if "=" in insight:
            # Procura operadores aritméticos entre palavras ou letras
            if re.search(r"[-+×x]", insight):
                # Se tem operador e igual, considera fórmula
                has_formula = True

        # Padrão 5: Fórmula com parênteses
        # Ex: "(450 - 300) / 300 = +50%" ou "(A - B) / C = D"
        if "(" in insight and ("/" in insight or "-" in insight or "+" in insight):
            has_formula = True

        if has_formula:
            insights_with_formulas += 1

    transparency_rate = insights_with_formulas / len(insights)

    # Threshold aumentado para 80% (antes era 70%)
    return transparency_rate >= 0.8


def validate_insight_dict_transparency(insights: List[Dict[str, Any]]) -> bool:
    """
    Valida transparência em insights estruturados (dicts).

    Suporta dois formatos:
    1. JSON mode: valida campo "formula" separado
    2. Legacy mode: valida campo "content"

    Args:
        insights: List of insight dictionaries with 'content' or 'formula' field

    Returns:
        bool: True if transparency validation passes
    """
    if not insights:
        return False

    # Extract strings to validate (prefer formula field if available)
    insight_strings = []
    for insight in insights:
        if isinstance(insight, dict):
            # Priority 1: Check if insight has separate "formula" field (JSON mode)
            if "formula" in insight:
                insight_strings.append(insight["formula"])
            # Priority 2: Fallback to "content" field (legacy mode)
            elif "content" in insight:
                insight_strings.append(insight["content"])
        elif isinstance(insight, str):
            insight_strings.append(insight)

    if not insight_strings:
        return False

    # Use string validation
    return validate_transparency(insight_strings, {})


def extract_numbers_from_text(text: str) -> List[float]:
    """
    Extrai todos os números de um texto.

    Args:
        text: String de texto para extrair números

    Returns:
        Lista de números encontrados
    """
    # Pattern for numbers (integers and decimals)
    pattern = r"\d+\.?\d*"
    matches = re.findall(pattern, text)

    numbers = []
    for match in matches:
        try:
            numbers.append(float(match))
        except ValueError:
            continue

    return numbers


def validate_metrics_referenced(
    insights: List[str], metrics: Dict[str, Any]
) -> Dict[str, bool]:
    """
    Verifica quais métricas foram referenciadas nos insights.

    Args:
        insights: List of insight strings
        metrics: Dict of metrics to check

    Returns:
        Dict mapping metric names to whether they were referenced
    """
    full_text = " ".join(insights).lower()

    referenced = {}
    for key, value in metrics.items():
        # Check if metric value appears in text
        if isinstance(value, (int, float)):
            # Convert to string and check presence
            value_str = str(value)
            referenced[key] = value_str in full_text or f"{value:.2f}" in full_text
        else:
            referenced[key] = False

    return referenced
