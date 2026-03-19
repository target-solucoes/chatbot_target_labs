"""
Text normalization utilities for robust string matching.

This module provides functions for normalizing text to enable
consistent matching between user queries and alias configurations.
"""

import re
import unicodedata
from typing import Optional


def normalize_text(text: str) -> str:
    """
    Normalize text for robust matching.

    Applies the following transformations:
    1. Remove accents/diacritics
    2. Convert to lowercase
    3. Remove special characters (keep only alphanumeric and spaces)
    4. Handle common Portuguese plurals
    5. Normalize whitespace

    Args:
        text: Input text to normalize

    Returns:
        Normalized text string

    Examples:
        >>> normalize_text("Região")
        'regiao'
        >>> normalize_text("VENDAS")
        'venda'
        >>> normalize_text("Código do Produto")
        'codigo do produto'
    """
    if not text:
        return ""

    # Step 1: Remove accents/diacritics
    text = remove_accents(text)

    # Step 2: Convert to lowercase
    text = text.lower()

    # Step 3: Replace special characters with spaces (keep alphanumeric and spaces)
    # This preserves word boundaries (e.g., "Código-do-Produto" -> "codigo do produto")
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    # Step 4: Handle Portuguese plurals
    text = handle_portuguese_plurals(text)

    # Step 5: Normalize whitespace
    text = " ".join(text.split())

    return text


def remove_accents(text: str) -> str:
    """
    Remove accents and diacritical marks from text.

    Uses Unicode normalization (NFKD) to decompose characters
    and then removes combining characters.

    Args:
        text: Input text with potential accents

    Returns:
        Text without accents

    Examples:
        >>> remove_accents("São Paulo")
        'Sao Paulo'
        >>> remove_accents("Açúcar")
        'Acucar'
    """
    if not text:
        return ""

    # Normalize to NFKD (Compatibility Decomposition)
    nfkd_form = unicodedata.normalize("NFKD", text)

    # Filter out combining characters (accents)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def handle_portuguese_plurals(text: str) -> str:
    """
    Convert common Portuguese plural forms to singular.

    Handles the following patterns:
    - Words ending in 'ões', 'oes', 'aos', 'ães' -> 'ao'
    - Words ending in 's' -> remove 's'

    Args:
        text: Text potentially containing plural words

    Returns:
        Text with plurals converted to singular

    Examples:
        >>> handle_portuguese_plurals("regioes")
        'regiao'
        >>> handle_portuguese_plurals("vendas")
        'venda'
        >>> handle_portuguese_plurals("produtos")
        'produto'
    """
    if not text:
        return ""

    # Split into words
    words = text.split()
    normalized_words = []

    for word in words:
        # Handle -ões, -oes, -aos, -ães -> -ao
        word = re.sub(r"(oes|aos|aes)$", "ao", word)

        # Handle regular -s plural (but preserve words naturally ending in 's')
        # Common exceptions: mais, mas, pois, após, etc.
        exceptions = ["mas", "mais", "pois", "apos", "tras", "atraves"]
        if word not in exceptions and len(word) > 2:
            word = re.sub(r"s$", "", word)

        normalized_words.append(word)

    return " ".join(normalized_words)


def fuzzy_normalize(text: str) -> str:
    """
    Aggressive normalization for fuzzy matching.

    This is a more aggressive version of normalize_text that also:
    - Removes articles and prepositions
    - Removes very common words

    Args:
        text: Input text

    Returns:
        Aggressively normalized text

    Examples:
        >>> fuzzy_normalize("O valor da venda")
        'valor venda'
        >>> fuzzy_normalize("Código do produto")
        'codigo produto'
    """
    # First apply standard normalization
    text = normalize_text(text)

    # Remove common Portuguese articles and prepositions
    stopwords = [
        "o",
        "a",
        "os",
        "as",  # articles
        "de",
        "da",
        "do",
        "das",
        "dos",  # prepositions
        "em",
        "no",
        "na",
        "nos",
        "nas",
        "por",
        "para",
        "com",
        "sem",
        "e",
        "ou",  # conjunctions
    ]

    words = text.split()
    filtered_words = [w for w in words if w not in stopwords and len(w) > 1]

    return " ".join(filtered_words)


def extract_numbers(text: str) -> list[int]:
    """
    Extract all numeric values from text.

    Args:
        text: Input text containing numbers

    Returns:
        List of extracted integers

    Examples:
        >>> extract_numbers("top 5 produtos em 2015")
        [5, 2015]
        >>> extract_numbers("compare vendas entre 100 e 200")
        [100, 200]
    """
    # Find all sequences of digits
    numbers = re.findall(r"\b\d+\b", text)
    return [int(n) for n in numbers]


def extract_quoted_terms(text: str) -> list[str]:
    """
    Extract terms enclosed in quotes.

    Args:
        text: Input text potentially containing quoted terms

    Returns:
        List of quoted terms

    Examples:
        >>> extract_quoted_terms('Buscar "São Paulo" e "Rio de Janeiro"')
        ['São Paulo', 'Rio de Janeiro']
    """
    # Match content within single or double quotes
    pattern = r'["\']([^"\']+)["\']'
    return re.findall(pattern, text)


def clean_column_name(column: str) -> str:
    """
    Clean a column name for consistent formatting.

    Args:
        column: Column name to clean

    Returns:
        Cleaned column name

    Examples:
        >>> clean_column_name("  Valor_Vendido  ")
        'Valor_Vendido'
        >>> clean_column_name("Des_Linha_Produto")
        'Des_Linha_Produto'
    """
    if not column:
        return ""

    # Strip whitespace
    column = column.strip()

    # If it's already in the standard format (Title_Case_With_Underscores), return as-is
    if "_" in column:
        # Ensure each part is title-cased
        parts = column.split("_")
        return "_".join(part.capitalize() for part in parts)

    return column


def similarity_key(text: str) -> str:
    """
    Generate a similarity key for approximate matching.

    This creates a normalized representation optimized for
    similarity comparisons using fuzzy matching algorithms.

    Args:
        text: Input text

    Returns:
        Similarity key

    Examples:
        >>> similarity_key("Região de Vendas")
        'regiaovenda'
    """
    # Apply aggressive normalization
    normalized = fuzzy_normalize(text)

    # Remove all spaces for character-level comparison
    return normalized.replace(" ", "")
