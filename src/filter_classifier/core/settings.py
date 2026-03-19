"""
Environment settings and configuration for filter_classifier.

This module loads environment variables and defines filter-specific constants.
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Filter Persistence Configuration
STORAGE_PATH: str = os.getenv(
    "FILTER_STORAGE_PATH",
    ".filter_state.json"
)

SESSION_TIMEOUT_MINUTES: int = int(os.getenv("FILTER_SESSION_TIMEOUT", "30"))

# Validation Configuration
FUZZY_THRESHOLD: float = float(os.getenv("FILTER_FUZZY_THRESHOLD", "0.75"))
MIN_CONFIDENCE_THRESHOLD: float = float(os.getenv("FILTER_MIN_CONFIDENCE", "0.60"))

# Value Validation (NEW - for detecting non-existent values like "PRODUTOS")
ENABLE_VALUE_VALIDATION: bool = os.getenv("ENABLE_VALUE_VALIDATION", "true").lower() == "true"

# Generic Terms Blacklist (NEW - reject these even if fuzzy matching finds suggestions)
# These are common query terms that should NEVER be treated as filter values
GENERIC_TERMS_BLACKLIST = {
    # Product-related generic terms (SIMPLE)
    "PRODUTOS", "PRODUTO", "PRODUTORES", "PRODUTOR",
    "produtos", "produto", "produtores", "produtor",

    # Product-related generic terms (COMPOSITE - EXPANDED)
    # These are generic category names that should NOT be used as filters
    # They appear in the dataset but represent broad categories, not specific values
    "PRODUTOS REVENDA", "produtos revenda",  # Too generic for Des_Linha_Produto
    "PRODUTOS ACABADOS", "produtos acabados",
    "PRODUTOS GERAIS", "produtos gerais",
    "ITENS", "itens", "ITEM", "item",
    "LINHAS", "linhas", "LINHA", "linha",
    "CATEGORIAS", "categorias", "CATEGORIA", "categoria",
    "GRUPOS", "grupos", "GRUPO", "grupo",
    "FAMILIAS", "familias", "FAMILIA", "familia",

    # Client-related generic terms
    "CLIENTES", "CLIENTE", "CONSUMIDORES", "CONSUMIDOR",
    "clientes", "cliente", "consumidores", "consumidor",
    "COMPRADORES", "compradores", "COMPRADOR", "comprador",

    # Sales-related generic terms
    "VENDAS", "VENDA", "VENDEDORES", "VENDEDOR",
    "vendas", "venda", "vendedores", "vendedor",
    "PEDIDOS", "pedidos", "PEDIDO", "pedido",
    "TRANSACOES", "transacoes", "TRANSACAO", "transacao",

    # Location-related generic terms (less common but possible)
    "CIDADES", "CIDADE", "ESTADOS", "ESTADO", "REGIOES", "REGIAO",
    "cidades", "cidade", "estados", "estado", "regioes", "regiao",
    "LOCALIDADES", "localidades", "LOCALIDADE", "localidade",
    "MUNICIPIOS", "municipios", "MUNICIPIO", "municipio",

    # Aggregation/Ranking terms that might be confused as values
    "MAIORES", "maiores", "MAIOR", "maior",
    "MENORES", "menores", "MENOR", "menor",
    "MELHORES", "melhores", "MELHOR", "melhor",
    "PIORES", "piores", "PIOR", "pior",
    "PRINCIPAIS", "principais", "PRINCIPAL", "principal",
    "TOP", "top",
    "TODOS", "todos", "TODO", "todo",
    "TODAS", "todas", "TODA", "toda",
    "GERAL", "geral", "GERAIS", "gerais",
}

# Dataset Sampling (for categorical value validation)
DATASET_SAMPLE_SIZE: Optional[int] = int(os.getenv("FILTER_DATASET_SAMPLE_SIZE", "1000"))

# LLM Configuration (inherited from main settings, but can be overridden)
# Gemini Migration: Try GEMINI_API_KEY first, fall back to OPENAI_API_KEY
# NOTE: Uses gemini-2.5-flash-lite (centralized config), not gemini-2.5-flash
OPENAI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY")
OPENAI_MODEL: str = os.getenv("GEMINI_MODEL") or os.getenv("OPENAI_MODEL", "gemini-2.5-flash-lite")
TEMPERATURE: float = float(os.getenv("TEMPERATURE", "0.7"))
MAX_TOKENS: int = int(os.getenv("MAX_TOKENS", "2000"))

# Data Paths (reuse from shared config)
ALIAS_PATH: str = os.getenv(
    "ALIAS_PATH",
    str(PROJECT_ROOT / "data" / "mappings" / "alias.yaml")
)
DATASET_PATH: str = os.getenv("DATASET_PATH", "")

# Valid Filter Operators
VALID_OPERATORS = ["=", ">", "<", ">=", "<=", "between", "in", "not_in"]

# CRUD Operation Types
CRUD_OPERATIONS = ["ADICIONAR", "ALTERAR", "REMOVER", "MANTER"]

# Operation Precedence (for conflict resolution)
OPERATION_PRECEDENCE = ["REMOVER", "ALTERAR", "ADICIONAR", "MANTER"]


def validate_settings() -> bool:
    """
    Validate that all critical settings are properly configured.

    Returns:
        bool: True if all settings are valid

    Raises:
        ValueError: If settings are invalid
        FileNotFoundError: If required files are missing
    """
    if SESSION_TIMEOUT_MINUTES < 1:
        raise ValueError(f"SESSION_TIMEOUT_MINUTES must be positive, got: {SESSION_TIMEOUT_MINUTES}")

    if FUZZY_THRESHOLD < 0 or FUZZY_THRESHOLD > 1:
        raise ValueError(f"FUZZY_THRESHOLD must be between 0 and 1, got: {FUZZY_THRESHOLD}")

    if MIN_CONFIDENCE_THRESHOLD < 0 or MIN_CONFIDENCE_THRESHOLD > 1:
        raise ValueError(f"MIN_CONFIDENCE_THRESHOLD must be between 0 and 1, got: {MIN_CONFIDENCE_THRESHOLD}")

    if DATASET_SAMPLE_SIZE is not None and DATASET_SAMPLE_SIZE < 1:
        raise ValueError(f"DATASET_SAMPLE_SIZE must be positive or None, got: {DATASET_SAMPLE_SIZE}")

    if not Path(ALIAS_PATH).exists():
        raise FileNotFoundError(f"Alias file not found at: {ALIAS_PATH}")

    if TEMPERATURE < 0 or TEMPERATURE > 2:
        raise ValueError(f"TEMPERATURE must be between 0 and 2, got: {TEMPERATURE}")

    return True
