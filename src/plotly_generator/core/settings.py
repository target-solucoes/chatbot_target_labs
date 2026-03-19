"""
Settings e configuracoes do Plotly Generator Agent.

Define constantes, diretorios e configuracoes padrao para geracao de graficos.
"""

import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv

load_dotenv()

# Raiz do projeto
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Diretorio de saida para graficos gerados
OUTPUT_DIR = os.getenv(
    "PLOTLY_OUTPUT_DIR",
    str(PROJECT_ROOT / "src" / "plotly_generator" / "generated_plots")
)

# Paleta de cores padrao
DEFAULT_PALETTE = os.getenv("PLOTLY_DEFAULT_PALETTE", "Blues")

# Tipos de grafico suportados
SUPPORTED_CHART_TYPES: List[str] = [
    "bar_horizontal",
    "bar_vertical",
    "bar_vertical_composed",
    "bar_vertical_stacked",
    "line",
    "line_composed",
    "pie",
    "histogram"
]

# Configuracoes de salvamento
SAVE_HTML_DEFAULT = os.getenv("PLOTLY_SAVE_HTML", "true").lower() == "true"
SAVE_PNG_DEFAULT = os.getenv("PLOTLY_SAVE_PNG", "false").lower() == "true"

# Configuracoes de Plotly.js
PLOTLY_JS_MODE = os.getenv("PLOTLY_JS_MODE", "cdn")  # "cdn", "inline", ou "directory"

# Dimensoes padrao para PNG
PNG_WIDTH = int(os.getenv("PLOTLY_PNG_WIDTH", "1200"))
PNG_HEIGHT = int(os.getenv("PLOTLY_PNG_HEIGHT", "800"))

# Configuracoes de estilo
FONT_FAMILY = os.getenv("PLOTLY_FONT_FAMILY", "Arial, sans-serif")
FONT_SIZE = int(os.getenv("PLOTLY_FONT_SIZE", "12"))

# Margens padrao
MARGIN_LEFT = int(os.getenv("PLOTLY_MARGIN_LEFT", "80"))
MARGIN_RIGHT = int(os.getenv("PLOTLY_MARGIN_RIGHT", "40"))
MARGIN_TOP = int(os.getenv("PLOTLY_MARGIN_TOP", "80"))
MARGIN_BOTTOM = int(os.getenv("PLOTLY_MARGIN_BOTTOM", "60"))

# Cores de fundo
PLOT_BGCOLOR = os.getenv("PLOTLY_PLOT_BGCOLOR", "white")
PAPER_BGCOLOR = os.getenv("PLOTLY_PAPER_BGCOLOR", "white")


def validate_settings() -> bool:
    """
    Valida todas as configuracoes do modulo.

    Verificacoes:
    - OUTPUT_DIR existe ou pode ser criado
    - DEFAULT_PALETTE e uma string valida
    - Dimensoes PNG sao positivas
    - Margens sao positivas

    Returns:
        True se todas as validacoes passarem

    Raises:
        ValueError: Se alguma configuracao for invalida
        FileNotFoundError: Se OUTPUT_DIR nao puder ser criado
    """
    # Validar OUTPUT_DIR
    output_path = Path(OUTPUT_DIR)
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise FileNotFoundError(
            f"Nao foi possivel criar diretorio de saida: {OUTPUT_DIR}"
        ) from e

    # Validar DEFAULT_PALETTE
    if not DEFAULT_PALETTE or not isinstance(DEFAULT_PALETTE, str):
        raise ValueError(
            f"DEFAULT_PALETTE deve ser uma string nao vazia: {DEFAULT_PALETTE}"
        )

    # Validar dimensoes PNG
    if PNG_WIDTH <= 0 or PNG_HEIGHT <= 0:
        raise ValueError(
            f"Dimensoes PNG devem ser positivas: {PNG_WIDTH}x{PNG_HEIGHT}"
        )

    # Validar margens
    margins = [MARGIN_LEFT, MARGIN_RIGHT, MARGIN_TOP, MARGIN_BOTTOM]
    if any(m < 0 for m in margins):
        raise ValueError(
            f"Margens devem ser positivas: L={MARGIN_LEFT}, R={MARGIN_RIGHT}, "
            f"T={MARGIN_TOP}, B={MARGIN_BOTTOM}"
        )

    # Validar font size
    if FONT_SIZE <= 0:
        raise ValueError(f"FONT_SIZE deve ser positivo: {FONT_SIZE}")

    return True


def get_default_layout_config() -> dict:
    """
    Retorna configuracao de layout padrao para graficos Plotly.

    Returns:
        Dicionario com configuracoes de layout
    """
    return {
        "font": {
            "family": FONT_FAMILY,
            "size": FONT_SIZE
        },
        "margin": {
            "l": MARGIN_LEFT,
            "r": MARGIN_RIGHT,
            "t": MARGIN_TOP,
            "b": MARGIN_BOTTOM
        },
        "plot_bgcolor": PLOT_BGCOLOR,
        "paper_bgcolor": PAPER_BGCOLOR
    }
