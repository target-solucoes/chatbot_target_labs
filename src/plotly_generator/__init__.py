"""
Plotly Generator Agent

Agente responsavel por gerar graficos interativos Plotly a partir dos outputs
dos agentes graphical_classifier e analytics_executor.

Modulos:
    - core: Configuracoes do agente
    - adapters: Processamento de inputs
    - generators: Geradores de graficos (um por tipo)
    - utils: Utilitarios reutilizaveis (styling, saving, etc.)
"""

from pathlib import Path

__version__ = "1.0.0"
__all__ = ["__version__"]

# Diretorio raiz do modulo
MODULE_ROOT = Path(__file__).parent
