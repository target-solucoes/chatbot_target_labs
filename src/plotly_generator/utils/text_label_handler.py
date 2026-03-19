"""
TextLabelHandler - Utilitário para gerenciar rótulos de texto longos em gráficos Plotly.

Este módulo fornece funcionalidades automáticas para:
- Quebra de linha (text wrapping) em rótulos longos
- Rotação inteligente de rótulos
- Ajuste dinâmico de margens
- Configuração de tamanho de fonte adaptativo

O objetivo é garantir que todos os rótulos de texto sejam exibidos
integralmente nos gráficos, evitando truncamento.
"""

import textwrap
from typing import List, Dict, Any, Optional, Tuple
import plotly.graph_objects as go

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class TextLabelHandler:
    """
    Gerenciador de rótulos de texto longos para gráficos Plotly.

    Aplica configurações automáticas para garantir que textos categóricos
    longos sejam exibidos integralmente, sem cortes.

    Features:
    - Quebra de linha automática em textos longos
    - Ajuste dinâmico de margens baseado no tamanho dos textos
    - Rotação inteligente de rótulos quando apropriado
    - Configuração de tamanho de fonte adaptativo

    Exemplo de uso:
        >>> handler = TextLabelHandler()
        >>> labels = ["Nome de Produto Muito Longo", "Outro Produto", ...]
        >>> wrapped_labels = handler.wrap_labels(labels, max_width=20)
        >>> handler.apply_categorical_axis_config(fig, labels, axis='x')
    """

    # Configurações padrão
    DEFAULT_MAX_CHARS_PER_LINE = 20  # Caracteres por linha antes de quebrar
    DEFAULT_MIN_FONT_SIZE = 9  # Tamanho mínimo de fonte
    DEFAULT_ROTATION_THRESHOLD = 15  # Comprimento que aciona rotação
    BASE_MARGIN = 80  # Margem base para eixos
    CHAR_WIDTH_PIXELS = 7  # Largura média de caractere em pixels

    def __init__(
        self,
        max_chars_per_line: int = DEFAULT_MAX_CHARS_PER_LINE,
        min_font_size: int = DEFAULT_MIN_FONT_SIZE,
        rotation_threshold: int = DEFAULT_ROTATION_THRESHOLD,
    ):
        """
        Inicializa o handler com configurações personalizáveis.

        Args:
            max_chars_per_line: Número máximo de caracteres por linha antes de quebra
            min_font_size: Tamanho mínimo de fonte permitido
            rotation_threshold: Comprimento de texto que aciona rotação
        """
        self.max_chars_per_line = max_chars_per_line
        self.min_font_size = min_font_size
        self.rotation_threshold = rotation_threshold

        logger.debug(
            f"TextLabelHandler inicializado: max_chars={max_chars_per_line}, "
            f"min_font={min_font_size}, rotation_threshold={rotation_threshold}"
        )

    def wrap_labels(
        self, labels: List[str], max_width: Optional[int] = None
    ) -> List[str]:
        """
        Aplica quebra de linha em rótulos longos.

        Args:
            labels: Lista de rótulos originais
            max_width: Largura máxima por linha (None usa o padrão da classe)

        Returns:
            Lista de rótulos com quebras de linha (<br> para HTML)

        Exemplo:
            >>> handler = TextLabelHandler()
            >>> labels = ["Produto com Nome Extremamente Longo", "Curto"]
            >>> handler.wrap_labels(labels, max_width=15)
            ['Produto com<br>Nome<br>Extremamente<br>Longo', 'Curto']
        """
        if max_width is None:
            max_width = self.max_chars_per_line

        wrapped = []
        for label in labels:
            # Converter para string caso não seja
            label_str = str(label) if label is not None else ""

            if len(label_str) <= max_width:
                wrapped.append(label_str)
            else:
                # Quebrar texto usando textwrap
                lines = textwrap.wrap(label_str, width=max_width)
                # Juntar com <br> para HTML Plotly
                wrapped.append("<br>".join(lines))

        logger.debug(
            f"Aplicada quebra de linha em {len(labels)} rótulos (max_width={max_width})"
        )
        return wrapped

    def calculate_max_label_length(self, labels: List[str]) -> int:
        """
        Calcula o comprimento máximo entre todos os rótulos.

        Args:
            labels: Lista de rótulos

        Returns:
            Comprimento do rótulo mais longo
        """
        if not labels:
            return 0

        max_len = max(len(str(label)) for label in labels if label is not None)
        logger.debug(f"Comprimento máximo de rótulo: {max_len} caracteres")
        return max_len

    def should_rotate_labels(self, labels: List[str]) -> bool:
        """
        Determina se os rótulos devem ser rotacionados.

        Rotação é recomendada quando:
        - O rótulo mais longo excede o threshold
        - Há muitos rótulos (>10)

        Args:
            labels: Lista de rótulos

        Returns:
            True se rotação é recomendada
        """
        max_len = self.calculate_max_label_length(labels)
        many_labels = len(labels) > 10

        should_rotate = max_len > self.rotation_threshold or many_labels

        logger.debug(
            f"Rotação de rótulos: {'SIM' if should_rotate else 'NÃO'} "
            f"(max_len={max_len}, count={len(labels)})"
        )
        return should_rotate

    def calculate_optimal_margin(
        self, labels: List[str], axis: str = "x", with_wrapping: bool = True
    ) -> int:
        """
        Calcula margem ótima baseada no tamanho dos rótulos.

        Args:
            labels: Lista de rótulos
            axis: Eixo a calcular margem ('x' ou 'y')
            with_wrapping: Se True, considera quebras de linha

        Returns:
            Margem recomendada em pixels
        """
        if not labels:
            return self.BASE_MARGIN

        max_len = self.calculate_max_label_length(labels)

        if with_wrapping:
            # Com wrapping, rótulos ficam mais estreitos mas mais altos
            if axis == "x":
                # Margem inferior aumenta com quebras de linha
                num_lines = max(1, max_len // self.max_chars_per_line)
                margin = self.BASE_MARGIN + (num_lines * 15)
            else:  # axis == "y"
                # Margem lateral aumenta com comprimento (após wrapping)
                effective_width = min(max_len, self.max_chars_per_line)
                margin = self.BASE_MARGIN + (effective_width * 3)
        else:
            # Sem wrapping, margem proporcional ao comprimento
            if axis == "x":
                margin = self.BASE_MARGIN + (max_len * 2)
            else:  # axis == "y"
                margin = self.BASE_MARGIN + (max_len * self.CHAR_WIDTH_PIXELS)

        # Limitar margem máxima para evitar gráficos muito distorcidos
        max_margin = 300 if axis == "y" else 200
        margin = min(margin, max_margin)

        logger.debug(
            f"Margem calculada para eixo {axis}: {margin}px "
            f"(max_len={max_len}, wrapping={with_wrapping})"
        )
        return margin

    def apply_categorical_axis_config(
        self,
        fig: go.Figure,
        labels: List[str],
        axis: str = "x",
        force_wrapping: bool = False,
        force_rotation: bool = False,
        rotation_angle: int = -45,
    ) -> Dict[str, Any]:
        """
        Aplica configuração automática para eixos categóricos com textos longos.

        Esta é a função principal que deve ser chamada pelos generators.
        Aplica automaticamente:
        - Quebra de linha quando apropriado
        - Rotação quando apropriado
        - Ajuste de margens
        - Tamanho de fonte adaptativo

        Args:
            fig: Figure Plotly a ser configurada
            labels: Lista de rótulos do eixo
            axis: Qual eixo configurar ('x' ou 'y')
            force_wrapping: Força quebra de linha mesmo em textos curtos
            force_rotation: Força rotação mesmo em textos curtos
            rotation_angle: Ângulo de rotação (-90 para vertical, -45 para diagonal)

        Returns:
            Dicionário com configurações aplicadas (para referência/debug)

        Exemplo:
            >>> handler = TextLabelHandler()
            >>> fig = go.Figure(...)
            >>> labels = ["Produto A com nome longo", "Produto B", ...]
            >>> config = handler.apply_categorical_axis_config(fig, labels, axis='x')
            >>> print(config['wrapped_labels_applied'])  # True/False
        """
        if not labels:
            logger.warning("Lista de rótulos vazia, nenhuma configuração aplicada")
            return {"applied": False}

        max_len = self.calculate_max_label_length(labels)

        # Determinar se precisa wrapping
        needs_wrapping = force_wrapping or max_len > self.max_chars_per_line

        # Determinar se precisa rotação
        needs_rotation = force_rotation or (
            not needs_wrapping and self.should_rotate_labels(labels)
        )

        # Aplicar wrapping se necessário
        processed_labels = labels
        if needs_wrapping:
            processed_labels = self.wrap_labels(labels)

        # Calcular margem ótima
        optimal_margin = self.calculate_optimal_margin(
            labels, axis=axis, with_wrapping=needs_wrapping
        )

        # Configurar eixo
        if axis == "x":
            # Use the original category strings as tickvals so Plotly
            # can match them to the trace data, preserving data-driven order.
            fig.update_xaxes(
                ticktext=processed_labels,
                tickvals=[str(l) for l in labels],  # original string values
                tickangle=rotation_angle if needs_rotation else 0,
                tickfont={"size": self.min_font_size if needs_wrapping else 11},
                automargin=True,  # Permitir que Plotly ajuste automaticamente
            )

            # Ajustar margem inferior
            fig.update_layout(
                margin=dict(
                    l=fig.layout.margin.l if fig.layout.margin else 80,
                    r=fig.layout.margin.r if fig.layout.margin else 40,
                    t=fig.layout.margin.t if fig.layout.margin else 40,
                    b=optimal_margin,
                )
            )
        else:  # axis == "y"
            # Use the original category strings as tickvals so Plotly
            # can match them to the trace data, and wrapped text as ticktext.
            avg_len = sum(len(str(l)) for l in labels) / max(len(labels), 1)
            adaptive_font = self.get_adaptive_font_size(len(labels), avg_len)

            fig.update_yaxes(
                ticktext=processed_labels,
                tickvals=[str(l) for l in labels],  # original string values
                tickfont={"size": adaptive_font},
                automargin=True,
            )

            # Let automargin handle it; only set a sensible minimum left margin
            fig.update_layout(
                margin=dict(
                    l=max(optimal_margin, 80),
                    r=fig.layout.margin.r if fig.layout.margin else 40,
                    t=fig.layout.margin.t if fig.layout.margin else 40,
                    b=fig.layout.margin.b if fig.layout.margin else 60,
                )
            )

        config = {
            "applied": True,
            "axis": axis,
            "max_label_length": max_len,
            "wrapped_labels_applied": needs_wrapping,
            "rotation_applied": needs_rotation,
            "optimal_margin": optimal_margin,
            "processed_labels_count": len(processed_labels),
        }

        logger.info(
            f"Configuração de rótulos aplicada no eixo {axis}: "
            f"wrapping={needs_wrapping}, rotation={needs_rotation}, "
            f"margin={optimal_margin}px"
        )

        return config

    def get_adaptive_font_size(self, num_labels: int, avg_label_length: float) -> int:
        """
        Calcula tamanho de fonte adaptativo baseado na quantidade e tamanho dos rótulos.

        Args:
            num_labels: Número de rótulos
            avg_label_length: Comprimento médio dos rótulos

        Returns:
            Tamanho de fonte recomendado (em pontos)
        """
        # Começar com tamanho padrão
        font_size = 11

        # Reduzir se muitos rótulos
        if num_labels > 20:
            font_size = 9
        elif num_labels > 10:
            font_size = 10

        # Reduzir se rótulos muito longos
        if avg_label_length > 30:
            font_size = min(font_size, 9)
        elif avg_label_length > 20:
            font_size = min(font_size, 10)

        # Garantir mínimo
        font_size = max(font_size, self.min_font_size)

        logger.debug(
            f"Tamanho de fonte adaptativo: {font_size}pt "
            f"(num_labels={num_labels}, avg_len={avg_label_length:.1f})"
        )

        return font_size
