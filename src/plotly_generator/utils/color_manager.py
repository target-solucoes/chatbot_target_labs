"""
ColorManager - Gerenciador de cores para graficos Plotly.

Fornece funcionalidades para:
- Obter paletas de cores
- Gerar sequencias de cores
- Aplicar cores a traces
- Gerenciar cores para agrupamentos
"""

from typing import List, Dict, Any
import plotly.express as px
import plotly.graph_objects as go

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class ColorManager:
    """
    Gerenciador de cores para graficos Plotly.

    Centraliza a logica de:
    - Selecao de paletas
    - Geracao de sequencias de cores
    - Aplicacao de cores a traces
    - Mapeamento categoria -> cor

    Exemplo:
        >>> manager = ColorManager()
        >>> colors = manager.get_palette_colors("Blues", 5)
        >>> trace_colors = manager.get_color_sequence(["A", "B", "C"], "Set1")
    """

    # Paletas Plotly suportadas
    PALETTES: Dict[str, List[str]] = {
        # Paletas sequenciais
        "Blues": px.colors.sequential.Blues,
        "Reds": px.colors.sequential.Reds,
        "Greens": px.colors.sequential.Greens,
        "Oranges": px.colors.sequential.Oranges,
        "Purples": px.colors.sequential.Purples,
        "Greys": px.colors.sequential.Greys,
        "Viridis": px.colors.sequential.Viridis,
        "Plasma": px.colors.sequential.Plasma,
        "Inferno": px.colors.sequential.Inferno,
        "Magma": px.colors.sequential.Magma,
        "Cividis": px.colors.sequential.Cividis,
        "Turbo": px.colors.sequential.Turbo,
        # Paletas qualitativas
        "Set1": px.colors.qualitative.Set1,
        "Set2": px.colors.qualitative.Set2,
        "Set3": px.colors.qualitative.Set3,
        "Pastel": px.colors.qualitative.Pastel,
        "Pastel1": px.colors.qualitative.Pastel1,
        "Pastel2": px.colors.qualitative.Pastel2,
        "Dark2": px.colors.qualitative.Dark2,
        "Vivid": px.colors.qualitative.Vivid,
        "Bold": px.colors.qualitative.Bold,
        "Safe": px.colors.qualitative.Safe,
    }

    # Paletas sequenciais (cores claras -> escuras)
    SEQUENTIAL_PALETTES = {
        "Blues", "Reds", "Greens", "Oranges", "Purples", "Greys",
        "Viridis", "Plasma", "Inferno", "Magma", "Cividis", "Turbo"
    }

    # Paleta padrao para diferentes contextos
    DEFAULT_PALETTE = "Blues"
    DEFAULT_QUALITATIVE_PALETTE = "Set1"

    # Threshold de brilho para considerar cor muito clara (0-255)
    # Cores acima deste valor podem ser invisiveis em fundo branco
    BRIGHTNESS_THRESHOLD = 200  # ~78% de brilho

    def __init__(self):
        """Inicializa o ColorManager."""
        logger.debug("ColorManager inicializado")

    def _calculate_brightness(self, color: str) -> float:
        """
        Calcula o brilho (luminancia) de uma cor RGB.

        Usa a formula de luminancia relativa do W3C:
        L = 0.2126 * R + 0.7152 * G + 0.0722 * B

        Args:
            color: Cor em formato rgb(r,g,b) ou #RRGGBB

        Returns:
            Valor de brilho entre 0 (preto) e 255 (branco)

        Exemplo:
            >>> manager = ColorManager()
            >>> manager._calculate_brightness("rgb(255,255,255)")
            255.0
            >>> manager._calculate_brightness("rgb(0,0,0)")
            0.0
        """
        import re

        # Tentar extrair RGB de string rgb(r,g,b)
        rgb_match = re.search(r'rgb\((\d+),\s*(\d+),\s*(\d+)\)', color)
        if rgb_match:
            r, g, b = map(int, rgb_match.groups())
        else:
            # Tentar extrair de formato hex #RRGGBB
            hex_color = color.lstrip('#')
            if len(hex_color) == 6:
                try:
                    r = int(hex_color[0:2], 16)
                    g = int(hex_color[2:4], 16)
                    b = int(hex_color[4:6], 16)
                except ValueError:
                    logger.warning(f"Cor invalida '{color}', usando brilho medio")
                    return 127.5
            else:
                logger.warning(f"Formato de cor desconhecido '{color}', usando brilho medio")
                return 127.5

        # Calcular luminancia usando formula W3C
        brightness = 0.2126 * r + 0.7152 * g + 0.0722 * b
        return brightness

    def _is_color_too_light(self, color: str, threshold: float = None) -> bool:
        """
        Verifica se uma cor e muito clara para ser visivel em fundo branco.

        Args:
            color: Cor em formato rgb() ou hex
            threshold: Limite de brilho (usa BRIGHTNESS_THRESHOLD se None)

        Returns:
            True se cor e muito clara (baixa visibilidade)

        Exemplo:
            >>> manager = ColorManager()
            >>> manager._is_color_too_light("rgb(250,250,250)")
            True
            >>> manager._is_color_too_light("rgb(50,100,50)")
            False
        """
        if threshold is None:
            threshold = self.BRIGHTNESS_THRESHOLD

        brightness = self._calculate_brightness(color)
        return brightness > threshold

    def get_palette_colors(
        self, palette: str = DEFAULT_PALETTE, n_colors: int = 10
    ) -> List[str]:
        """
        Retorna lista de cores da paleta especificada.

        Se a paleta nao existir, usa paleta padrao.
        Se n_colors > tamanho da paleta, repete as cores.

        Args:
            palette: Nome da paleta
            n_colors: Numero de cores desejadas

        Returns:
            Lista de cores em formato hex/rgb

        Exemplo:
            >>> manager = ColorManager()
            >>> colors = manager.get_palette_colors("Blues", 3)
            >>> len(colors)
            3
        """
        if palette not in self.PALETTES:
            logger.warning(
                f"Paleta '{palette}' desconhecida, usando '{self.DEFAULT_PALETTE}'"
            )
            palette = self.DEFAULT_PALETTE

        colors = self.PALETTES[palette]

        # Se precisar mais cores que a paleta tem, repetir
        if n_colors > len(colors):
            multiplier = (n_colors // len(colors)) + 1
            colors = colors * multiplier

        result = colors[:n_colors]
        logger.debug(f"Retornando {len(result)} cores da paleta '{palette}'")
        return result

    def get_visible_palette_colors(
        self,
        palette: str = DEFAULT_PALETTE,
        n_colors: int = 10,
        skip_light_colors: bool = True
    ) -> List[str]:
        """
        Retorna cores da paleta, evitando cores muito claras automaticamente.

        Para paletas sequenciais, pula as cores muito claras no inicio.
        Para paletas qualitativas, filtra cores claras ou retorna todas se necessario.

        Args:
            palette: Nome da paleta
            n_colors: Numero de cores desejadas
            skip_light_colors: Se True, evita cores muito claras

        Returns:
            Lista de cores com boa visibilidade

        Exemplo:
            >>> manager = ColorManager()
            >>> colors = manager.get_visible_palette_colors("Greens", 5)
            >>> # Retorna cores escuras/medias, pulando as muito claras
        """
        if not skip_light_colors:
            # Comportamento padrao sem filtragem
            return self.get_palette_colors(palette, n_colors)

        # Obter paleta completa
        all_colors = self.get_palette_colors(palette, 100)  # Pegar muitas cores

        # Verificar se e paleta sequencial
        is_sequential = palette in self.SEQUENTIAL_PALETTES

        if is_sequential:
            # Para sequenciais: pular cores muito claras do inicio
            # Sequenciais vao de claro (inicio) para escuro (fim)
            visible_colors = []
            for color in all_colors:
                if not self._is_color_too_light(color):
                    visible_colors.append(color)

            if len(visible_colors) < n_colors:
                # Se nao tiver cores escuras suficientes, usar as mais escuras disponiveis
                # Pegar do final da paleta (cores mais escuras)
                start_idx = max(0, len(all_colors) - n_colors)
                result = all_colors[start_idx:start_idx + n_colors]
                logger.debug(
                    f"Paleta sequencial '{palette}': usando cores do indice "
                    f"{start_idx} ao {start_idx + n_colors - 1} (mais escuras)"
                )
            else:
                # Pegar n_colors das cores visiveis
                result = visible_colors[:n_colors]
                logger.debug(
                    f"Paleta sequencial '{palette}': {len(result)} cores visiveis selecionadas"
                )
        else:
            # Para qualitativas: filtrar cores claras, mas aceitar se nao tiver alternativa
            visible_colors = [c for c in all_colors if not self._is_color_too_light(c)]

            if len(visible_colors) >= n_colors:
                result = visible_colors[:n_colors]
                logger.debug(
                    f"Paleta qualitativa '{palette}': {len(result)} cores visiveis selecionadas"
                )
            else:
                # Se nao tiver cores visiveis suficientes, usar todas disponiveis
                result = all_colors[:n_colors]
                logger.warning(
                    f"Paleta qualitativa '{palette}': cores visiveis insuficientes, "
                    f"usando {len(result)} cores (algumas podem ser claras)"
                )

        return result

    def get_color_sequence(
        self,
        categories: List[str],
        palette: str = DEFAULT_QUALITATIVE_PALETTE,
        skip_light_colors: bool = True
    ) -> Dict[str, str]:
        """
        Cria mapeamento de categorias para cores VISIVEIS.

        Automaticamente evita cores muito claras para melhor visualizacao.

        Args:
            categories: Lista de categorias unicas
            palette: Nome da paleta a usar
            skip_light_colors: Se True, evita cores claras (recomendado)

        Returns:
            Dicionario {categoria: cor}

        Exemplo:
            >>> manager = ColorManager()
            >>> mapping = manager.get_color_sequence(["A", "B", "C"], "Set1")
            >>> "A" in mapping
            True
        """
        n_colors = len(categories)

        # Usar metodo que filtra cores claras automaticamente
        colors = self.get_visible_palette_colors(palette, n_colors, skip_light_colors)

        color_mapping = {category: colors[i] for i, category in enumerate(categories)}

        logger.debug(
            f"Criado mapeamento de {len(categories)} categorias para cores visiveis "
            f"(skip_light_colors={skip_light_colors})"
        )
        return color_mapping

    def apply_color_to_trace(
        self, trace: go.Scatter | go.Bar | go.Pie, color: str, opacity: float = 1.0
    ) -> None:
        """
        Aplica cor a uma trace Plotly.

        Modifica a trace in-place adicionando configuracao de cor.

        Args:
            trace: Trace Plotly (Scatter, Bar, Pie, etc.)
            color: Cor em formato hex/rgb
            opacity: Opacidade (0.0 a 1.0)

        Exemplo:
            >>> trace = go.Bar(x=[1, 2], y=[3, 4])
            >>> manager.apply_color_to_trace(trace, "#FF5733", 0.8)
        """
        if isinstance(trace, (go.Bar, go.Scatter)):
            trace.marker = dict(color=color, opacity=opacity)
        elif isinstance(trace, go.Pie):
            # Para pie charts, aplicar apenas cor (nao suporta opacity no marker)
            trace.marker = dict(colors=[color])

        logger.debug(f"Cor '{color}' aplicada a trace do tipo {type(trace).__name__}")

    def get_gradient_colors(
        self, values: List[float], palette: str = "Blues", reverse: bool = False
    ) -> List[str]:
        """
        Gera cores em gradiente baseado em valores.

        Util para mapas de calor ou graficos onde a cor representa intensidade.

        Args:
            values: Lista de valores numericos
            palette: Nome da paleta
            reverse: Se True, inverte a ordem das cores

        Returns:
            Lista de cores correspondentes aos valores

        Exemplo:
            >>> manager = ColorManager()
            >>> values = [10, 50, 100]
            >>> colors = manager.get_gradient_colors(values, "Reds")
            >>> len(colors) == len(values)
            True
        """
        if not values:
            return []

        min_val = min(values)
        max_val = max(values)
        value_range = max_val - min_val if max_val != min_val else 1

        # Obter paleta de cores
        palette_colors = self.get_palette_colors(palette, 100)
        if reverse:
            palette_colors = palette_colors[::-1]

        # Mapear valores para indices de cores
        colors = []
        for value in values:
            # Normalizar valor para 0-99
            normalized = int(((value - min_val) / value_range) * 99)
            normalized = max(0, min(99, normalized))  # Clamp
            colors.append(palette_colors[normalized])

        logger.debug(
            f"Gerado gradiente de {len(colors)} cores para valores "
            f"no range [{min_val}, {max_val}]"
        )
        return colors

    def create_color_scale(self, palette: str = "Viridis") -> List[List[Any]]:
        """
        Cria escala de cores para graficos de contorno/superficie.

        Args:
            palette: Nome da paleta

        Returns:
            Lista de pares [posicao, cor] para Plotly colorscale

        Exemplo:
            >>> manager = ColorManager()
            >>> scale = manager.create_color_scale("Viridis")
            >>> isinstance(scale, list)
            True
        """
        colors = self.get_palette_colors(palette, 10)

        # Criar escala normalizada [0.0, 0.1, 0.2, ..., 1.0]
        positions = [i / (len(colors) - 1) for i in range(len(colors))]

        colorscale = [[pos, color] for pos, color in zip(positions, colors)]

        logger.debug(
            f"Criada escala de cores com {len(colors)} pontos para '{palette}'"
        )
        return colorscale

    def get_contrasting_text_color(self, background_color: str) -> str:
        """
        Determina cor de texto (preto ou branco) baseado na cor de fundo.

        Usa formula de luminancia relativa para decidir.

        Args:
            background_color: Cor de fundo em formato hex (ex: "#FF5733")

        Returns:
            "#000000" (preto) ou "#FFFFFF" (branco)

        Exemplo:
            >>> manager = ColorManager()
            >>> manager.get_contrasting_text_color("#FF5733")
            '#FFFFFF'
        """
        # Remover '#' se presente
        hex_color = background_color.lstrip("#")

        # Converter para RGB
        try:
            r = int(hex_color[0:2], 16) / 255
            g = int(hex_color[2:4], 16) / 255
            b = int(hex_color[4:6], 16) / 255
        except (ValueError, IndexError):
            logger.warning(
                f"Cor invalida '{background_color}', usando branco para texto"
            )
            return "#FFFFFF"

        # Calcular luminancia relativa
        luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b

        # Se luminancia > 0.5, usar preto; caso contrario, branco
        text_color = "#000000" if luminance > 0.5 else "#FFFFFF"

        logger.debug(
            f"Cor de texto '{text_color}' escolhida para fundo '{background_color}'"
        )
        return text_color
