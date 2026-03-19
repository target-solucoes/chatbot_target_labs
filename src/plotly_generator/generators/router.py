"""
GeneratorRouter - Roteador que seleciona o generator apropriado baseado em chart_type.

Usa pattern Registry para permitir adicionar novos generators facilmente sem
modificar o codigo existente.
"""

from typing import Dict, Type

from src.plotly_generator.generators.base import BasePlotlyGenerator
from src.plotly_generator.utils.plot_styler import PlotStyler
from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class GeneratorRouter:
    """
    Roteador que seleciona o generator apropriado baseado em chart_type.

    Implementa o pattern Registry para facilitar a extensibilidade:
    - Adicionar novo generator: router.register("scatter", ScatterGenerator)
    - Obter generator: router.get_generator("scatter")

    Exemplo:
        >>> from src.plotly_generator.utils.plot_styler import PlotStyler
        >>> styler = PlotStyler()
        >>> router = GeneratorRouter(styler)
        >>> generator = router.get_generator("bar_horizontal")
        >>> type(generator).__name__
        'BarHorizontalGenerator'
    """

    def __init__(self, styler: PlotStyler):
        """
        Inicializa o router com um PlotStyler.

        Args:
            styler: Instancia de PlotStyler para passar aos generators
        """
        self.styler = styler
        self._registry: Dict[str, Type[BasePlotlyGenerator]] = {}
        self._register_default_generators()
        logger.info(
            f"GeneratorRouter inicializado com {len(self._registry)} generators"
        )

    def _register_default_generators(self) -> None:
        """
        Registra os generators padrao (Fase 2 + Fase 3).

        Fase 2 (Basicos):
        - bar_horizontal
        - bar_vertical
        - pie
        - line

        Fase 3 (Avancados):
        - bar_vertical_composed
        - bar_vertical_stacked
        - line_composed
        - histogram
        """
        # Import lazy (apenas quando router for criado)

        # Fase 2: Generators basicos
        try:
            from src.plotly_generator.generators.bar_horizontal_generator import (
                BarHorizontalGenerator,
            )

            self.register("bar_horizontal", BarHorizontalGenerator)
        except ImportError:
            logger.warning("BarHorizontalGenerator nao disponivel")

        try:
            from src.plotly_generator.generators.bar_vertical_generator import (
                BarVerticalGenerator,
            )

            self.register("bar_vertical", BarVerticalGenerator)
        except ImportError:
            logger.warning("BarVerticalGenerator nao disponivel")

        try:
            from src.plotly_generator.generators.pie_generator import PieGenerator

            self.register("pie", PieGenerator)
        except ImportError:
            logger.warning("PieGenerator nao disponivel")

        try:
            from src.plotly_generator.generators.line_generator import LineGenerator

            self.register("line", LineGenerator)
        except ImportError:
            logger.warning("LineGenerator nao disponivel")

        # Fase 3: Generators avancados
        try:
            from src.plotly_generator.generators.bar_vertical_composed_generator import (
                BarVerticalComposedGenerator,
            )

            self.register("bar_vertical_composed", BarVerticalComposedGenerator)
        except ImportError:
            logger.warning("BarVerticalComposedGenerator nao disponivel")

        try:
            from src.plotly_generator.generators.bar_vertical_stacked_generator import (
                BarVerticalStackedGenerator,
            )

            self.register("bar_vertical_stacked", BarVerticalStackedGenerator)
        except ImportError:
            logger.warning("BarVerticalStackedGenerator nao disponivel")

        try:
            from src.plotly_generator.generators.line_composed_generator import (
                LineComposedGenerator,
            )

            self.register("line_composed", LineComposedGenerator)
        except ImportError:
            logger.warning("LineComposedGenerator nao disponivel")

        try:
            from src.plotly_generator.generators.histogram_generator import (
                HistogramGenerator,
            )

            self.register("histogram", HistogramGenerator)
        except ImportError:
            logger.warning("HistogramGenerator nao disponivel")

        logger.debug(f"Generators registrados: {list(self._registry.keys())}")

    def register(
        self, chart_type: str, generator_class: Type[BasePlotlyGenerator]
    ) -> None:
        """
        Registra um novo generator no router.

        Permite adicionar generators dinamicamente sem modificar o router.

        Args:
            chart_type: Tipo de grafico (ex: "scatter", "box")
            generator_class: Classe do generator (deve herdar de BasePlotlyGenerator)

        Example:
            >>> class ScatterGenerator(BasePlotlyGenerator):
            ...     def validate(self, chart_spec, data): pass
            ...     def generate(self, chart_spec, data): pass
            >>> router.register("scatter", ScatterGenerator)
            >>> "scatter" in router.get_supported_chart_types()
            True
        """
        if not issubclass(generator_class, BasePlotlyGenerator):
            raise TypeError(
                f"{generator_class.__name__} deve herdar de BasePlotlyGenerator"
            )

        self._registry[chart_type] = generator_class
        logger.debug(
            f"Generator registrado: '{chart_type}' -> {generator_class.__name__}"
        )

    def get_generator(self, chart_type: str) -> BasePlotlyGenerator:
        """
        Retorna instancia do generator apropriado para o chart_type.

        Args:
            chart_type: Tipo de grafico (ex: "bar_horizontal", "pie")

        Returns:
            Instancia de BasePlotlyGenerator especializada

        Raises:
            ValueError: Se chart_type nao estiver registrado

        Example:
            >>> router = GeneratorRouter(PlotStyler())
            >>> generator = router.get_generator("bar_horizontal")
            >>> isinstance(generator, BasePlotlyGenerator)
            True
        """
        if chart_type not in self._registry:
            supported_types = list(self._registry.keys())
            raise ValueError(
                f"Chart type '{chart_type}' nao suportado. "
                f"Tipos suportados: {supported_types}"
            )

        generator_class = self._registry[chart_type]
        generator_instance = generator_class(styler=self.styler)

        logger.debug(
            f"Generator instanciado: {generator_class.__name__} para '{chart_type}'"
        )

        return generator_instance

    def get_supported_chart_types(self) -> list[str]:
        """
        Retorna lista de tipos de grafico suportados.

        Returns:
            Lista com chart_types registrados

        Example:
            >>> router = GeneratorRouter(PlotStyler())
            >>> "bar_horizontal" in router.get_supported_chart_types()
            True
        """
        return list(self._registry.keys())

    def is_supported(self, chart_type: str) -> bool:
        """
        Verifica se um chart_type esta registrado.

        Args:
            chart_type: Tipo de grafico a verificar

        Returns:
            True se suportado, False caso contrario

        Example:
            >>> router = GeneratorRouter(PlotStyler())
            >>> router.is_supported("bar_horizontal")
            True
            >>> router.is_supported("unknown_type")
            False
        """
        return chart_type in self._registry
