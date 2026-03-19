"""
Modulo para deteccao de contexto em queries de usuario.

Analisa padroes na query original para refinar decisoes de agregacao,
especialmente para decidir entre COUNT e COUNT DISTINCT.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class ContextDetector:
    """
    Detecta padroes contextuais em queries para refinar agregacoes.

    Principais funcoes:
    - Distinguir COUNT vs COUNT DISTINCT baseado em palavras-chave
    - Identificar intencao de agregacao (soma, media, contagem)
    - Detectar qualificadores temporais ou espaciais
    """

    # Padroes que indicam COUNT DISTINCT
    DISTINCT_PATTERNS = [
        r'\b(unico|única|unicos|únicas)\b',
        r'\b(distinto|distintos|distinta|distintas)\b',
        r'\b(diferente|diferentes)\b',
        r'\b(quantos|quantas)\s+(diferentes|distintos|unicos)\b',
        r'\b(tipos|categorias|variedades)\s+de\b',
    ]

    # Padroes que indicam COUNT simples
    COUNT_PATTERNS = [
        r'\b(total\s+de|quantidade\s+de)\b',
        r'\b(numero\s+de|número\s+de)\b',
        r'\b(quantas\s+vezes|quantos\s+registros)\b',
        r'\b(contagem|contar)\b',
    ]

    # Padroes que indicam SUM
    SUM_PATTERNS = [
        r'\b(soma|somar|total)\b',
        r'\b(somatorio|somatório)\b',
        r'\b(quanto\s+foi|qual\s+o\s+total)\b',
    ]

    # Padroes que indicam AVG
    AVG_PATTERNS = [
        r'\b(media|média|medio|médio)\b',
        r'\b(em\s+media|em\s+média)\b',
        r'\b(ticket\s+medio|ticket\s+médio)\b',
    ]

    # Padroes que indicam MIN/MAX
    MIN_PATTERNS = [
        r'\b(menor|minimo|mínimo|mais\s+baixo)\b',
    ]

    MAX_PATTERNS = [
        r'\b(maior|maximo|máximo|mais\s+alto)\b',
    ]

    def __init__(self):
        """Inicializa o detector de contexto."""
        # Compila regex patterns para melhor performance
        self.distinct_regex = [re.compile(p, re.IGNORECASE) for p in self.DISTINCT_PATTERNS]
        self.count_regex = [re.compile(p, re.IGNORECASE) for p in self.COUNT_PATTERNS]
        self.sum_regex = [re.compile(p, re.IGNORECASE) for p in self.SUM_PATTERNS]
        self.avg_regex = [re.compile(p, re.IGNORECASE) for p in self.AVG_PATTERNS]
        self.min_regex = [re.compile(p, re.IGNORECASE) for p in self.MIN_PATTERNS]
        self.max_regex = [re.compile(p, re.IGNORECASE) for p in self.MAX_PATTERNS]

    def should_use_distinct(self, query: str) -> bool:
        """
        Determina se deve usar COUNT DISTINCT baseado na query.

        Args:
            query: Texto da query original do usuario

        Returns:
            True se deve usar COUNT DISTINCT
        """
        query_lower = query.lower()

        # Verifica padroes de distinct
        for pattern in self.distinct_regex:
            if pattern.search(query_lower):
                logger.debug(f"Padrao DISTINCT detectado na query: {pattern.pattern}")
                return True

        return False

    def detect_aggregation_intent(self, query: str) -> Optional[str]:
        """
        Detecta a intencao de agregacao na query.

        Args:
            query: Texto da query original do usuario

        Returns:
            Tipo de agregacao: "sum", "avg", "count", "count_distinct", "min", "max" ou None
        """
        query_lower = query.lower()

        # Ordem de prioridade na deteccao

        # 1. MIN/MAX (mais especificos)
        for pattern in self.min_regex:
            if pattern.search(query_lower):
                logger.debug(f"Intencao MIN detectada: {pattern.pattern}")
                return "min"

        for pattern in self.max_regex:
            if pattern.search(query_lower):
                logger.debug(f"Intencao MAX detectada: {pattern.pattern}")
                return "max"

        # 2. AVG (antes de SUM porque 'média' é mais específico)
        for pattern in self.avg_regex:
            if pattern.search(query_lower):
                logger.debug(f"Intencao AVG detectada: {pattern.pattern}")
                return "avg"

        # 3. SUM
        for pattern in self.sum_regex:
            if pattern.search(query_lower):
                logger.debug(f"Intencao SUM detectada: {pattern.pattern}")
                return "sum"

        # 4. COUNT DISTINCT
        if self.should_use_distinct(query):
            return "count_distinct"

        # 5. COUNT simples
        for pattern in self.count_regex:
            if pattern.search(query_lower):
                logger.debug(f"Intencao COUNT detectada: {pattern.pattern}")
                return "count"

        # Nenhum padrao detectado
        return None

    def refine_aggregation(
        self,
        base_aggregation: str,
        query: str,
        column_type: str
    ) -> str:
        """
        Refina a agregacao baseada no contexto da query.

        Args:
            base_aggregation: Agregacao base sugerida pelo AggregationSelector
            query: Query original do usuario
            column_type: Tipo da coluna ("numeric", "categorical", "temporal")

        Returns:
            Agregacao refinada
        """
        # Detecta intencao explicita na query
        detected_intent = self.detect_aggregation_intent(query)

        # Se nao detectou intencao explicita, usa a base
        if not detected_intent:
            return base_aggregation

        # Valida se a intencao detectada e compativel com o tipo da coluna
        if column_type == "numeric":
            # Para numericas, todas as agregacoes sao validas
            logger.info(
                f"Refinando agregacao numerica: {base_aggregation} -> {detected_intent} "
                f"(detectado na query)"
            )
            return detected_intent

        elif column_type in ["categorical", "temporal"]:
            # Para categoricas/temporais, apenas COUNT e COUNT DISTINCT sao validos
            if detected_intent in ["count", "count_distinct"]:
                logger.info(
                    f"Refinando agregacao categorica: {base_aggregation} -> {detected_intent}"
                )
                return detected_intent
            else:
                logger.warning(
                    f"Intencao '{detected_intent}' invalida para coluna {column_type}. "
                    f"Mantendo: {base_aggregation}"
                )
                return base_aggregation

        return base_aggregation

    def extract_ranking_intent(self, query: str) -> Optional[dict]:
        """
        Detecta se a query solicita ranking ou ordenacao.

        Args:
            query: Query original do usuario

        Returns:
            Dict com informacoes do ranking ou None
        """
        ranking_patterns = [
            (r'\b(\d+)\s+(maiores|menores|primeiros|ultimos|últimos)\b', 'explicit'),
            (r'\b(top|ranking)\s+(\d+)\b', 'top'),
            (r'\b(maiores|menores)\b', 'implicit'),
        ]

        query_lower = query.lower()

        for pattern, pattern_type in ranking_patterns:
            match = re.search(pattern, query_lower, re.IGNORECASE)
            if match:
                result = {
                    "type": pattern_type,
                    "direction": "DESC" if "maior" in match.group(0) or "top" in match.group(0) else "ASC",
                    "match": match.group(0)
                }

                # Extrai limite se disponivel
                numbers = re.findall(r'\d+', match.group(0))
                if numbers:
                    result["limit"] = int(numbers[0])

                logger.debug(f"Ranking detectado: {result}")
                return result

        return None

    def is_temporal_aggregation(self, query: str) -> bool:
        """
        Detecta se a query solicita agregacao temporal.

        Args:
            query: Query original do usuario

        Returns:
            True se for agregacao temporal
        """
        temporal_patterns = [
            r'\b(por\s+mes|por\s+mês|mensal|mensalmente)\b',
            r'\b(por\s+ano|anual|anualmente)\b',
            r'\b(por\s+dia|diario|diário|diariamente)\b',
            r'\b(por\s+trimestre|trimestral)\b',
            r'\b(evolucao|evolução|tendencia|tendência)\b',
            r'\b(historico|histórico|ao\s+longo\s+do\s+tempo)\b',
        ]

        query_lower = query.lower()

        for pattern in temporal_patterns:
            if re.search(pattern, query_lower):
                logger.debug(f"Agregacao temporal detectada: {pattern}")
                return True

        return False
