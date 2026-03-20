"""
PreMatchEngine - Pre-LLM fuzzy matching engine for filter value resolution.

Resolves query tokens against the ValueCatalog using exact and fuzzy matching
before the LLM call. Provides ranked candidates with scoring based on:
- Match quality (exact vs fuzzy)
- Column cardinality (lower = higher priority)
- Alias.yaml matches
- Important values from filter_hints

This runs locally with zero LLM token cost and adds <15ms latency per query.
"""

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from rapidfuzz import fuzz, process

from src.shared_lib.utils.logger import get_logger
from src.shared_lib.core.config import load_alias_data, build_keyword_to_column_map
from src.shared_lib.data.value_catalog import ValueCatalog, normalize_text

logger = get_logger(__name__)

# Scoring constants
EXACT_MATCH_BASE = 100
CARDINALITY_BOOST_VERY_LOW = 20  # 1-2 values
CARDINALITY_BOOST_LOW = 20       # 3-5 values
CARDINALITY_BOOST_MEDIUM = 10    # 6-30 values
CARDINALITY_BOOST_HIGH = 0       # 30+ values
ALIAS_MATCH_BOOST = 15
IMPORTANT_VALUE_BOOST = 10
FUZZY_THRESHOLD = 75
AUTO_RESOLVE_SCORE_DIFF = 20


@dataclass
class MatchCandidate:
    """A candidate match from the pre-matching engine."""

    token_original: str
    column: str
    value: str
    score: float
    match_type: str  # "exact_value", "fuzzy_value", "alias_column"
    ambiguous: bool = False

    def to_dict(self) -> Dict:
        return {
            "token": self.token_original,
            "column": self.column,
            "value": self.value,
            "score": self.score,
            "match_type": self.match_type,
            "ambiguous": self.ambiguous,
        }


class PreMatchEngine:
    """
    Pre-LLM matching engine that resolves query tokens to dataset values.

    Uses ValueCatalog for value lookup and alias.yaml for column resolution.
    """

    def __init__(self, catalog: Optional[ValueCatalog] = None):
        self.catalog = catalog or ValueCatalog.get_instance()
        self._alias_data = load_alias_data()
        self._keyword_to_column = build_keyword_to_column_map()
        self._important_values = self._load_important_values()
        self._value_aliases = self._load_value_aliases()
        self._categories = self._load_categories()
        self._stopwords = self._build_stopwords()
        self._valid_years = self._load_valid_years()
        self._temporal_column = self._get_year_column()

    def _load_important_values(self) -> Dict[str, Set[str]]:
        """Load important_values from filter_hints in alias.yaml."""
        hints = self._alias_data.get("filter_hints", {})
        important = hints.get("important_values", {})
        result = {}
        for col, values in important.items():
            result[col] = {normalize_text(str(v)) for v in values}
        return result

    def _load_categories(self) -> Dict[str, List[str]]:
        """Load semantic categories from alias.yaml."""
        return self._alias_data.get("categories", {})

    def _load_valid_years(self) -> Set[int]:
        """Load valid years from filter_hints in alias.yaml."""
        hints = self._alias_data.get("filter_hints", {})
        years = hints.get("valid_years", [])
        return set(int(y) for y in years)

    def _get_year_column(self) -> str:
        """Get the column name used for year filtering.

        Reads from temporal_mapping.derived_columns or falls back to 'ano'.
        """
        temporal = self._alias_data.get("temporal_mapping", {})
        derived = temporal.get("derived_columns", {})
        return derived.get("year", "ano")

    def _load_value_aliases(self) -> Dict[str, Dict[str, str]]:
        """
        Load value_aliases from alias.yaml.

        Returns: {column -> {normalized_alias -> real_dataset_value}}
        """
        raw = self._alias_data.get("value_aliases", {})
        result = {}
        for col, mappings in raw.items():
            result[col] = {}
            for alias_term, real_value in mappings.items():
                norm = normalize_text(str(alias_term))
                result[col][norm] = str(real_value)
        return result

    def _build_stopwords(self) -> Set[str]:
        """Build a set of stopwords that should not be matched as values."""
        return {
            # Portuguese stopwords
            "de", "do", "da", "dos", "das", "em", "no", "na", "nos", "nas",
            "por", "para", "com", "sem", "que", "qual", "quais", "como",
            "o", "a", "os", "as", "um", "uma", "uns", "umas",
            "e", "ou", "mais", "menos", "muito", "pouco",
            # Query verbs/terms
            "mostre", "mostra", "mostrar", "quero", "ver", "exibir", "exiba",
            "listar", "liste", "filtrar", "filtre", "dados", "dado",
            "grafico", "tabela", "comparar", "compare", "comparativo",
            "total", "soma", "media", "quantidade",
            # Ranking/aggregation terms
            "top", "maiores", "maior", "menores", "menor",
            "melhores", "melhor", "piores", "pior",
            "principais", "principal", "todos", "todas", "geral",
            # Generic nouns that should not be filter values
            "vendas", "venda", "produtos", "produto", "clientes", "cliente",
            "faturamento", "receita", "market", "share", "marketshare",
            # Column name aliases used as generic terms (not as filter values)
            "marcas", "fabricantes", "categorias", "subcategorias",
            "regioes", "estados", "canais", "tipos",
        }

    def _tokenize(self, query: str) -> List[str]:
        """Tokenize query into individual words, removing stopwords."""
        # Lowercase and clean
        text = query.lower().strip()
        # Split on whitespace and punctuation (keep hyphens within words)
        tokens = re.findall(r"[a-zA-Z\u00C0-\u024F0-9]+(?:-[a-zA-Z\u00C0-\u024F0-9]+)*", text)
        return tokens

    def _generate_ngrams(self, tokens: List[str], max_n: int = 3) -> List[Tuple[str, int, int]]:
        """
        Generate n-grams from token list.

        Returns: List of (ngram_text, start_idx, end_idx)
        Ordered: longer n-grams first (greedy matching).
        """
        ngrams = []
        for n in range(min(max_n, len(tokens)), 0, -1):
            for i in range(len(tokens) - n + 1):
                gram_tokens = tokens[i : i + n]
                gram_text = " ".join(gram_tokens)
                ngrams.append((gram_text, i, i + n))
        return ngrams

    def _get_cardinality_boost(self, column: str) -> float:
        """Get scoring boost based on column cardinality."""
        tier = self.catalog.get_cardinality_tier(column)
        boosts = {
            "very_low": CARDINALITY_BOOST_VERY_LOW,
            "low": CARDINALITY_BOOST_LOW,
            "medium": CARDINALITY_BOOST_MEDIUM,
            "high": CARDINALITY_BOOST_HIGH,
        }
        return boosts.get(tier, 0)

    def _is_important_value(self, column: str, normalized_value: str) -> bool:
        """Check if a value is in the important_values list for its column."""
        return normalized_value in self._important_values.get(column, set())

    def _get_alias_column(self, term: str) -> Optional[str]:
        """Check if term is a direct alias for a column."""
        return self._keyword_to_column.get(term.lower())

    def _get_category_for_column(self, column: str) -> Optional[str]:
        """Get the semantic category a column belongs to."""
        for category, columns in self._categories.items():
            if column in columns:
                return category
        return None

    def _compute_score(
        self, base_score: float, column: str, normalized_value: str, token: str
    ) -> float:
        """Compute final score with all boosts applied."""
        score = base_score

        # Cardinality boost
        score += self._get_cardinality_boost(column)

        # Alias boost: if the token is a known alias for this column
        alias_col = self._get_alias_column(token)
        if alias_col and alias_col == column:
            score += ALIAS_MATCH_BOOST

        # Important value boost
        if self._is_important_value(column, normalized_value):
            score += IMPORTANT_VALUE_BOOST

        return score

    def find_candidates(self, query: str) -> List[MatchCandidate]:
        """
        Find filter candidates by matching query tokens against ValueCatalog.

        Steps:
        1. Tokenize query
        2. Generate n-grams (1, 2, 3 tokens)
        3. For each n-gram: exact match -> fuzzy match
        4. Score and disambiguate
        5. Return ranked candidates
        """
        tokens = self._tokenize(query)
        if not tokens:
            return []

        ngrams = self._generate_ngrams(tokens)
        raw_candidates: List[MatchCandidate] = []
        consumed_indices: Set[int] = set()

        for gram_text, start_idx, end_idx in ngrams:
            # Skip if any token in this n-gram was already consumed by a longer match
            if any(i in consumed_indices for i in range(start_idx, end_idx)):
                continue

            normalized = normalize_text(gram_text)

            # Skip stopwords (only for unigrams)
            if start_idx + 1 == end_idx and normalized in self._stopwords:
                continue

            # Handle pure numbers: check if it's a valid year
            if normalized.isdigit():
                year_val = int(normalized)
                if year_val in self._valid_years:
                    # Valid year → generate candidate for the year column
                    year_candidate = MatchCandidate(
                        token_original=gram_text,
                        column=self._temporal_column,
                        value=year_val,
                        score=EXACT_MATCH_BASE + IMPORTANT_VALUE_BOOST,
                        match_type="year_match",
                    )
                    raw_candidates.append(year_candidate)
                    for i in range(start_idx, end_idx):
                        consumed_indices.add(i)
                    logger.debug(
                        f"[PreMatchEngine] Year match: {gram_text} -> "
                        f"{self._temporal_column}={year_val}"
                    )
                # Skip further processing for digits (not a categorical value)
                continue

            is_unigram = (end_idx - start_idx) == 1
            candidates_for_gram = []

            # Step 1: Exact lookup in inverted index
            exact_matches = self.catalog.lookup_exact(normalized)
            if exact_matches:
                for col, original_val in exact_matches:
                    score = self._compute_score(
                        EXACT_MATCH_BASE, col, normalized, gram_text
                    )
                    candidates_for_gram.append(
                        MatchCandidate(
                            token_original=gram_text,
                            column=col,
                            value=original_val,
                            score=score,
                            match_type="exact_value",
                        )
                    )

            # Step 1.5: Value alias lookup (from value_aliases in alias.yaml)
            # Runs regardless of exact matches — aliases may point to different
            # columns (e.g. "granado" -> fabricante exact, but also marca via alias)
            for col, alias_map in self._value_aliases.items():
                if normalized in alias_map:
                    real_value = alias_map[normalized]
                    # Avoid duplicate if already found via exact match in same column
                    already_found = any(
                        c.column == col and c.value == real_value
                        for c in candidates_for_gram
                    )
                    if not already_found:
                        score = self._compute_score(
                            EXACT_MATCH_BASE, col, normalize_text(real_value), gram_text
                        )
                        candidates_for_gram.append(
                            MatchCandidate(
                                token_original=gram_text,
                                column=col,
                                value=real_value,
                                score=score,
                                match_type="value_alias",
                            )
                        )

            # Step 2: Fuzzy matching — only for unigrams to avoid
            # multi-token grams consuming tokens that would exact-match as unigrams
            if not candidates_for_gram and is_unigram:
                all_normalized = self.catalog.get_all_normalized_values()
                if all_normalized:
                    fuzzy_results = process.extract(
                        normalized,
                        all_normalized,
                        scorer=fuzz.ratio,
                        limit=5,
                        score_cutoff=FUZZY_THRESHOLD,
                    )
                    for matched_norm, fuzzy_score, _ in fuzzy_results:
                        matches = self.catalog.lookup_exact(matched_norm)
                        for col, original_val in matches:
                            score = self._compute_score(
                                fuzzy_score, col, matched_norm, gram_text
                            )
                            candidates_for_gram.append(
                                MatchCandidate(
                                    token_original=gram_text,
                                    column=col,
                                    value=original_val,
                                    score=score,
                                    match_type="fuzzy_value",
                                )
                            )

            if candidates_for_gram:
                # Mark consumed tokens
                for i in range(start_idx, end_idx):
                    consumed_indices.add(i)
                raw_candidates.extend(candidates_for_gram)

        # Disambiguate and rank
        return self._disambiguate(raw_candidates)

    def _disambiguate(self, candidates: List[MatchCandidate]) -> List[MatchCandidate]:
        """
        Disambiguate candidates when same token matches multiple columns.

        Rules:
        1. Score diff >= 20: auto-resolve to highest score
        2. Score diff < 20: mark as ambiguous, keep both for LLM
        3. Same category conflict: keep highest score only
        """
        # Group by token
        by_token: Dict[str, List[MatchCandidate]] = {}
        for c in candidates:
            key = c.token_original.lower()
            if key not in by_token:
                by_token[key] = []
            by_token[key].append(c)

        result = []
        for token, token_candidates in by_token.items():
            if len(token_candidates) == 1:
                result.append(token_candidates[0])
                continue

            # Sort by score descending
            token_candidates.sort(key=lambda c: c.score, reverse=True)
            best = token_candidates[0]

            # Check if clear winner
            if len(token_candidates) > 1:
                second = token_candidates[1]
                score_diff = best.score - second.score

                if score_diff >= AUTO_RESOLVE_SCORE_DIFF:
                    # Clear winner
                    result.append(best)
                else:
                    # Check same-category conflict
                    best_cat = self._get_category_for_column(best.column)
                    second_cat = self._get_category_for_column(second.column)

                    if best_cat and best_cat == second_cat:
                        # Same category: keep only the best
                        result.append(best)
                    else:
                        # Different categories: mark ambiguous, keep both
                        best.ambiguous = True
                        second.ambiguous = True
                        result.append(best)
                        result.append(second)
            else:
                result.append(best)

        # Sort final result by score descending
        result.sort(key=lambda c: c.score, reverse=True)
        return result

    def format_candidates_for_prompt(self, candidates: List[MatchCandidate]) -> str:
        """
        Format candidates as a markdown table for injection into the LLM prompt.

        Returns empty string if no candidates found.
        """
        if not candidates:
            return ""

        lines = [
            "## Candidatos Pre-Resolvidos",
            "",
            "O sistema identificou os seguintes candidatos nos dados reais:",
            "",
            "| Termo | Coluna Sugerida | Valor Sugerido | Confianca | Tipo |",
            "|-------|----------------|----------------|-----------|------|",
        ]

        for c in candidates:
            confidence = min(c.score / 135.0, 1.0)  # Normalize to 0-1
            ambig_marker = " [AMBIGUO]" if c.ambiguous else ""
            lines.append(
                f"| \"{c.token_original}\" | {c.column} | {c.value} | "
                f"{confidence:.2f}{ambig_marker} | {c.match_type} |"
            )

        lines.extend([
            "",
            "Utilize esses candidatos como base para sua resposta.",
            "Se concordar, use os valores sugeridos.",
            "Se discordar, explique no reasoning.",
        ])

        return "\n".join(lines)
