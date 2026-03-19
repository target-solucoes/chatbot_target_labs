"""
FilterParser - LLM-based filter extraction and CRUD operation identification.

This module provides intelligent parsing of user queries to detect filters
and classify CRUD operations (ADICIONAR, ALTERAR, REMOVER, MANTER).
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from langchain_core.messages import SystemMessage, HumanMessage

from src.filter_classifier.models.llm_loader import create_structured_llm
from src.filter_classifier.core.settings import MIN_CONFIDENCE_THRESHOLD
from src.filter_classifier.validation.semantic_validator import SemanticValidator
from src.filter_classifier.validation.query_mention_validator import (
    QueryMentionValidator,
)
from src.graphic_classifier.tools.alias_mapper import AliasMapper
from src.shared_lib.data.dataset_column_extractor import DatasetColumnExtractor

logger = logging.getLogger(__name__)


class FilterParser:
    """
    LLM-based parser for extracting filters and identifying CRUD operations.

    This class uses a carefully engineered prompt with few-shot examples to:
    1. Detect filters mentioned in user queries
    2. Extract column names, values, and operators
    3. Classify filter operations as ADICIONAR, ALTERAR, REMOVER, or MANTER
    4. Provide confidence scoring for filter detection
    """

    def __init__(
        self,
        alias_mapper: Optional[AliasMapper] = None,
        dataset_path: Optional[str] = None,
        prompt_path: Optional[str] = None,
    ):
        """
        Initialize the FilterParser with LLM and required tools.

        Args:
            alias_mapper: AliasMapper instance for column alias resolution.
                If None, will be initialized with default alias path.
            dataset_path: Path to dataset for column extraction.
                If None, column placeholders will be empty.
            prompt_path: Path to prompt template. If None, uses default location.
        """
        self.llm = create_structured_llm()
        self.alias_mapper = alias_mapper
        self.dataset_path = dataset_path

        # Load prompt template
        if prompt_path is None:
            prompt_path = self._get_default_prompt_path()
        self.prompt_template = self._load_prompt_template(prompt_path)

        # Cache for dataset columns and categorical values
        self._dataset_columns: Optional[List[str]] = None
        self._categorical_values: Optional[Dict[str, List[Any]]] = None

        logger.info("[FilterParser] Initialized successfully")

    def _get_default_prompt_path(self) -> str:
        """Get default path to prompt template."""
        return str(Path(__file__).parent.parent / "prompts" / "filter_parser_prompt.md")

    def _load_prompt_template(self, prompt_path: str) -> str:
        """
        Load prompt template from markdown file.

        Args:
            prompt_path: Path to prompt template file.

        Returns:
            str: Prompt template content.

        Raises:
            FileNotFoundError: If prompt file doesn't exist.
        """
        path = Path(prompt_path)
        if not path.exists():
            raise FileNotFoundError(f"Prompt template not found at: {prompt_path}")

        with open(path, "r", encoding="utf-8") as f:
            template = f.read()

        logger.debug(f"[FilterParser] Loaded prompt template from {prompt_path}")
        return template

    def _get_dataset_columns(self) -> List[str]:
        """
        Extract dataset columns (cached).

        Returns:
            List of column names from dataset.
        """
        if self._dataset_columns is not None:
            return self._dataset_columns

        if self.dataset_path is None:
            logger.warning(
                "[FilterParser] No dataset path provided, returning empty columns"
            )
            return []

        try:
            extractor = DatasetColumnExtractor()
            self._dataset_columns = extractor.get_columns(self.dataset_path)
            logger.debug(
                f"[FilterParser] Extracted {len(self._dataset_columns)} columns"
            )
            return self._dataset_columns
        except Exception as e:
            logger.error(f"[FilterParser] Failed to extract columns: {str(e)}")
            return []

    def _get_column_aliases(self) -> Dict[str, List[str]]:
        """
        Get column aliases from AliasMapper.

        Returns:
            Dict mapping canonical columns to their aliases.
        """
        if self.alias_mapper is None:
            return {}

        try:
            # Get all mappings from alias mapper
            aliases_dict = {}
            for column in self._get_dataset_columns():
                # Get reverse mapping (canonical -> aliases)
                aliases_dict[column] = self.alias_mapper.get_column_aliases(column)
            return aliases_dict
        except Exception as e:
            logger.warning(f"[FilterParser] Failed to get aliases: {str(e)}")
            return {}

    def _get_categorical_values(self, limit: int = 20) -> Dict[str, List[Any]]:
        """
        Extract categorical values from dataset (cached).

        Args:
            limit: Maximum number of unique values per column to include.

        Returns:
            Dict mapping column names to lists of valid categorical values.
        """
        if self._categorical_values is not None:
            return self._categorical_values

        if self.dataset_path is None:
            return {}

        try:
            import pandas as pd

            # Load dataset (sample for performance)
            df = pd.read_parquet(self.dataset_path, engine="pyarrow")
            if len(df) > 1000:
                df = df.sample(n=1000, random_state=42)

            categorical_values = {}
            for col in df.columns:
                # Only include columns with reasonable number of unique values
                unique_vals = df[col].dropna().unique()
                if len(unique_vals) <= 50:  # Categorical threshold
                    categorical_values[col] = list(unique_vals[:limit])

            self._categorical_values = categorical_values
            logger.debug(
                f"[FilterParser] Extracted categorical values for {len(categorical_values)} columns"
            )
            return categorical_values

        except Exception as e:
            logger.error(
                f"[FilterParser] Failed to extract categorical values: {str(e)}"
            )
            return {}

    def _build_prompt(
        self, query: str, current_filters: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Build complete prompt by substituting placeholders.

        Args:
            query: User query to analyze.
            current_filters: Currently active filters (for CRUD classification).

        Returns:
            str: Complete prompt ready for LLM.
        """
        # Get dynamic content
        dataset_columns = self._get_dataset_columns()
        column_aliases = self._get_column_aliases()
        # REMOVED: categorical_values = self._get_categorical_values()
        # REASON: Injecting real dataset values causes LLM to infer filters
        # based on semantic association (e.g., "produto" -> "PRODUTOS REVENDA")
        # Validation of values will be done AFTER detection, not DURING
        current_filters = current_filters or {}

        # Format placeholders
        columns_str = (
            ", ".join(dataset_columns) if dataset_columns else "No columns available"
        )

        aliases_str = (
            "\n".join(
                [
                    f"- {col}: {', '.join(aliases)}"
                    for col, aliases in column_aliases.items()
                    if aliases
                ]
            )
            if column_aliases
            else "No aliases configured"
        )

        # REMOVED categorical_str injection - replaced with validation notice
        categorical_str = (
            "Valores categoricos serao validados APOS a deteccao.\n"
            "IMPORTANTE: Detecte APENAS valores EXPLICITAMENTE mencionados na query.\n"
            "NAO invente, sugira ou infira valores baseado em conhecimento geral.\n"
            "Se o usuario menciona 'produto' (generico), NAO adicione nenhum filtro de produto.\n"
            "Se o usuario menciona 'ADESIVOS' (especifico), detecte Des_Grupo_Produto = 'ADESIVOS'."
        )

        current_filters_str = json.dumps(current_filters, indent=2, ensure_ascii=False)

        # Substitute placeholders
        prompt = self.prompt_template.replace("{dataset_columns}", columns_str)
        prompt = prompt.replace("{column_aliases}", aliases_str)
        prompt = prompt.replace("{categorical_values}", categorical_str)
        prompt = prompt.replace("{current_filters}", current_filters_str)
        prompt = prompt.replace("{query}", query)

        return prompt

    def parse_query(
        self, query: str, current_filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Parse user query to extract filters and identify CRUD operations.

        This is the main method that:
        1. Builds the prompt with current context
        2. Invokes LLM to extract filters
        3. Parses and validates the response
        4. Returns structured filter specification

        Args:
            query: User query in natural language.
            current_filters: Currently active filters from previous session.

        Returns:
            Dict with structure:
            {
                "detected_filters": {
                    "column_name": {
                        "value": ...,
                        "operator": "=",
                        "confidence": 0.95
                    }
                },
                "crud_operations": {
                    "ADICIONAR": ["col1"],
                    "ALTERAR": ["col2"],
                    "REMOVER": ["col3"],
                    "MANTER": ["col4"]
                },
                "reasoning": "...",
                "confidence": 0.90
            }

        Raises:
            ValueError: If LLM response is invalid or unparseable.
        """
        logger.info(f"[FilterParser] Parsing query: {query}")

        try:
            # Build prompt with context
            prompt = self._build_prompt(query, current_filters)

            # Invoke LLM
            messages = [HumanMessage(content=prompt)]
            response = self.llm.invoke(messages)

            # Extract JSON from response
            response_text = response.content.strip()

            # Handle markdown code blocks
            if "```json" in response_text:
                response_text = (
                    response_text.split("```json")[1].split("```")[0].strip()
                )
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()

            # Parse JSON
            parsed_response = json.loads(response_text)

            # Validate response structure
            self._validate_response(parsed_response)

            # SEMANTIC VALIDATION: Remove numeric filters and validate categorical values
            parsed_response = self._validate_semantic_filters(
                parsed_response, query, current_filters or {}
            )

            # Log confidence
            confidence = parsed_response.get("confidence", 0.0)
            logger.info(
                f"[FilterParser] Parsed successfully (confidence: {confidence:.2f})"
            )

            if confidence < MIN_CONFIDENCE_THRESHOLD:
                logger.warning(
                    f"[FilterParser] Low confidence ({confidence:.2f}) "
                    f"below threshold ({MIN_CONFIDENCE_THRESHOLD})"
                )

            # Include LLM response for token tracking
            parsed_response["_llm_response"] = response

            return parsed_response

        except json.JSONDecodeError as e:
            logger.error(
                f"[FilterParser] Failed to parse LLM response as JSON: {str(e)}"
            )
            logger.debug(f"[FilterParser] Raw response: {response_text}")
            raise ValueError(f"Invalid JSON response from LLM: {str(e)}")

        except Exception as e:
            logger.error(f"[FilterParser] Error during parsing: {str(e)}")
            raise

    def _validate_response(self, response: Dict[str, Any]) -> None:
        """
        Validate LLM response structure.

        Args:
            response: Parsed JSON response from LLM.

        Raises:
            ValueError: If response structure is invalid.
        """
        required_keys = ["detected_filters", "crud_operations", "confidence"]
        missing_keys = [key for key in required_keys if key not in response]

        if missing_keys:
            raise ValueError(f"Missing required keys in response: {missing_keys}")

        # Validate crud_operations structure
        crud_ops = response["crud_operations"]
        expected_ops = ["ADICIONAR", "ALTERAR", "REMOVER", "MANTER"]
        for op in expected_ops:
            if op not in crud_ops:
                logger.warning(f"[FilterParser] Missing CRUD operation: {op}")
                response["crud_operations"][op] = []

        # Validate confidence range
        confidence = response["confidence"]
        if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
            raise ValueError(f"Invalid confidence value: {confidence}")

    def _validate_semantic_filters(
        self, response: Dict[str, Any], query: str, current_filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Valida filtros semanticamente para garantir que apenas valores categoricos
        sejam aceitos, rejeitando valores numericos de termos de ranking.

        Esta funcao:
        - Remove filtros com valores numericos isolados
        - Detecta termos de ranking na query
        - Remove filtros pontuais em queries de ranking
        - NOVO: Valida se valores foram MENCIONADOS na query (nao inferidos)
        - Corrige operacoes CRUD baseado na validacao semantica

        Args:
            response: Parsed LLM response with crud_operations
            query: Query original do usuario
            current_filters: Currently active filters

        Returns:
            Corrected response with semantically validated filters
        """
        detected_filters = response.get("detected_filters", {})
        crud = response["crud_operations"]

        # Step 1: Validar filtros detectados (remover numericos)
        valid_filters, rejected_numeric = (
            SemanticValidator.validate_non_quantitative_filters(detected_filters, query)
        )

        # Log filtros rejeitados por serem numericos
        if rejected_numeric:
            logger.warning(
                f"[SemanticValidation] {len(rejected_numeric)} filtros numericos rejeitados: "
                f"{list(rejected_numeric.keys())}"
            )
            for col, info in rejected_numeric.items():
                logger.debug(f"  - {col}: {info['value']} (Razao: {info['reason']})")

        # Step 2: NOVA VALIDACAO - Verificar se valores foram MENCIONADOS na query
        # Isso previne que o LLM infira valores baseado em associacao semantica
        # Ex: "produto" -> "PRODUTOS REVENDA" (REJEITADO)
        valid_filters, rejected_inferred = QueryMentionValidator.validate_filters(
            valid_filters, query
        )

        # Log filtros rejeitados por inferencia indevida
        if rejected_inferred:
            logger.warning(
                f"[QueryMentionValidation] {len(rejected_inferred)} filtros inferidos rejeitados: "
                f"{list(rejected_inferred.keys())}"
            )
            for col, info in rejected_inferred.items():
                logger.warning(
                    f"  - {col}: '{info['value']}' nao mencionado em query '{info['query']}' "
                    f"(Razao: {info['reason']})"
                )

        # Step 3: Atualizar detected_filters apenas com filtros validos
        response["detected_filters"] = valid_filters

        # Step 4: Reconstruir CRUD operations baseado em filtros validos
        corrected_crud = self._rebuild_crud_operations(
            valid_filters, current_filters, query
        )

        # Update response
        response["crud_operations"] = corrected_crud
        logger.info(
            f"[SemanticValidation] CRUD operations: "
            f"ADD={len(corrected_crud['ADICIONAR'])}, "
            f"ALTER={len(corrected_crud['ALTERAR'])}, "
            f"REMOVE={len(corrected_crud['REMOVER'])}, "
            f"KEEP={len(corrected_crud['MANTER'])}"
        )

        return response

    def _rebuild_crud_operations(
        self, valid_filters: Dict[str, Any], current_filters: Dict[str, Any], query: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Reconstroi operacoes CRUD baseado em filtros validos e contexto atual.

        Args:
            valid_filters: Filtros validados semanticamente
            current_filters: Filtros atualmente ativos
            query: Query original

        Returns:
            Dict com operacoes CRUD corrigidas
        """
        corrected_crud = {"ADICIONAR": {}, "ALTERAR": {}, "REMOVER": {}, "MANTER": {}}

        # Persistent filter columns - construido dinamicamente de alias.yaml
        # Inclui colunas temporais (e virtuais) que definem escopo da sessao
        persistent_columns = set()
        try:
            from src.shared_lib.core.config import get_temporal_columns

            temporal = get_temporal_columns()
            if temporal:
                persistent_columns.update(temporal)
                # Colunas virtuais derivadas de temporais
                persistent_columns.update({"Ano", "Mes", "Trimestre", "Semestre"})
        except Exception:
            pass

        # Step 1: Process valid filters
        for col, info in valid_filters.items():
            value = info.get("value") if isinstance(info, dict) else info

            if col in current_filters:
                # Column exists → ALTERAR or MANTER
                if current_filters[col] != value:
                    corrected_crud["ALTERAR"][col] = value
                    logger.debug(
                        f"[RebuildCRUD] ALTERAR {col}: {current_filters[col]} → {value}"
                    )
                else:
                    # Same value → MANTER
                    corrected_crud["MANTER"][col] = value
            else:
                # New column → ADICIONAR
                corrected_crud["ADICIONAR"][col] = value
                logger.debug(f"[RebuildCRUD] ADICIONAR {col}: {value}")

        # Step 2: Identificar filtros pontuais para remover (se query tem ranking)
        pontual_to_remove = SemanticValidator.should_remove_pontual_filters(
            current_filters, query
        )

        # Step 3: Process current_filters not mentioned
        for col, value in current_filters.items():
            if col not in valid_filters:
                # Column exists but not mentioned
                if col in pontual_to_remove:
                    # Pontual filters should be removed in ranking queries
                    corrected_crud["REMOVER"][col] = value
                    logger.debug(
                        f"[RebuildCRUD] REMOVER {col} (pontual em query de ranking)"
                    )
                elif col in persistent_columns:
                    # Persistent filters should be maintained
                    corrected_crud["MANTER"][col] = value
                    logger.debug(f"[RebuildCRUD] MANTER {col} (filtro persistente)")
                else:
                    # Default: maintain if not explicitly removed
                    corrected_crud["MANTER"][col] = value

        return corrected_crud

    def identify_crud_operations(
        self, detected_filters: Dict[str, Any], current_filters: Dict[str, Any]
    ) -> Dict[str, List[str]]:
        """
        Identify CRUD operations by comparing detected filters with current filters.

        NOTE: This method is deprecated in favor of the integrated approach
        where parse_query() returns both detected_filters and crud_operations
        from a single LLM call. This method is kept for compatibility and
        can be used as a fallback or for testing.

        Args:
            detected_filters: Filters detected in current query.
            current_filters: Filters currently active in session.

        Returns:
            Dict with CRUD operation classifications:
            {
                "ADICIONAR": ["col1"],
                "ALTERAR": ["col2"],
                "REMOVER": ["col3"],
                "MANTER": ["col4"]
            }
        """
        logger.debug("[FilterParser] Using fallback CRUD identification")

        operations = {"ADICIONAR": [], "ALTERAR": [], "REMOVER": [], "MANTER": []}

        # Identify new filters (ADICIONAR)
        for col in detected_filters:
            if col not in current_filters:
                operations["ADICIONAR"].append(col)
            else:
                # Check if value changed (ALTERAR)
                if detected_filters[col] != current_filters[col]:
                    operations["ALTERAR"].append(col)

        # Identify filters to keep (MANTER)
        for col in current_filters:
            if col not in detected_filters:
                operations["MANTER"].append(col)

        logger.debug(f"[FilterParser] CRUD operations: {operations}")
        return operations
