"""
Modulo responsavel pela selecao inteligente de funcoes de agregacao SQL.

Este modulo determina automaticamente qual funcao de agregacao usar
(SUM, COUNT, AVG, etc.) baseado no tipo da coluna e no contexto da query.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Literal, Optional, Set
import yaml

logger = logging.getLogger(__name__)


AggregationType = Literal[
    "sum", "avg", "count", "count_distinct", "min", "max", "median", "std", "var"
]


class AggregationSelector:
    """
    Seleciona funcoes de agregacao apropriadas baseado em:
    1. Tipo da coluna (numeric, categorical, temporal) do alias.yaml
    2. Schema SQL (BIGINT, VARCHAR, etc.) como fallback
    3. Override explicito do usuario

    Regras de Selecao:
    - Colunas numericas (Valor_Vendido, Qtd_Vendida, Peso_Vendido): SUM
    - Colunas categoricas (Cod_Vendedor, UF_Cliente, etc.): COUNT
    - Colunas temporais (Data, Mes, Ano): COUNT
    - Override do usuario sempre tem prioridade
    """

    # Tipos SQL que indicam colunas quantitativas
    QUANTITATIVE_SQL_TYPES = {
        "BIGINT",
        "INTEGER",
        "SMALLINT",
        "TINYINT",
        "DOUBLE",
        "FLOAT",
        "DECIMAL",
        "NUMERIC",
        "REAL",
    }

    # Tipos SQL que indicam colunas categoricas
    CATEGORICAL_SQL_TYPES = {"VARCHAR", "STRING", "TEXT", "CHAR", "BOOLEAN"}

    # Tipos SQL temporais
    TEMPORAL_SQL_TYPES = {"DATE", "TIMESTAMP", "TIME", "DATETIME"}

    def __init__(self, alias_yaml_path: Optional[str] = None):
        """
        Inicializa o seletor de agregacao.

        Args:
            alias_yaml_path: Caminho customizado para alias.yaml.
                           Se None, usa o caminho padrao do projeto.
        """
        self.column_types: Dict[str, Set[str]] = {
            "numeric": set(),
            "categorical": set(),
            "temporal": set(),
        }

        # Carrega configuracao do alias.yaml
        self._load_column_types(alias_yaml_path)

    def _load_column_types(self, custom_path: Optional[str] = None) -> None:
        """
        Carrega a classificacao de tipos de colunas do alias.yaml.

        Args:
            custom_path: Caminho customizado para o arquivo YAML
        """
        if custom_path:
            yaml_path = Path(custom_path)
        else:
            # Caminho padrao relativo a raiz do projeto
            project_root = Path(__file__).parent.parent.parent.parent
            yaml_path = project_root / "data" / "mappings" / "alias.yaml"

        if not yaml_path.exists():
            logger.warning(
                f"Arquivo alias.yaml nao encontrado em {yaml_path}. "
                "Usando apenas schema SQL para deteccao de tipos."
            )
            return

        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            # Carrega classificacao de tipos
            column_types_config = config.get("column_types", {})

            for type_name in ["numeric", "categorical", "temporal"]:
                columns = column_types_config.get(type_name, [])
                self.column_types[type_name] = set(columns)

            logger.info(
                f"Configuracao de tipos carregada: "
                f"{len(self.column_types['numeric'])} numericas, "
                f"{len(self.column_types['categorical'])} categoricas, "
                f"{len(self.column_types['temporal'])} temporais"
            )

        except Exception as e:
            logger.error(f"Erro ao carregar alias.yaml: {e}")

    def _detect_type_from_config(self, column_name: str) -> Optional[str]:
        """
        Detecta o tipo da coluna baseado na configuracao do alias.yaml.

        Args:
            column_name: Nome da coluna

        Returns:
            "numeric", "categorical", "temporal" ou None se nao encontrado
        """
        for type_name, columns in self.column_types.items():
            if column_name in columns:
                return type_name
        return None

    def _detect_type_from_schema(
        self, column_name: str, schema: Dict[str, str]
    ) -> Optional[str]:
        """
        Detecta o tipo da coluna baseado no schema SQL (fallback).

        Args:
            column_name: Nome da coluna
            schema: Mapeamento coluna -> tipo SQL

        Returns:
            "numeric", "categorical", "temporal" ou None
        """
        sql_type = schema.get(column_name, "").upper()

        if sql_type in self.QUANTITATIVE_SQL_TYPES:
            return "numeric"
        elif sql_type in self.CATEGORICAL_SQL_TYPES:
            return "categorical"
        elif sql_type in self.TEMPORAL_SQL_TYPES:
            return "temporal"

        return None

    def get_column_type(
        self, column_name: str, schema: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Determina o tipo de uma coluna.

        Prioridade:
        1. Virtual metrics from alias.yaml (COUNT(*) aggregations)
        2. Configuracao do alias.yaml
        3. Schema SQL (fallback)
        4. Assume categorical como padrao seguro

        Args:
            column_name: Nome da coluna
            schema: Schema SQL opcional para fallback

        Returns:
            "numeric", "categorical", "temporal" ou "virtual"
        """
        # Prioridade 0: Virtual metrics (e.g. 'Numero de Clientes')
        try:
            from src.shared_lib.core.config import load_alias_data

            alias_data = load_alias_data()
            virtual_metrics = set(alias_data.get("metrics", {}).keys())
            if column_name in virtual_metrics:
                logger.debug(
                    f"Coluna '{column_name}' identificada como metrica virtual (COUNT(*))"
                )
                return "virtual"
        except Exception:
            pass

        # Prioridade 1: Configuracao explicita
        type_from_config = self._detect_type_from_config(column_name)
        if type_from_config:
            logger.debug(
                f"Coluna '{column_name}' identificada como '{type_from_config}' via alias.yaml"
            )
            return type_from_config

        # Prioridade 2: Schema SQL
        if schema:
            type_from_schema = self._detect_type_from_schema(column_name, schema)
            if type_from_schema:
                logger.debug(
                    f"Coluna '{column_name}' identificada como '{type_from_schema}' "
                    f"via schema SQL ({schema.get(column_name)})"
                )
                return type_from_schema

        # Fallback: Assume categorical (mais seguro)
        logger.warning(
            f"Tipo da coluna '{column_name}' nao identificado. "
            "Assumindo 'categorical' como padrao."
        )
        return "categorical"

    def select_aggregation(
        self,
        column_name: str,
        schema: Optional[Dict[str, str]] = None,
        user_specified: Optional[str] = None,
        context: Optional[str] = None,
    ) -> str:
        """
        Seleciona a funcao de agregacao apropriada para uma coluna.

        Prioridade de Decisao (CORRIGIDA):
        1. Determinar tipo da coluna PRIMEIRO
        2. Validar se agregacao especificada e compativel com o tipo
        3. Corrigir agregacoes incompativeis (ex: COUNT em numerica -> SUM)
        4. Preservar agregacoes intencionais validas (AVG, MIN, MAX)

        Args:
            column_name: Nome da coluna
            schema: Schema SQL opcional
            user_specified: Agregacao especificada pelo usuario ou pelo classifier
            context: Contexto da query para decisoes contextuais

        Returns:
            Funcao de agregacao: "sum", "count", "count_distinct", etc.
        """
        # PASSO 1: Determina tipo da coluna PRIMEIRO
        column_type = self.get_column_type(column_name, schema)

        # PASSO 1.5: Virtual metrics always use COUNT
        if column_type == "virtual":
            logger.debug(
                f"Metrica virtual '{column_name}' -> COUNT (resolves to COUNT(*))"
            )
            return "count"

        # PASSO 2: Define agregacao padrao baseada no tipo
        if column_type == "numeric":
            default_aggregation = "sum"
        elif column_type in ["categorical", "temporal"]:
            default_aggregation = "count"
        else:
            default_aggregation = "count"  # Fallback seguro

        # PASSO 3: Valida agregacao especificada
        if user_specified:
            user_agg_lower = user_specified.lower()

            # Agregacoes claramente intencionais que devem ser preservadas
            intentional_aggregations = {"avg", "min", "max", "median", "std", "var"}

            # Se e uma agregacao intencional E valida para o tipo da coluna
            if user_agg_lower in intentional_aggregations:
                # Para colunas numericas, todas essas sao validas
                if column_type == "numeric":
                    logger.info(
                        f"Preservando agregacao intencional '{user_agg_lower.upper()}' "
                        f"para coluna numerica '{column_name}'"
                    )
                    return user_agg_lower

                # Para categoricas/temporais, essas agregacoes sao invalidas
                else:
                    logger.warning(
                        f"Agregacao '{user_agg_lower.upper()}' invalida para coluna "
                        f"{column_type} '{column_name}'. Corrigindo para {default_aggregation.upper()}"
                    )
                    return default_aggregation

            # COUNT DISTINCT e sempre valido
            if user_agg_lower == "count_distinct":
                logger.info(
                    f"Preservando COUNT DISTINCT para '{column_name}' ({column_type})"
                )
                return user_agg_lower

            # CORRECAO PRINCIPAL: COUNT em coluna numerica -> SUM
            if user_agg_lower == "count" and column_type == "numeric":
                logger.warning(
                    f"CORRECAO: Coluna numerica '{column_name}' com COUNT especificado. "
                    f"Corrigindo para SUM (agregacao apropriada para valores quantitativos)"
                )
                return "sum"

            # SUM em coluna categorica/temporal -> COUNT
            if user_agg_lower == "sum" and column_type in ["categorical", "temporal"]:
                logger.warning(
                    f"CORRECAO: Coluna {column_type} '{column_name}' com SUM especificado. "
                    f"Corrigindo para COUNT (agregacao apropriada para valores qualitativos)"
                )
                return "count"

            # Se chegou aqui, a agregacao especificada e compativel
            if user_agg_lower in ["sum", "count"]:
                logger.debug(
                    f"Agregacao '{user_agg_lower.upper()}' compativel com coluna "
                    f"{column_type} '{column_name}'"
                )
                return user_agg_lower

        # PASSO 4: Usa agregacao padrao baseada no tipo
        logger.info(
            f"Usando agregacao padrao para '{column_name}' ({column_type}): "
            f"{default_aggregation.upper()}"
        )
        return default_aggregation

    def is_numeric_column(
        self, column_name: str, schema: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Verifica se uma coluna e numerica.

        Args:
            column_name: Nome da coluna
            schema: Schema SQL opcional

        Returns:
            True se a coluna for numerica
        """
        return self.get_column_type(column_name, schema) == "numeric"

    def is_categorical_column(
        self, column_name: str, schema: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Verifica se uma coluna e categorica.

        Args:
            column_name: Nome da coluna
            schema: Schema SQL opcional

        Returns:
            True se a coluna for categorica
        """
        return self.get_column_type(column_name, schema) == "categorical"

    def is_temporal_column(
        self, column_name: str, schema: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Verifica se uma coluna e temporal.

        Args:
            column_name: Nome da coluna
            schema: Schema SQL opcional

        Returns:
            True se a coluna for temporal
        """
        return self.get_column_type(column_name, schema) == "temporal"
