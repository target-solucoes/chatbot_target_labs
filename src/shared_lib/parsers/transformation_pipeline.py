"""
Chart Spec Transformation Pipeline - Modular and Testable

Este módulo implementa um pipeline configurável de transformações
para ChartOutput, separando cada transformação em etapas independentes.

Conforme especificado em planning_graphical_correction.md - Fase 3.3:
- Pipeline com etapas independentes e componíveis
- Logs estruturados por etapa
- Facilidade para isolar e testar transformações

Referência: planning_graphical_correction.md - Fase 3.3
"""

import logging
import time
from typing import Dict, Any, Callable, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class TransformationResult:
    """Resultado de uma transformação aplicada."""

    step_name: str
    success: bool
    spec: Dict[str, Any]
    duration_ms: float
    changes_made: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class TransformationStep:
    """
    Uma etapa de transformação no pipeline.

    Encapsula uma função de transformação com nome e metadados.
    """

    def __init__(
        self,
        name: str,
        func: Callable[[Dict[str, Any]], Dict[str, Any]],
        description: Optional[str] = None,
    ):
        """
        Inicializa uma etapa de transformação.

        Args:
            name: Nome identificador da etapa
            func: Função que aplica a transformação
            description: Descrição opcional do que a etapa faz
        """
        self.name = name
        self.func = func
        self.description = description or f"Apply {name} transformation"

    def apply(self, spec: Dict[str, Any]) -> TransformationResult:
        """
        Aplica a transformação ao spec.

        Args:
            spec: Spec a transformar

        Returns:
            TransformationResult com resultado e metadados
        """
        logger.info(f"[TransformationStep] Starting: {self.name}")
        logger.debug(f"[TransformationStep] {self.description}")

        start_time = time.perf_counter()

        try:
            # Aplicar transformação
            transformed_spec = self.func(spec)

            end_time = time.perf_counter()
            duration_ms = (end_time - start_time) * 1000

            # Detectar mudanças (comparação simples)
            changes = self._detect_changes(spec, transformed_spec)

            result = TransformationResult(
                step_name=self.name,
                success=True,
                spec=transformed_spec,
                duration_ms=duration_ms,
                changes_made=changes,
            )

            logger.info(
                f"[TransformationStep] Completed: {self.name} "
                f"({duration_ms:.2f}ms, {len(changes)} changes)"
            )

            if changes:
                logger.debug(f"[TransformationStep] Changes: {changes}")

            return result

        except Exception as e:
            end_time = time.perf_counter()
            duration_ms = (end_time - start_time) * 1000

            logger.error(
                f"[TransformationStep] Failed: {self.name} ({duration_ms:.2f}ms) - {e}"
            )

            result = TransformationResult(
                step_name=self.name,
                success=False,
                spec=spec,  # Retornar spec original
                duration_ms=duration_ms,
                errors=[str(e)],
            )

            return result

    def _detect_changes(
        self, before: Dict[str, Any], after: Dict[str, Any]
    ) -> List[str]:
        """
        Detecta mudanças entre dois specs (comparação simplificada).

        Args:
            before: Spec antes da transformação
            after: Spec depois da transformação

        Returns:
            Lista de descrições de mudanças
        """
        changes = []

        # Comparar campos top-level
        before_keys = set(before.keys())
        after_keys = set(after.keys())

        # Campos adicionados
        added_keys = after_keys - before_keys
        for key in added_keys:
            changes.append(f"Added field: {key}")

        # Campos removidos
        removed_keys = before_keys - after_keys
        for key in removed_keys:
            changes.append(f"Removed field: {key}")

        # Campos modificados
        common_keys = before_keys & after_keys
        for key in common_keys:
            before_value = before[key]
            after_value = after[key]

            # Comparação simplificada (não profunda)
            if before_value != after_value:
                # Para listas/dicts, apenas indicar mudança
                if isinstance(after_value, (list, dict)):
                    if isinstance(after_value, list):
                        before_len = (
                            len(before_value) if isinstance(before_value, list) else 0
                        )
                        after_len = len(after_value)
                        if before_len != after_len:
                            changes.append(
                                f"Modified {key}: length {before_len} -> {after_len}"
                            )
                        else:
                            changes.append(f"Modified {key}: content changed")
                    else:
                        changes.append(f"Modified {key}")
                else:
                    changes.append(f"Modified {key}: {before_value} -> {after_value}")

        return changes


class ChartSpecTransformationPipeline:
    """
    Pipeline configurável de transformações para ChartOutput.

    Permite adicionar etapas de transformação dinamicamente e
    executá-las sequencialmente com logging detalhado.
    """

    def __init__(self, name: str = "default"):
        """
        Inicializa o pipeline.

        Args:
            name: Nome identificador do pipeline
        """
        self.name = name
        self.steps: List[TransformationStep] = []
        self.execution_history: List[TransformationResult] = []

    def add_step(
        self,
        name: str,
        func: Callable[[Dict[str, Any]], Dict[str, Any]],
        description: Optional[str] = None,
    ) -> "ChartSpecTransformationPipeline":
        """
        Adiciona uma etapa ao pipeline.

        Args:
            name: Nome da etapa
            func: Função de transformação
            description: Descrição opcional

        Returns:
            Self (para chaining)
        """
        step = TransformationStep(name, func, description)
        self.steps.append(step)

        logger.debug(f"[Pipeline:{self.name}] Added step: {name}")

        return self

    def transform(
        self, spec: Dict[str, Any], stop_on_error: bool = False
    ) -> Dict[str, Any]:
        """
        Aplica todas as transformações sequencialmente.

        Args:
            spec: Spec inicial a transformar
            stop_on_error: Se True, para no primeiro erro; se False, continua

        Returns:
            Spec transformado após todas as etapas

        Raises:
            RuntimeError: Se stop_on_error=True e uma etapa falhar
        """
        logger.info(
            f"[TransformationPipeline:{self.name}] Starting pipeline with {len(self.steps)} steps"
        )

        start_time = time.perf_counter()
        result_spec = spec
        self.execution_history = []

        for i, step in enumerate(self.steps, 1):
            logger.info(
                f"[Pipeline:{self.name}] Step {i}/{len(self.steps)}: {step.name}"
            )

            try:
                step_result = step.apply(result_spec)
                self.execution_history.append(step_result)

                if not step_result.success:
                    if stop_on_error:
                        raise RuntimeError(
                            f"Step '{step.name}' failed: {step_result.errors}"
                        )
                    else:
                        logger.warning(
                            f"[Pipeline:{self.name}] Step '{step.name}' failed but continuing"
                        )
                        # Não atualizar result_spec se etapa falhou
                        continue

                # Atualizar spec com resultado da etapa
                result_spec = step_result.spec

            except Exception as e:
                logger.error(
                    f"[Pipeline:{self.name}] Unexpected error in step '{step.name}': {e}"
                )

                if stop_on_error:
                    raise
                else:
                    logger.warning(f"[Pipeline:{self.name}] Continuing after error")

        end_time = time.perf_counter()
        total_duration_ms = (end_time - start_time) * 1000

        # Estatísticas finais
        successful_steps = sum(1 for r in self.execution_history if r.success)
        failed_steps = len(self.execution_history) - successful_steps
        total_changes = sum(len(r.changes_made) for r in self.execution_history)

        logger.info(
            f"[TransformationPipeline:{self.name}] Pipeline completed: "
            f"{successful_steps}/{len(self.steps)} successful, "
            f"{failed_steps} failed, "
            f"{total_changes} total changes, "
            f"{total_duration_ms:.2f}ms total"
        )

        return result_spec

    def get_execution_summary(self) -> Dict[str, Any]:
        """
        Retorna sumário da última execução do pipeline.

        Returns:
            Dict com estatísticas e resultados de cada etapa
        """
        if not self.execution_history:
            return {
                "pipeline_name": self.name,
                "steps_executed": 0,
                "message": "No execution history available",
            }

        return {
            "pipeline_name": self.name,
            "steps_executed": len(self.execution_history),
            "successful_steps": sum(1 for r in self.execution_history if r.success),
            "failed_steps": sum(1 for r in self.execution_history if not r.success),
            "total_duration_ms": sum(r.duration_ms for r in self.execution_history),
            "total_changes": sum(len(r.changes_made) for r in self.execution_history),
            "steps": [
                {
                    "name": r.step_name,
                    "success": r.success,
                    "duration_ms": r.duration_ms,
                    "changes": len(r.changes_made),
                    "warnings": len(r.warnings),
                    "errors": len(r.errors),
                }
                for r in self.execution_history
            ],
        }

    def clear(self) -> None:
        """Remove todas as etapas do pipeline."""
        self.steps = []
        self.execution_history = []
        logger.debug(f"[Pipeline:{self.name}] Cleared all steps")
