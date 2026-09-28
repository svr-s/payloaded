"""payloaded.

A Python package to transform flat tabular data into nested,
hierarchical API payloads with grouping and reconciliation.
"""

from payloaded.audit import AuditReport, ReconciliationError, reconcile
from payloaded.builder import PayloadBuilder, build_payloads, summarize
from payloaded.expressions import (
    CompiledFormula,
    FormulaError,
    FormulaEvaluationError,
    FormulaSecurityError,
    GeneratorContext,
)
from payloaded.models import ConditionConfig, EntityConfig, FieldMapping, PayloadConfig

__version__ = "0.2.0"

__all__ = [
    "__version__",
    "build_payloads",
    "summarize",
    "PayloadBuilder",
    "reconcile",
    "AuditReport",
    "ReconciliationError",
    "PayloadConfig",
    "ConditionConfig",
    "EntityConfig",
    "FieldMapping",
    "CompiledFormula",
    "GeneratorContext",
    "FormulaError",
    "FormulaSecurityError",
    "FormulaEvaluationError",
]

