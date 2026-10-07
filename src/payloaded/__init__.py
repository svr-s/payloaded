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
from payloaded.template import PayloadTemplate, render_envelope
from payloaded.models import ConditionConfig, EntityConfig, FieldMapping, PayloadConfig

from payloaded import testing
from payloaded.testing import (
    assert_meta_balanced,
    assert_node_counts,
    assert_payload_schema,
    assert_reconciled,
)

__version__ = "0.2.6"

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
    "PayloadTemplate",
    "render_envelope",
    "testing",
    "assert_payload_schema",
    "assert_reconciled",
    "assert_node_counts",
    "assert_meta_balanced",
]

