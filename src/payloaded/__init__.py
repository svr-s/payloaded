"""payloaded.

A Python package to transform flat tabular data into nested,
hierarchical API payloads with grouping and reconciliation.
"""

from payloaded.audit import AuditReport, ReconciliationError, reconcile
from payloaded.builder import PayloadBuilder, build_payloads
from payloaded.models import ConditionConfig, EntityConfig, FieldMapping, PayloadConfig

__version__ = "0.0.1"

__all__ = [
    "__version__",
    "build_payloads",
    "PayloadBuilder",
    "reconcile",
    "AuditReport",
    "ReconciliationError",
    "PayloadConfig",
    "ConditionConfig",
    "EntityConfig",
    "FieldMapping",
]
