"""Data models and configuration schemas for payloaded."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
import json
from pathlib import Path


@dataclass
class FieldMapping:
    """Represents a mapping between a payload placeholder and a source column.

    Attributes:
        payload_key: The placeholder name inside the JSON template (e.g. 'batch_id' for '{batch_id}').
        source_key: Column name (str) or 0-based column index (int) in the source tabular data.
        default: Optional fallback value if the column value is null or missing.
        type_cast: Optional type name ('int', 'float', 'str', 'bool') for explicit casting.
    """

    payload_key: str
    source_key: Union[str, int]
    default: Optional[Any] = None
    type_cast: Optional[str] = None

    @property
    def file_key(self) -> str:
        """Backward-compatibility alias for source_key."""
        return str(self.source_key)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> FieldMapping:
        """Create a FieldMapping instance from a raw dictionary with whitespace tolerance."""
        payload_raw = data.get("payload_key", "")
        payload_key = str(payload_raw).strip().strip("{}").strip()

        # Support 'source_key' (preferred) or 'file_key' (backward compatibility)
        source_raw = data.get("source_key", data.get("file_key", ""))
        if isinstance(source_raw, int):
            source_key: Union[str, int] = source_raw
        elif isinstance(source_raw, str):
            source_key = source_raw.strip()
        else:
            source_key = str(source_raw).strip()

        return cls(
            payload_key=payload_key,
            source_key=source_key,
            default=data.get("default"),
            type_cast=data.get("type_cast"),
        )


@dataclass
class EntityConfig:
    """Configuration for a specific hierarchical entity / level in the payload structure.

    Attributes:
        path: Path in the payload hierarchy ('root', '$', 'orders', 'orders.line_items').
        repeat_limit: Maximum repetitions of this entity allowed per parent (e.g. max 50).
        group_by: List of keys (payload_key or source_key) that identify uniqueness for grouping.
        mappings: List of field mappings between payload placeholders and source columns.
    """

    path: str
    repeat_limit: Optional[int] = None
    group_by: List[str] = field(default_factory=list)
    mappings: List[FieldMapping] = field(default_factory=list)

    @property
    def normalized_path(self) -> str:
        """Return standardized path where '$' or '' are normalized to 'root'."""
        p = self.path.strip()
        if p in ("", "$", "[root]"):
            return "root"
        return p

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EntityConfig:
        """Create an EntityConfig instance from a raw dictionary, deduplicating mappings."""
        path = str(data.get("path", "root")).strip()
        repeat_limit = data.get("repeat_limit")
        if repeat_limit is not None:
            repeat_limit = int(repeat_limit)

        group_by_raw = data.get("group_by", [])
        if isinstance(group_by_raw, (str, int)):
            group_by = [str(group_by_raw).strip()]
        else:
            group_by = [str(k).strip() for k in group_by_raw if str(k).strip()]

        mappings_raw = data.get("mappings", [])
        raw_mappings = [FieldMapping.from_dict(m) for m in mappings_raw]

        # In case of duplicate payload_key or source_key, retain only the first occurrence
        deduped_mappings: List[FieldMapping] = []
        seen_payload_keys = set()
        seen_source_keys = set()

        for m in raw_mappings:
            if m.payload_key and m.payload_key in seen_payload_keys:
                continue
            if m.source_key != "" and m.source_key in seen_source_keys:
                continue

            if m.payload_key:
                seen_payload_keys.add(m.payload_key)
            if m.source_key != "":
                seen_source_keys.add(m.source_key)

            deduped_mappings.append(m)

        return cls(
            path=path,
            repeat_limit=repeat_limit,
            group_by=group_by,
            mappings=deduped_mappings,
        )


@dataclass
class ConditionConfig:
    """Represents a specific routing condition with its own template and entities.

    Attributes:
        condition_rule: List of matching rules (e.g. ['~Terminated'] or ['New', 'Update']).
                        Empty list indicates an unconditional match.
        payload_template: The payload template structure (dict, list, str, Path).
        entities: List of EntityConfig objects defining hierarchy and mappings.
    """

    condition_rule: List[str] = field(default_factory=list)
    payload_template: Any = None
    entities: List[EntityConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], default_template: Any = None) -> ConditionConfig:
        """Parse ConditionConfig from dictionary with whitespace stripping on rules."""
        raw_rule = data.get("condition_rule", [])
        if isinstance(raw_rule, (str, int)):
            rule_str = str(raw_rule).strip()
            condition_rule = [rule_str] if rule_str else []
        elif isinstance(raw_rule, (list, tuple)):
            condition_rule = [str(r).strip() for r in raw_rule if str(r).strip()]
        else:
            condition_rule = []

        template = data.get("payload_template", default_template)
        raw_entities = data.get("entities", [])
        entities = [EntityConfig.from_dict(e) for e in raw_entities]

        return cls(
            condition_rule=condition_rule,
            payload_template=template,
            entities=entities,
        )


@dataclass
class PayloadConfig:
    """Top-level configuration holding conditions or standalone entities.

    Attributes:
        condition_source_key: Column name (str) or 0-based index (int) determining conditions.
                              Empty string indicates unconditional execution.
        conditions: List of ConditionConfig objects.
    """

    condition_source_key: Union[str, int] = ""
    conditions: List[ConditionConfig] = field(default_factory=list)

    @property
    def entities(self) -> List[EntityConfig]:
        """Convenience accessor to the first condition's entities for backward compatibility."""
        if self.conditions:
            return self.conditions[0].entities
        return []

    def get_entity(self, path: str) -> Optional[EntityConfig]:
        """Look up an EntityConfig by its path in the primary condition."""
        if not self.conditions:
            return None
        norm = "root" if path in ("", "$", "[root]") else path
        for entity in self.conditions[0].entities:
            if entity.normalized_path == norm:
                return entity
        return None

    @classmethod
    def from_dict(cls, data: Dict[str, Any], default_template: Any = None) -> PayloadConfig:
        """Instantiate PayloadConfig from dictionary, handling both canonical and legacy formats."""
        raw_key = data.get("condition_source_key", data.get("source_key", ""))
        if isinstance(raw_key, int):
            condition_source_key: Union[str, int] = raw_key
        else:
            condition_source_key = str(raw_key).strip()

        conditions: List[ConditionConfig] = []
        if "conditions" in data and isinstance(data["conditions"], (list, tuple)):
            for cond_data in data["conditions"]:
                conditions.append(ConditionConfig.from_dict(cond_data, default_template=default_template))
        elif "entities" in data:
            # Unconditional mode via legacy flat structure
            template = data.get("payload_template", default_template)
            raw_entities = data.get("entities", [])
            entities = [EntityConfig.from_dict(e) for e in raw_entities]
            conditions.append(ConditionConfig(
                condition_rule=[],
                payload_template=template,
                entities=entities,
            ))

        return cls(
            condition_source_key=condition_source_key,
            conditions=conditions,
        )

    @classmethod
    def from_file(cls, filepath: Union[str, Path], default_template: Any = None) -> PayloadConfig:
        """Load configuration from a JSON or YAML file."""
        p = Path(filepath)
        if not p.is_file():
            raise FileNotFoundError(f"Configuration file not found: {filepath}")

        content = p.read_text(encoding="utf-8")
        if p.suffix.lower() in (".yaml", ".yml"):
            try:
                import yaml  # type: ignore
                data = yaml.safe_load(content)
            except ImportError:
                raise ImportError(
                    "PyYAML is required to parse .yaml/.yml config files. "
                    "Install it or use a JSON config file."
                )
        else:
            data = json.loads(content)

        return cls.from_dict(data, default_template=default_template)
