"""Data models and configuration schemas for payloaded."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
import json
from pathlib import Path


@dataclass
class FieldMapping:
    """Represents a mapping between a payload placeholder and a source file column.

    Attributes:
        payload_key: The placeholder name inside the JSON template (e.g. 'batch_id' for '{batch_id}').
        file_key: The column name in the source tabular data / DataFrame.
        default: Optional fallback value if the column value is null or missing.
        type_cast: Optional type name ('int', 'float', 'str', 'bool') for explicit casting.
    """

    payload_key: str
    file_key: str
    default: Optional[Any] = None
    type_cast: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> FieldMapping:
        """Create a FieldMapping instance from a raw dictionary."""
        return cls(
            payload_key=str(data.get("payload_key", "")).strip("{}"),
            file_key=str(data.get("file_key", "")),
            default=data.get("default"),
            type_cast=data.get("type_cast"),
        )


@dataclass
class EntityConfig:
    """Configuration for a specific hierarchical entity / level in the payload structure.

    Attributes:
        path: Path in the payload hierarchy ('root', '$', 'orders', 'orders.line_items').
        repeat_limit: Maximum repetitions of this entity allowed per parent (e.g. max 50).
        group_by: List of keys (file_key or payload_key) that identify uniqueness for grouping.
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
        """Create an EntityConfig instance from a raw dictionary."""
        path = data.get("path", "root")
        repeat_limit = data.get("repeat_limit")
        if repeat_limit is not None:
            repeat_limit = int(repeat_limit)

        group_by_raw = data.get("group_by", [])
        if isinstance(group_by_raw, str):
            group_by = [group_by_raw]
        else:
            group_by = [str(k) for k in group_by_raw]

        mappings_raw = data.get("mappings", [])
        mappings = [FieldMapping.from_dict(m) for m in mappings_raw]

        return cls(
            path=path,
            repeat_limit=repeat_limit,
            group_by=group_by,
            mappings=mappings,
        )


@dataclass
class PayloadConfig:
    """Top-level configuration holding all entity configurations.

    Attributes:
        entities: List of EntityConfig objects defining the hierarchy.
    """

    entities: List[EntityConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PayloadConfig:
        """Instantiate PayloadConfig from a dictionary."""
        raw_entities = data.get("entities", [])
        entities = [EntityConfig.from_dict(e) for e in raw_entities]
        return cls(entities=entities)

    @classmethod
    def from_file(cls, filepath: Union[str, Path]) -> PayloadConfig:
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

        return cls.from_dict(data)

    def get_entity(self, path: str) -> Optional[EntityConfig]:
        """Look up an EntityConfig by its path."""
        norm = "root" if path in ("", "$", "[root]") else path
        for entity in self.entities:
            if entity.normalized_path == norm:
                return entity
        return None
