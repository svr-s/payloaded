"""Template parsing and rendering engine for payloaded."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd

from payloaded.compat import is_null_or_nan
from payloaded.models import EntityConfig, FieldMapping, PayloadConfig

PLACEHOLDER_REGEX = re.compile(r"\{([a-zA-Z0-9_\-\.]+)\}")


def _clean_scalar_value(val: Any) -> Any:
    """Handle NA / None and convert numpy/pandas scalars to native Python types."""
    if is_null_or_nan(val):
        return None
    if isinstance(val, (list, dict)):
        return val
    if hasattr(val, "item"):
        return val.item()
    return val


def _render_value(template_val: Any, record: Dict[str, Any], mappings_by_key: Dict[str, FieldMapping]) -> Any:
    """Substitute placeholders in a template value using the current record data.

    If the template value is strictly a single placeholder like '{item_qty}',
    the raw typed value (e.g. int/float) is preserved rather than stringified.
    """
    if not isinstance(template_val, str):
        return template_val

    # Check if template_val is exactly a single placeholder '{key}'
    exact_match = re.fullmatch(r"\{([a-zA-Z0-9_\-\.]+)\}", template_val.strip())
    if exact_match:
        key = exact_match.group(1)
        raw_val = record.get(key)
        mapping = mappings_by_key.get(key)
        if raw_val is None and mapping and mapping.default is not None:
            raw_val = mapping.default
        return _clean_scalar_value(raw_val)

    # String with embedded placeholders (e.g. 'Order #{order_no}')
    def _replace_match(match: re.Match) -> str:
        key = match.group(1)
        val = record.get(key)
        mapping = mappings_by_key.get(key)
        if val is None and mapping and mapping.default is not None:
            val = mapping.default
        if pd.isna(val) or val is None:
            return ""
        return str(val)

    return PLACEHOLDER_REGEX.sub(_replace_match, template_val)


class PayloadTemplate:
    """Encapsulates the JSON payload structure skeleton and rendering rules."""

    def __init__(self, raw_structure: Union[dict, list, str, Path]):
        """Initialize PayloadTemplate from a dict, list, JSON string, or file path."""
        self.raw_template = self._parse_raw(raw_structure)
        self.is_root_list = isinstance(self.raw_template, list)

    @staticmethod
    def _parse_raw(source: Union[dict, list, str, Path]) -> Union[dict, list]:
        """Parse source template into python dict or list."""
        if isinstance(source, (dict, list)):
            return copy.deepcopy(source)

        if isinstance(source, str):
            try:
                return json.loads(source)
            except json.JSONDecodeError as err:
                raise ValueError(f"Invalid JSON template string: {err}") from err

        raise TypeError(f"Unsupported template type: {type(source).__name__}. Template must be a dict, list, or JSON string.")

    def get_template_clone(self) -> Union[dict, list]:
        """Return a deep copy of the raw template."""
        return copy.deepcopy(self.raw_template)


def render_envelope(
    envelope: Any,
    payload_batch: Any,
    record: Optional[Dict[str, Any]] = None,
    mappings_by_key: Optional[Dict[str, FieldMapping]] = None,
) -> Any:
    """Inject a generated payload batch into a wrapper envelope structure.

    Replaces '{payload_template}' or '{payload}' placeholder with payload_batch,
    and substitutes any header/metadata placeholders using record.
    """
    if envelope is None:
        return payload_batch

    if isinstance(envelope, str):
        try:
            parsed = json.loads(envelope)
            return render_envelope(parsed, payload_batch, record, mappings_by_key)
        except Exception:
            pass

    mappings = mappings_by_key or {}
    rec = record or {}

    def _traverse(node: Any) -> Any:
        if isinstance(node, dict):
            return {k: _traverse(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [_traverse(item) for item in node]
        elif isinstance(node, str):
            stripped = node.strip()
            if stripped in ("{payload_template}", "{payload}"):
                return payload_batch
            # Render placeholders if present
            if "{" in node and "}" in node and rec:
                return _render_value(node, rec, mappings)
            return node
        return node

    return _traverse(envelope)
