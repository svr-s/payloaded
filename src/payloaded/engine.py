"""Core hierarchical grouping, chunking, and hydration engine."""

from __future__ import annotations

import copy
import math
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd

from payloaded.models import EntityConfig, FieldMapping, PayloadConfig
from payloaded.template import PayloadTemplate, _render_value


def _match_column_name(target: Union[str, int], df_columns: List[Any]) -> Optional[Any]:
    """Match a column by exact name, integer index, or whitespace-stripped name."""
    # 1. Exact match in df_columns
    if target in df_columns:
        return target

    # 2. If target is an integer index pointing to position in df_columns
    if isinstance(target, int) and 0 <= target < len(df_columns):
        return df_columns[target]

    target_str = str(target).strip()

    # 3. Stripped string match against stringified column names
    for col in df_columns:
        if str(col).strip() == target_str:
            return col

    # 4. If target_str is digits (e.g. "0"), check if it indexes into df_columns
    if target_str.isdigit():
        idx = int(target_str)
        if 0 <= idx < len(df_columns):
            return df_columns[idx]

    return None


def _resolve_column(key: str, df_columns: List[str], mappings_by_payload_key: Dict[str, FieldMapping]) -> str:
    """Resolve a group_by key (payload_key or source_key/index) into an actual DataFrame column name."""
    cleaned_key = str(key).strip()

    # 1. Primary: check if cleaned_key is a payload_key
    if cleaned_key in mappings_by_payload_key:
        source_key = mappings_by_payload_key[cleaned_key].source_key
        matched = _match_column_name(source_key, df_columns)
        if matched is not None:
            return matched

    # 2. Fallback: check if cleaned_key directly matches a DataFrame column or index
    matched = _match_column_name(cleaned_key, df_columns)
    if matched is not None:
        return matched

    raise KeyError(
        f"Grouping key '{key}' could not be resolved in DataFrame columns: {df_columns}"
    )


def _split_into_chunks(items: List[Any], chunk_size: Optional[int]) -> List[List[Any]]:
    """Split a list of items into chunks of max chunk_size.

    If chunk_size is None or <= 0, returns the items in a single chunk.
    """
    if not items:
        return []
    if chunk_size is None or chunk_size <= 0:
        return [items]
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


class HierarchyEngine:
    """Manages multi-level data hierarchy, grouping, and batch chunking."""

    def __init__(self, config: PayloadConfig, template: PayloadTemplate):
        self.config = config
        self.template = template
        self.mappings_by_payload_key: Dict[str, FieldMapping] = {}
        self.resolved_source_col: Dict[str, Optional[str]] = {}
        for entity in self.config.entities:
            for m in entity.mappings:
                self.mappings_by_payload_key[m.payload_key] = m

    def process_dataframe(self, df: pd.DataFrame) -> List[Tuple[Any, int]]:
        """Transform flat DataFrame into a list of (payload_data, row_count) tuples.

        Each tuple represents one complete payload ready for JSON serialization
        along with the exact count of source rows accounted for in that payload.
        """
        if df.empty:
            return []

        # Validate and resolve mapped columns in df
        df_cols = list(df.columns)
        self.resolved_source_col = {}
        for entity in self.config.entities:
            for m in entity.mappings:
                matched = _match_column_name(m.source_key, df_cols)
                if matched is None and m.default is None:
                    raise KeyError(
                        f"Mapped source column '{m.source_key}' (for payload key '{m.payload_key}') "
                        f"was not found in source columns: {df_cols}"
                    )
                self.resolved_source_col[m.payload_key] = matched

        raw_template = self.template.get_template_clone()
        return self._generate_payloads(df, raw_template)

    def _generate_payloads(self, df: pd.DataFrame, raw_template: Union[dict, list]) -> List[Tuple[Any, int]]:
        """Orchestrate multi-level grouping and chunking against the template."""
        is_root_list = isinstance(raw_template, list)
        root_entity = self.config.get_entity("root")
        root_limit = root_entity.repeat_limit if root_entity else None

        # Build entity tree hierarchy starting from root
        # 1. Identify child entities
        child_entities = [e for e in self.config.entities if e.normalized_path != "root"]

        # If there are no child entities or simple flat payload:
        if not child_entities:
            return self._process_flat(df, raw_template, root_entity, is_root_list)

        # Multi-level processing:
        # Determine root group keys
        root_group_cols = []
        if root_entity and root_entity.group_by:
            root_group_cols = [
                _resolve_column(k, list(df.columns), self.mappings_by_payload_key)
                for k in root_entity.group_by
            ]

        # Template prototype for each root element
        root_elem_template = raw_template[0] if is_root_list else raw_template

        # Generate root items
        root_items: List[Tuple[Any, int]] = []
        if root_group_cols:
            grouped = df.groupby(root_group_cols, sort=False, dropna=False)
            for _, group_df in grouped:
                items = self._process_root_element(group_df, root_elem_template, root_entity, child_entities)
                root_items.extend(items)
        else:
            items = self._process_root_element(df, root_elem_template, root_entity, child_entities)
            root_items.extend(items)

        # Now chunk root items into payloads
        if is_root_list:
            chunked_batches = _split_into_chunks(root_items, root_limit)
            payloads: List[Tuple[Any, int]] = []
            for batch in chunked_batches:
                batch_data = [item[0] for item in batch]
                batch_rows = sum(item[1] for item in batch)
                payloads.append((batch_data, batch_rows))
            return payloads
        else:
            # Root is an object: each root item becomes its own payload
            return [(item[0], item[1]) for item in root_items]

    def _process_root_element(
        self,
        group_df: pd.DataFrame,
        elem_template: dict,
        root_entity: Optional[EntityConfig],
        child_entities: List[EntityConfig],
    ) -> List[Tuple[dict, int]]:
        """Process a single root element (e.g. one batch) down through its nested children."""
        # Find direct child entities whose path is a top-level key in elem_template
        # Example: path == "orders"
        direct_children = [e for e in child_entities if "." not in e.path]

        if not direct_children:
            # Single root element with all rows in group_df
            rendered = self._hydrate_dict(elem_template, group_df.iloc[0].to_dict(), root_entity)
            return [(rendered, len(group_df))]

        # Handle direct children (e.g. orders)
        # Note: In standard payloads, there is typically one primary repeating array at Level 1
        primary_child = direct_children[0]
        child_path = primary_child.path  # e.g. "orders"
        child_template_list = elem_template.get(child_path, [{}])
        child_elem_template = child_template_list[0] if isinstance(child_template_list, list) and child_template_list else {}

        # Grandchildren under this child
        grandchild_prefix = f"{child_path}."
        grandchildren = [e for e in child_entities if e.path.startswith(grandchild_prefix)]

        # Group by child's group_by keys
        child_group_cols = []
        if primary_child.group_by:
            child_group_cols = [
                _resolve_column(k, list(group_df.columns), self.mappings_by_payload_key)
                for k in primary_child.group_by
            ]

        # Generate child items
        child_items: List[Tuple[dict, int]] = []
        if child_group_cols:
            grouped = group_df.groupby(child_group_cols, sort=False, dropna=False)
            for _, sub_df in grouped:
                c_items = self._process_child_element(sub_df, child_elem_template, primary_child, grandchildren)
                child_items.extend(c_items)
        else:
            c_items = self._process_child_element(group_df, child_elem_template, primary_child, grandchildren)
            child_items.extend(c_items)

        # Chunk child items by primary_child.repeat_limit
        child_chunks = _split_into_chunks(child_items, primary_child.repeat_limit)

        # For each child chunk, create a clone of the root element
        results: List[Tuple[dict, int]] = []
        root_record = group_df.iloc[0].to_dict()

        for chunk in child_chunks:
            root_copy = copy.deepcopy(elem_template)
            # Hydrate root scalars
            hydrated_root = self._hydrate_dict(root_copy, root_record, root_entity)
            # Inject chunk of child items
            hydrated_root[child_path] = [item[0] for item in chunk]
            chunk_rows = sum(item[1] for item in chunk)
            results.append((hydrated_root, chunk_rows))

        return results

    def _process_child_element(
        self,
        sub_df: pd.DataFrame,
        child_elem_template: dict,
        child_entity: EntityConfig,
        grandchildren: List[EntityConfig],
    ) -> List[Tuple[dict, int]]:
        """Process a child element (e.g. an order) and its grandchildren (e.g. line_items)."""
        if not grandchildren:
            # Leaf level
            record = sub_df.iloc[0].to_dict()
            rendered = self._hydrate_dict(child_elem_template, record, child_entity)
            return [(rendered, len(sub_df))]

        primary_grandchild = grandchildren[0]
        # Relative path under child: e.g. "orders.line_items" -> "line_items"
        gc_key = primary_grandchild.path.split(".", 1)[1]

        gc_template_list = child_elem_template.get(gc_key, [{}])
        gc_elem_template = gc_template_list[0] if isinstance(gc_template_list, list) and gc_template_list else {}

        # Grandchild items
        gc_group_cols = []
        if primary_grandchild.group_by:
            gc_group_cols = [
                _resolve_column(k, list(sub_df.columns), self.mappings_by_payload_key)
                for k in primary_grandchild.group_by
            ]

        gc_items: List[Tuple[dict, int]] = []
        if gc_group_cols:
            grouped = sub_df.groupby(gc_group_cols, sort=False, dropna=False)
            for _, gc_df in grouped:
                row_record = gc_df.iloc[0].to_dict()
                rendered_gc = self._hydrate_dict(gc_elem_template, row_record, primary_grandchild)
                gc_items.append((rendered_gc, len(gc_df)))
        else:
            # Every row is a grandchild item
            for _, row in sub_df.iterrows():
                row_record = row.to_dict()
                rendered_gc = self._hydrate_dict(gc_elem_template, row_record, primary_grandchild)
                gc_items.append((rendered_gc, 1))

        # Chunk grandchildren by primary_grandchild.repeat_limit
        gc_chunks = _split_into_chunks(gc_items, primary_grandchild.repeat_limit)

        results: List[Tuple[dict, int]] = []
        child_record = sub_df.iloc[0].to_dict()

        for chunk in gc_chunks:
            child_copy = copy.deepcopy(child_elem_template)
            hydrated_child = self._hydrate_dict(child_copy, child_record, child_entity)
            hydrated_child[gc_key] = [item[0] for item in chunk]
            chunk_rows = sum(item[1] for item in chunk)
            results.append((hydrated_child, chunk_rows))

        return results

    def _process_flat(
        self,
        df: pd.DataFrame,
        raw_template: Union[dict, list],
        root_entity: Optional[EntityConfig],
        is_root_list: bool,
    ) -> List[Tuple[Any, int]]:
        """Handle simple flat datasets without sub-arrays."""
        elem_template = raw_template[0] if is_root_list else raw_template
        results: List[Tuple[Any, int]] = []

        if is_root_list:
            root_limit = root_entity.repeat_limit if root_entity else None
            items: List[Tuple[dict, int]] = []
            for _, row in df.iterrows():
                rendered = self._hydrate_dict(elem_template, row.to_dict(), root_entity)
                items.append((rendered, 1))
            chunks = _split_into_chunks(items, root_limit)
            for chunk in chunks:
                results.append(([item[0] for item in chunk], len(chunk)))
            return results
        else:
            # Single object per row
            for _, row in df.iterrows():
                rendered = self._hydrate_dict(elem_template, row.to_dict(), root_entity)
                results.append((rendered, 1))
            return results

    def _hydrate_dict(
        self,
        template_obj: dict,
        source_record: dict,
        entity: Optional[EntityConfig],
    ) -> dict:
        """Recursively hydrate scalar values in a template dictionary."""
        # Create a lookup mapping for this entity or global
        lookup: Dict[str, Any] = {}
        if entity:
            for m in entity.mappings:
                matched_col = self.resolved_source_col.get(m.payload_key)
                if matched_col is not None and matched_col in source_record:
                    val = source_record[matched_col]
                    lookup[m.payload_key] = m.default if (pd.isna(val) and m.default is not None) else val
                else:
                    lookup[m.payload_key] = m.default
        # Fallback to any matching key in source_record
        for k, v in source_record.items():
            if k not in lookup:
                lookup[k] = v

        hydrated = {}
        for key, val in template_obj.items():
            if isinstance(val, dict):
                hydrated[key] = self._hydrate_dict(val, source_record, entity)
            elif isinstance(val, list):
                # Sub-arrays are populated during hierarchical processing
                hydrated[key] = copy.deepcopy(val)
            else:
                hydrated[key] = _render_value(val, lookup, self.mappings_by_payload_key)
        return hydrated
