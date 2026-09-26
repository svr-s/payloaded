"""Core hierarchical grouping, chunking, and hydration engine."""

from __future__ import annotations

import copy
import math
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import pandas as pd

from payloaded.expressions import GeneratorContext
from payloaded.models import EntityConfig, FieldMapping, PayloadConfig
from payloaded.template import PayloadTemplate, _render_value


def _is_blank(v: Any) -> bool:
    """Check if a value is null, NaN, empty string, or whitespace-only."""
    if v is None:
        return True
    if pd.isna(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


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


def _extract_wildcard_tokens(entity: EntityConfig, df_columns: List[Any]) -> List[str]:
    """Scan DataFrame columns against wildcard source_keys in entity mappings and return sorted tokens.

    Supports patterns like 'orderid*' or 'order_*_id'. Tokens are naturally sorted so
    that numeric tokens like '2', '3', '10' sort in correct ascending numerical order.
    """
    str_cols = [str(c).strip() for c in df_columns]
    tokens_set: Set[str] = set()

    for m in entity.mappings:
        if isinstance(m.source_key, str) and "*" in m.source_key:
            pattern_str = m.source_key.strip()
            # Escape regex special characters except '*'
            escaped = re.escape(pattern_str).replace(r"\*", r"(.*)")
            regex = re.compile(rf"^{escaped}$", re.IGNORECASE)
            for col in str_cols:
                match = regex.match(col)
                if match:
                    tokens_set.add(match.group(1))

    if not tokens_set:
        return []

    # Sort naturally: blank string first, then integer if numeric, else string
    def _sort_key(t: str) -> Tuple[int, Union[int, str]]:
        if t == "":
            return (0, 0)
        clean = t.lstrip("_- ")
        if clean.isdigit():
            return (1, int(clean))
        if t.isdigit():
            return (1, int(t))
        return (2, t.lower())

    return sorted(tokens_set, key=_sort_key)


def _resolve_wildcard_record(
    entity: EntityConfig,
    base_record: Dict[Any, Any],
    token: str,
    df_columns: List[Any],
) -> Dict[str, Any]:
    """Construct an unrolled row dictionary for a specific wildcard token.

    Resolves wildcards in source_key (e.g. 'orderid*' with token '2' -> 'orderid2').
    If a column does not exist in the source DataFrame (e.g. missing 'ordername3'),
    it gracefully sets the value to None without raising errors.
    """
    record: Dict[str, Any] = dict(base_record)
    for m in entity.mappings:
        if isinstance(m.source_key, str) and "*" in m.source_key:
            col_name = m.source_key.strip().replace("*", token)
            matched_col = _match_column_name(col_name, df_columns)
            if matched_col is not None and matched_col in base_record:
                record[m.payload_key] = base_record[matched_col]
            else:
                record[m.payload_key] = m.default
        elif m.source_key is not None and str(m.source_key).strip() != "":
            matched_col = _match_column_name(m.source_key, df_columns)
            if matched_col is not None and matched_col in base_record:
                record[m.payload_key] = base_record[matched_col]
            else:
                record[m.payload_key] = m.default
    return record


def _is_unrolled_item_empty(rendered_dict: dict, entity: EntityConfig) -> bool:
    """Check if all values in an unrolled item are blank/empty or if any required fields are missing."""
    # If the dictionary is completely empty (all omitted)
    if not rendered_dict:
        return True

    # Check if every scalar value is blank
    has_meaningful_value = False
    for k, v in rendered_dict.items():
        if isinstance(v, (dict, list)):
            if len(v) > 0:
                has_meaningful_value = True
                break
        elif not _is_blank(v):
            has_meaningful_value = True
            break

    return not has_meaningful_value


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
    """Manages multi-level data hierarchy, grouping, batch chunking, and formula evaluation."""

    def __init__(
        self,
        entities: Union[List[EntityConfig], PayloadConfig],
        template: PayloadTemplate,
        gen_context: Optional[GeneratorContext] = None,
    ):
        if isinstance(entities, PayloadConfig):
            self.entities = entities.entities
        else:
            self.entities = entities

        self.template = template
        self.gen_context = gen_context or GeneratorContext()
        self.mappings_by_payload_key: Dict[str, FieldMapping] = {}
        self.resolved_source_col: Dict[str, Optional[str]] = {}
        for entity in self.entities:
            for m in entity.mappings:
                self.mappings_by_payload_key[m.payload_key] = m

    def get_entity(self, path: str) -> Optional[EntityConfig]:
        """Look up an EntityConfig by its path."""
        norm = "root" if path in ("", "$", "[root]") else path
        for entity in self.entities:
            if entity.normalized_path == norm:
                return entity
        return None

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
        for entity in self.entities:
            for m in entity.mappings:
                if m.formula:
                    if m.compiled_formula:
                        for ref_col in m.compiled_formula.referenced_columns:
                            matched = _match_column_name(ref_col, df_cols)
                            if matched is None:
                                raise KeyError(
                                    f"Column '{ref_col}' referenced in formula '{m.formula}' "
                                    f"was not found in source columns: {df_cols}"
                                )
                    self.resolved_source_col[m.payload_key] = None
                    continue

                if m.source_key is None or str(m.source_key).strip() == "":
                    self.resolved_source_col[m.payload_key] = None
                    continue

                if isinstance(m.source_key, str) and "*" in m.source_key:
                    # Wildcard mappings are dynamically resolved per token during hydration
                    self.resolved_source_col[m.payload_key] = None
                    continue

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
        root_entity = self.get_entity("root")
        root_limit = root_entity.repeat_limit if root_entity else None

        # Build entity tree hierarchy starting from root
        # 1. Identify child entities
        child_entities = [e for e in self.entities if e.normalized_path != "root"]

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
        root_record = group_df.iloc[0].to_dict()
        root_group_cols = []
        if root_entity and root_entity.group_by:
            root_group_cols = [
                _resolve_column(k, list(group_df.columns), self.mappings_by_payload_key)
                for k in root_entity.group_by
            ]
        if root_group_cols:
            root_id = "_".join(str(root_record.get(col, "")) for col in root_group_cols)
        else:
            root_id = "root"

        # Find direct child entities whose path is a top-level key in elem_template
        direct_children = [e for e in child_entities if "." not in e.path]

        if not direct_children:
            # Single root element with all rows in group_df
            rendered = self._hydrate_dict(elem_template, root_record, root_entity, parent_id="root")
            return [(rendered, len(group_df))]

        # Handle direct children (e.g. orders)
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
                c_items = self._process_child_element(
                    sub_df, child_elem_template, primary_child, grandchildren, parent_id=root_id
                )
                child_items.extend(c_items)
        else:
            c_items = self._process_child_element(
                group_df, child_elem_template, primary_child, grandchildren, parent_id=root_id
            )
            child_items.extend(c_items)

        # Chunk child items by primary_child.repeat_limit
        child_chunks = _split_into_chunks(child_items, primary_child.repeat_limit)

        # For each child chunk, create a clone of the root element
        results: List[Tuple[dict, int]] = []

        for chunk in child_chunks:
            root_copy = copy.deepcopy(elem_template)
            # Hydrate root scalars
            hydrated_root = self._hydrate_dict(root_copy, root_record, root_entity, parent_id="root")
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
        parent_id: str = "root",
    ) -> List[Tuple[dict, int]]:
        """Process a child element (e.g. an order) and its grandchildren (e.g. line_items)."""
        child_record = sub_df.iloc[0].to_dict()
        child_group_cols = []
        if child_entity.group_by:
            child_group_cols = [
                _resolve_column(k, list(sub_df.columns), self.mappings_by_payload_key)
                for k in child_entity.group_by
            ]
        if child_group_cols:
            child_id = "_".join(str(child_record.get(col, "")) for col in child_group_cols)
        else:
            child_id = f"{child_entity.path}_{id(sub_df)}"

        if not grandchildren:
            # Check if this child entity uses wildcard unpivot
            if child_entity.has_wildcard_mappings:
                tokens = _extract_wildcard_tokens(child_entity, list(sub_df.columns))
                items: List[Tuple[dict, int]] = []
                for _, row in sub_df.iterrows():
                    base_rec = row.to_dict()
                    for token in tokens:
                        token_rec = _resolve_wildcard_record(child_entity, base_rec, token, list(sub_df.columns))
                        rendered = self._hydrate_dict(child_elem_template, token_rec, child_entity, parent_id=parent_id)
                        if not _is_unrolled_item_empty(rendered, child_entity):
                            items.append((rendered, 1))
                return items

            # Leaf level: if child entity does not group, each row is a distinct item
            if not child_entity.group_by and len(sub_df) > 1:
                items: List[Tuple[dict, int]] = []
                for _, row in sub_df.iterrows():
                    record = row.to_dict()
                    rendered = self._hydrate_dict(child_elem_template, record, child_entity, parent_id=parent_id)
                    items.append((rendered, 1))
                return items
            else:
                rendered = self._hydrate_dict(child_elem_template, child_record, child_entity, parent_id=parent_id)
                return [(rendered, len(sub_df))]

        primary_grandchild = grandchildren[0]
        # Relative path under child: e.g. "orders.line_items" -> "line_items"
        gc_key = primary_grandchild.path.split(".", 1)[1]

        gc_template_list = child_elem_template.get(gc_key, [{}])
        gc_elem_template = gc_template_list[0] if isinstance(gc_template_list, list) and gc_template_list else {}

        # Grandchild items
        gc_items: List[Tuple[dict, int]] = []
        if primary_grandchild.has_wildcard_mappings:
            tokens = _extract_wildcard_tokens(primary_grandchild, list(sub_df.columns))
            for _, row in sub_df.iterrows():
                base_rec = row.to_dict()
                for token in tokens:
                    token_rec = _resolve_wildcard_record(primary_grandchild, base_rec, token, list(sub_df.columns))
                    rendered_gc = self._hydrate_dict(
                        gc_elem_template, token_rec, primary_grandchild, parent_id=child_id
                    )
                    if not _is_unrolled_item_empty(rendered_gc, primary_grandchild):
                        gc_items.append((rendered_gc, 1))
        else:
            gc_group_cols = []
            if primary_grandchild.group_by:
                gc_group_cols = [
                    _resolve_column(k, list(sub_df.columns), self.mappings_by_payload_key)
                    for k in primary_grandchild.group_by
                ]

            if gc_group_cols:
                grouped = sub_df.groupby(gc_group_cols, sort=False, dropna=False)
                for _, gc_df in grouped:
                    row_record = gc_df.iloc[0].to_dict()
                    rendered_gc = self._hydrate_dict(
                        gc_elem_template, row_record, primary_grandchild, parent_id=child_id
                    )
                    gc_items.append((rendered_gc, len(gc_df)))
            else:
                # Every row is a grandchild item
                for _, row in sub_df.iterrows():
                    row_record = row.to_dict()
                    rendered_gc = self._hydrate_dict(
                        gc_elem_template, row_record, primary_grandchild, parent_id=child_id
                    )
                    gc_items.append((rendered_gc, 1))

        # Chunk grandchildren by primary_grandchild.repeat_limit
        gc_chunks = _split_into_chunks(gc_items, primary_grandchild.repeat_limit)

        results: List[Tuple[dict, int]] = []

        for chunk in gc_chunks:
            child_copy = copy.deepcopy(child_elem_template)
            hydrated_child = self._hydrate_dict(
                child_copy, child_record, child_entity, parent_id=parent_id
            )
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
            if root_entity and root_entity.has_wildcard_mappings:
                tokens = _extract_wildcard_tokens(root_entity, list(df.columns))
                for _, row in df.iterrows():
                    base_rec = row.to_dict()
                    for token in tokens:
                        token_rec = _resolve_wildcard_record(root_entity, base_rec, token, list(df.columns))
                        rendered = self._hydrate_dict(elem_template, token_rec, root_entity, parent_id="root")
                        if not _is_unrolled_item_empty(rendered, root_entity):
                            items.append((rendered, 1))
            else:
                for _, row in df.iterrows():
                    rendered = self._hydrate_dict(elem_template, row.to_dict(), root_entity, parent_id="root")
                    items.append((rendered, 1))
            chunks = _split_into_chunks(items, root_limit)
            for chunk in chunks:
                results.append(([item[0] for item in chunk], len(chunk)))
            return results
        else:
            # Single object per row
            if root_entity and root_entity.has_wildcard_mappings:
                tokens = _extract_wildcard_tokens(root_entity, list(df.columns))
                for _, row in df.iterrows():
                    base_rec = row.to_dict()
                    for token in tokens:
                        token_rec = _resolve_wildcard_record(root_entity, base_rec, token, list(df.columns))
                        rendered = self._hydrate_dict(elem_template, token_rec, root_entity, parent_id="root")
                        if not _is_unrolled_item_empty(rendered, root_entity):
                            results.append((rendered, 1))
                return results
            else:
                for _, row in df.iterrows():
                    rendered = self._hydrate_dict(elem_template, row.to_dict(), root_entity, parent_id="root")
                    results.append((rendered, 1))
                return results

    def _hydrate_dict(
        self,
        template_obj: dict,
        source_record: dict,
        entity: Optional[EntityConfig],
        parent_id: str = "root",
        payload_index: int = 0,
    ) -> dict:
        """Recursively hydrate scalar values in a template dictionary."""
        # Create a lookup mapping for this entity or global
        lookup: Dict[str, Any] = {}
        if entity:
            for m in entity.mappings:
                if m.formula and m.compiled_formula:
                    val = m.compiled_formula.evaluate(
                        source_record,
                        gen_context=self.gen_context,
                        parent_id=parent_id,
                        payload_index=payload_index,
                    )
                    lookup[m.payload_key] = val
                else:
                    matched_col = self.resolved_source_col.get(m.payload_key)
                    if matched_col is not None and matched_col in source_record:
                        val = source_record[matched_col]
                        lookup[m.payload_key] = m.default if (pd.isna(val) and m.default is not None) else val
                    elif m.payload_key in source_record:
                        val = source_record[m.payload_key]
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
                hydrated[key] = self._hydrate_dict(val, source_record, entity, parent_id, payload_index)
            elif isinstance(val, list):
                # Sub-arrays are populated during hierarchical processing
                hydrated[key] = copy.deepcopy(val)
            else:
                rendered_val = _render_value(val, lookup, self.mappings_by_payload_key)

                # Check if this placeholder has omit_if_blank enabled
                exact_match = re.fullmatch(r"\{([a-zA-Z0-9_\-\.]+)\}", str(val).strip())
                mapping = self.mappings_by_payload_key.get(exact_match.group(1)) if exact_match else None
                if mapping is None and entity:
                    for m in entity.mappings:
                        if m.payload_key == key:
                            mapping = m
                            break

                if mapping and mapping.omit_if_blank and _is_blank(rendered_val):
                    continue

                hydrated[key] = rendered_val
        return hydrated

