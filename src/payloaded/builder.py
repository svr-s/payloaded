"""Main orchestration pipeline and user-facing API for payloaded."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from payloaded.audit import AuditReport, reconcile
from payloaded.compat import (
    compute_meta_summary,
    create_output_dataframe,
    get_pandas,
    get_polars,
    is_pandas_df,
    is_polars_df,
    polars_to_pandas_safe,
    slice_dataframe_chunks,
)
from payloaded.loader import load_sources
from payloaded.models import PayloadConfig
from payloaded.router import ConditionRouter
from payloaded.template import PayloadTemplate

OUTPUT_COLUMNS = [
    "index",
    "condition_rule",
    "condition_value",
    "rows_in_payload",
    "running_total",
    "payload",
    "source_filename",
]


class PayloadBuilder:
    """Builder class for transforming tabular datasets into nested, conditional API payloads.

    Supports both pandas and polars DataFrames natively.

    Example:
        >>> builder = PayloadBuilder(config=config)
        >>> df_payloads = builder.build(df)
    """

    def __init__(
        self,
        config: Union[dict, PayloadConfig],
        template: Optional[Union[dict, list, str]] = None,
        output_format: str = "json_string",
        indent: Optional[int] = None,
    ):
        """Initialize PayloadBuilder.

        Args:
            config: Canonical configuration dict or PayloadConfig.
            template: Optional default JSON template if not specified directly inside conditions.
            output_format: 'json_string' (default, ready for Postman/HTTP) or 'dict'.
            indent: Optional indentation for JSON serialization (e.g. 2 for pretty-printed).
        """
        if output_format not in ("json_string", "dict"):
            raise ValueError(f"output_format must be 'json_string' or 'dict', got: {output_format}")

        if isinstance(config, PayloadConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = PayloadConfig.from_dict(config, default_template=template)
        else:
            raise TypeError(
                f"Unsupported config type: {type(config).__name__}. "
                "Config must be an in-memory dict or PayloadConfig object."
            )

        self.template = template
        self.output_format = output_format
        self.indent = indent
        self.router = ConditionRouter(self.config, default_template=template)

    def _get_top_level_group_keys(self) -> List[str]:
        """Collect top-level group_by keys across conditions to prevent severing parent groups during chunking."""
        group_keys: List[str] = []
        for cond in self.config.conditions:
            for ent in cond.entities:
                if ent.path == "root" and ent.group_by:
                    for g in ent.group_by:
                        if g not in group_keys:
                            group_keys.append(g)
        return group_keys

    def stream(
        self,
        source: Any,
        chunksize: Optional[int] = None,
    ) -> Iterator[Any]:
        """Stream chunks of payloads as mini-DataFrames matching the input source type (pandas or polars).

        Args:
            source: pandas.DataFrame, polars.DataFrame, or CSV filepath / sources list.
            chunksize: Number of source rows per chunk. If None, processes each source file as a chunk.

        Yields:
            DataFrames of 7 canonical columns matching the source type.
        """
        source_pairs = load_sources(source)
        group_keys = self._get_top_level_group_keys()
        global_index = 1

        for filename, raw_df in source_pairs:
            # Determine if incoming source is polars
            source_is_polars = is_polars_df(raw_df)
            target_type = "polars" if source_is_polars else "pandas"

            file_running_total = 0

            # Determine chunk iterator
            if chunksize is not None and chunksize > 0:
                chunks = slice_dataframe_chunks(raw_df, chunksize, group_columns=group_keys)
            else:
                chunks = [raw_df]

            for chunk_df in chunks:
                # Convert chunk to pandas for the router engine if needed
                if is_polars_df(chunk_df):
                    engine_df = polars_to_pandas_safe(chunk_df)
                else:
                    engine_df = chunk_df

                payload_tuples = self.router.process_dataframe(engine_df)
                chunk_records: List[Dict[str, Any]] = []

                for payload_data, rows_in_payload, cond_rule, cond_val in payload_tuples:
                    file_running_total += rows_in_payload

                    if self.output_format == "json_string":
                        formatted_payload = json.dumps(
                            payload_data,
                            ensure_ascii=False,
                            indent=self.indent,
                        )
                    else:
                        formatted_payload = payload_data

                    chunk_records.append({
                        "index": global_index,
                        "condition_rule": cond_rule,
                        "condition_value": cond_val,
                        "rows_in_payload": rows_in_payload,
                        "running_total": file_running_total,
                        "payload": formatted_payload,
                        "source_filename": filename,
                    })
                    global_index += 1

                yield create_output_dataframe(chunk_records, OUTPUT_COLUMNS, target_type=target_type)

    def build(
        self,
        source: Any,
        chunksize: Optional[int] = None,
    ) -> Any:
        """Process source dataset(s) and produce an auditable output DataFrame.

        If source is a polars DataFrame, returns a polars DataFrame.
        If source is a pandas DataFrame, returns a pandas DataFrame.
        If chunksize is provided, processes data in slices and concatenates the result.
        """
        source_is_polars = is_polars_df(source)
        target_type = "polars" if source_is_polars else "pandas"

        if chunksize is not None and chunksize > 0:
            chunk_dfs = list(self.stream(source, chunksize=chunksize))
            if not chunk_dfs:
                return create_output_dataframe([], OUTPUT_COLUMNS, target_type=target_type)

            if target_type == "polars":
                pl = get_polars()
                return pl.concat(chunk_dfs)
            else:
                pd = get_pandas()
                return pd.concat(chunk_dfs, ignore_index=True)

        # Standard non-chunked execution
        source_pairs = load_sources(source)
        records: List[Dict[str, Any]] = []
        global_index = 1

        for filename, raw_df in source_pairs:
            source_is_polars = is_polars_df(raw_df)
            target_type = "polars" if source_is_polars else "pandas"

            if source_is_polars:
                engine_df = polars_to_pandas_safe(raw_df)
            else:
                engine_df = raw_df

            file_running_total = 0
            payload_tuples = self.router.process_dataframe(engine_df)

            for payload_data, rows_in_payload, cond_rule, cond_val in payload_tuples:
                file_running_total += rows_in_payload

                if self.output_format == "json_string":
                    formatted_payload = json.dumps(
                        payload_data,
                        ensure_ascii=False,
                        indent=self.indent,
                    )
                else:
                    formatted_payload = payload_data

                records.append({
                    "index": global_index,
                    "condition_rule": cond_rule,
                    "condition_value": cond_val,
                    "rows_in_payload": rows_in_payload,
                    "running_total": file_running_total,
                    "payload": formatted_payload,
                    "source_filename": filename,
                })
                global_index += 1

        return create_output_dataframe(records, OUTPUT_COLUMNS, target_type=target_type)


def summarize(payloads_df: Any) -> Any:
    """Compute an aggregated summary DataFrame from a payloads DataFrame.

    Aggregates by ('condition_rule', 'condition_value') with metrics:
    - payload_count: total payloads produced for this condition
    - total_rows: total tabular source rows packed
    - avg_rows_per_payload: average rows per payload batch
    - min_rows: minimum batch size
    - max_rows: maximum batch size

    Includes an 'ALL / TOTAL' row at the end.
    Returns a pandas.DataFrame or polars.DataFrame matching the input type.
    """
    return compute_meta_summary(payloads_df)


def build_payloads(
    source: Any,
    config: Optional[Union[dict, PayloadConfig]] = None,
    template: Optional[Union[dict, list, str]] = None,
    output_format: str = "json_string",
    indent: Optional[int] = None,
    chunksize: Optional[int] = None,
    stream: bool = False,
    **kwargs: Any,
) -> Any:
    """Transform tabular source data into nested, batch-chunked API payloads.

    Supports both pandas.DataFrame and polars.DataFrame. The returned output type
    matches the input type automatically.

    Args:
        source: Single pandas or polars DataFrame, or list of DataFrames.
        config: Canonical configuration dictionary or PayloadConfig.
        template: Optional default payload template if not specified in config conditions.
        output_format: 'json_string' (default, ready for Postman) or 'dict'.
        indent: Optional indentation for JSON serialization.
        chunksize: Optional row count to chunk source processing in batches.
        stream: If True, yields chunk DataFrames as an iterator rather than returning the tuple.

    Returns:
        Tuple[DataFrame, DataFrame]: (payloads_df, meta_df) containing payloads and reconciliation summary,
        or Iterator[DataFrame] if stream=True.
    """
    actual_config = config
    actual_template = template

    if actual_config is not None and isinstance(actual_config, (dict, list)):
        is_config_dict = (
            isinstance(actual_config, dict)
            and ("conditions" in actual_config or "entities" in actual_config or "condition_source_key" in actual_config)
        )
        if not is_config_dict and actual_template is not None:
            actual_template, actual_config = actual_config, actual_template

    if actual_config is None:
        raise ValueError("A configuration must be provided via 'config'.")

    builder = PayloadBuilder(
        config=actual_config,
        template=actual_template,
        output_format=output_format,
        indent=indent,
    )

    if stream:
        return builder.stream(source, chunksize=chunksize)

    payloads_df = builder.build(source, chunksize=chunksize)
    meta_df = compute_meta_summary(payloads_df)
    return payloads_df, meta_df

