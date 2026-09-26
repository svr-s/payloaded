"""Main orchestration pipeline and user-facing API for payloaded."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from payloaded.audit import AuditReport, reconcile
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

    Example:
        >>> builder = PayloadBuilder(config=config)
        >>> df_payloads = builder.build("data/*.csv")
    """

    def __init__(
        self,
        config: Union[dict, str, Path, PayloadConfig],
        template: Optional[Union[dict, list, str, Path]] = None,
        output_format: str = "json_string",
        indent: Optional[int] = None,
    ):
        """Initialize PayloadBuilder.

        Args:
            config: Canonical configuration dict, path to .json/.yaml config file, or PayloadConfig.
            template: Optional default JSON template if not specified directly inside conditions.
            output_format: 'json_string' (default, ready for Postman/HTTP) or 'dict'.
            indent: Optional indentation for JSON serialization (e.g. 2 for pretty-printed).
        """
        if output_format not in ("json_string", "dict"):
            raise ValueError(f"output_format must be 'json_string' or 'dict', got: {output_format}")

        if isinstance(config, PayloadConfig):
            self.config = config
        elif isinstance(config, (str, Path)):
            self.config = PayloadConfig.from_file(config, default_template=template)
        elif isinstance(config, dict):
            self.config = PayloadConfig.from_dict(config, default_template=template)
        else:
            raise TypeError(f"Unsupported config type: {type(config)}")

        self.template = template
        self.output_format = output_format
        self.indent = indent
        self.router = ConditionRouter(self.config, default_template=template)

    def build(
        self,
        source: Union[str, Path, pd.DataFrame, List[Union[str, Path, pd.DataFrame]]],
    ) -> pd.DataFrame:
        """Process source dataset(s) and produce an auditable output DataFrame.

        Returns DataFrame with 7 standard columns:
            ['index', 'condition_rule', 'condition_value', 'rows_in_payload',
             'running_total', 'payload', 'source_filename']
        """
        source_pairs = load_sources(source)
        records: List[Dict[str, Any]] = []

        global_index = 1

        for filename, df in source_pairs:
            file_running_total = 0
            payload_tuples = self.router.process_dataframe(df)

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

        output_df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
        return output_df


def build_payloads(
    source: Union[str, Path, pd.DataFrame, List[Union[str, Path, pd.DataFrame]]],
    config: Optional[Union[dict, str, Path, PayloadConfig]] = None,
    template: Optional[Union[dict, list, str, Path]] = None,
    output_format: str = "json_string",
    indent: Optional[int] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Transform tabular source data into nested, batch-chunked API payloads.

    Args:
        source: Single DataFrame, CSV filepath, glob pattern, or list of sources.
        config: Canonical configuration dictionary, file path, or PayloadConfig.
        template: Optional default payload template if not specified in config conditions.
        output_format: 'json_string' (default, ready for Postman) or 'dict'.
        indent: Optional indentation for JSON serialization.

    Returns:
        DataFrame with columns:
        ['index', 'condition_rule', 'condition_value', 'rows_in_payload',
         'running_total', 'payload', 'source_filename'].
    """
    # Backward compatibility: handle positional call as build_payloads(source, template, config)
    actual_config = config
    actual_template = template

    if actual_config is not None and isinstance(actual_config, (dict, list)):
        is_config_dict = (
            isinstance(actual_config, dict)
            and ("conditions" in actual_config or "entities" in actual_config or "condition_source_key" in actual_config)
        )
        if not is_config_dict and actual_template is not None:
            # Positions were swapped: build_payloads(source, template, config)
            actual_template, actual_config = actual_config, actual_template

    if actual_config is None:
        raise ValueError("A configuration must be provided via 'config'.")

    builder = PayloadBuilder(
        config=actual_config,
        template=actual_template,
        output_format=output_format,
        indent=indent,
    )
    return builder.build(source)
