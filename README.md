# payloaded

[![PyPI version](https://img.shields.io/pypi/v/payloaded.svg)](https://pypi.org/project/payloaded/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**`payloaded`** (`import payloaded as pld`) transforms flat tabular data (CSVs, DataFrames) into nested, hierarchical JSON payloads for REST/GraphQL APIs with conditional routing, multi-level batch chunking, and mathematical row reconciliation.

---

## Key Features

- **Conditional Routing & Negation Rules**: Route records to distinct payload structures using positive matches (`['New', 'Update']`) or negation rules (`['~Terminated']`).
- **Condition Value Partitioning**: Records matching a rule are partitioned by their actual status values so disparate states are never mixed in the same payload.
- **Consistent Canonical Configuration**: Single, uniform configuration layout for both conditional and unconditional jobs.
- **Column Name & Index Flexibility**: Map source columns using either column name strings (whitespace-tolerant) or 0-based integer column indices.
- **Multi-Level Hierarchy & Grouping**: Group rows across parent, child, and grandchild entities (`root`, `orders`, `orders.line_items`) using single or composite keys.
- **Cascading Batch Limits**: Enforce maximum item limits at any level of the hierarchy (e.g. max 50 orders, max 100 line items).
- **Audit & Zero-Loss Reconciliation**: Generates an auditable DataFrame containing payload indices, rules, partitioned values, row counts, running totals, and Postman-ready JSON payloads.

---

## Installation

```bash
pip install payloaded
```

---

## Canonical Configuration Schema

`payloaded` uses a single, consistent configuration format:

```python
config = {
    "condition_source_key": "Status",  # Column name, 0-based column index, or "" if unconditional
    "conditions": [
        {
            "condition_rule": ["~Terminated"],  # Rule list, or [] if unconditional
            "payload_template": [ ... ],         # Payload JSON structure skeleton
            "entities": [ ... ]                  # Hierarchy, grouping, and field mappings
        }
    ]
}
```

---

## Quickstart

### 1. Conditional Routing Example

```python
import pandas as pd
import payloaded as pld

# Flat tabular dataset with status column
df_source = pd.DataFrame([
    {"Batch_ID": "B01", "Status": "New", "Order_ID": "ORD_1", "SKU": "A1", "Qty": 2},
    {"Batch_ID": "B01", "Status": "New", "Order_ID": "ORD_2", "SKU": "A2", "Qty": 1},
    {"Batch_ID": "B01", "Status": "Update", "Order_ID": "ORD_3", "SKU": "B1", "Qty": 5},
    {"Batch_ID": "B01", "Status": "Terminated", "Order_ID": "ORD_4", "SKU": "C1", "Qty": 0},
])

# Canonical configuration with conditional routing & negation
config = {
    "condition_source_key": "Status",
    "conditions": [
        {
            # Matches all active statuses (~Terminated)
            # Automatically partitions 'New' and 'Update' into separate payloads
            "condition_rule": ["~Terminated"],
            "payload_template": [
                {
                    "batch_id": "{batch_id}",
                    "orders": [
                        {
                            "order_id": "{order_id}",
                            "items": [{"sku": "{sku}", "qty": "{qty}"}]
                        }
                    ]
                }
            ],
            "entities": [
                {
                    "path": "root",
                    "repeat_limit": 10,
                    "group_by": ["batch_id"],
                    "mappings": [{"payload_key": "batch_id", "source_key": "Batch_ID"}],
                },
                {
                    "path": "orders",
                    "repeat_limit": 50,
                    "group_by": ["order_id"],
                    "mappings": [{"payload_key": "order_id", "source_key": "Order_ID"}],
                },
                {
                    "path": "orders.items",
                    "repeat_limit": 100,
                    "mappings": [
                        {"payload_key": "sku", "source_key": "SKU"},
                        {"payload_key": "qty", "source_key": "Qty", "type_cast": "int"},
                    ],
                },
            ],
        },
        {
            # Specialized payload structure for terminations
            "condition_rule": ["Terminated"],
            "payload_template": [
                {
                    "batch_id": "{batch_id}",
                    "cancellations": [{"order_id": "{order_id}"}]
                }
            ],
            "entities": [
                {
                    "path": "root",
                    "group_by": ["batch_id"],
                    "mappings": [{"payload_key": "batch_id", "source_key": "Batch_ID"}],
                },
                {
                    "path": "cancellations",
                    "group_by": ["order_id"],
                    "mappings": [{"payload_key": "order_id", "source_key": "Order_ID"}],
                },
            ],
        },
    ]
}

# Generate auditable payloads DataFrame
df_payloads = pld.build_payloads(
    source=df_source,
    config=config,
    output_format="json_string"  # or "dict"
)

# Verify zero data loss with mathematical reconciliation
audit_report = pld.reconcile(df_payloads, expected_rows=len(df_source))
print(audit_report.summary())
```

---

### 2. Unconditional Example

When all records follow the same structure unconditionally, set `condition_source_key: ""` and `condition_rule: []`:

```python
config = {
    "condition_source_key": "",
    "conditions": [
        {
            "condition_rule": [],
            "payload_template": [{"id": "{id}", "name": "{name}"}],
            "entities": [
                {
                    "path": "root",
                    "mappings": [
                        {"payload_key": "id", "source_key": "ID"},
                        {"payload_key": "name", "source_key": "Name"},
                    ],
                }
            ],
        }
    ],
}
```

---

## Output DataFrame Structure

The resulting DataFrame contains 7 canonical columns for end-to-end traceability and Postman/API dispatch:

| Column | Type | Description |
|---|---|---|
| `index` | `int` | Sequential 1-based payload identifier |
| `condition_rule` | `str` | Matching condition rule expression (e.g. `~Terminated`) |
| `condition_value` | `str` | Actual partitioned condition value (e.g. `New`, `Update`) |
| `rows_in_payload` | `int` | Exact count of source tabular rows packed into this payload |
| `running_total` | `int` | Cumulative row count within the current source file |
| `payload` | `str` / `dict` | Complete, nested JSON payload string or Python dictionary |
| `source_filename` | `str` | Origin filename for multi-file traceability |

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

