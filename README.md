# payloaded

[![PyPI version](https://img.shields.io/pypi/v/payloaded.svg)](https://pypi.org/project/payloaded/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**`payloaded`** transforms flat tabular data (CSVs, DataFrames) into nested, hierarchical JSON payloads for REST/GraphQL APIs with grouping, batch chunking, and mathematical row reconciliation.

---

## Key Features

- **Template-Driven Payloads**: Map tabular columns directly to placeholders in complex, nested JSON skeletons.
- **Hierarchical Grouping**: Group rows by parent entities and automatically nest repeating child and grandchild elements.
- **Batch Chunking**: Split repeating entities across multiple payloads when batch limits are exceeded (e.g., max 50 line items per payload).
- **Audit & Zero-Loss Reconciliation**: Generates an auditable DataFrame containing payload indices, row tallies, source filenames, and Postman-ready payloads.

---

## Installation

```bash
pip install payloaded
```

---

## Quickstart

```python
import pandas as pd
import payloaded as pld

# 1. Source flat tabular data
df_source = pd.DataFrame([
    {"Batch_Number": "B001", "Order_ID": "ORD_101", "Item_SKU": "SKU_01", "Quantity": 2},
    {"Batch_Number": "B001", "Order_ID": "ORD_101", "Item_SKU": "SKU_02", "Quantity": 5},
    {"Batch_Number": "B001", "Order_ID": "ORD_102", "Item_SKU": "SKU_03", "Quantity": 1},
])

# 2. Define your desired nested payload structure skeleton
template = [
    {
        "batch_id": "{batch_id}",
        "orders": [
            {
                "order_id": "{order_id}",
                "line_items": [
                    {"sku": "{sku}", "qty": "{qty}"}
                ]
            }
        ]
    }
]

# 3. Configure entity hierarchy, group keys, limits, and mappings
config = {
    "entities": [
        {
            "path": "root",
            "repeat_limit": 20,
            "group_by": ["batch_id"],
            "mappings": [{"payload_key": "batch_id", "file_key": "Batch_Number"}],
        },
        {
            "path": "orders",
            "repeat_limit": 30,
            "group_by": ["order_id"],
            "mappings": [{"payload_key": "order_id", "file_key": "Order_ID"}],
        },
        {
            "path": "orders.line_items",
            "repeat_limit": 40,
            "mappings": [
                {"payload_key": "sku", "file_key": "Item_SKU"},
                {"payload_key": "qty", "file_key": "Quantity", "type_cast": "int"},
            ],
        },
    ]
}

# 4. Generate auditable payloads DataFrame
df_payloads = pld.build_payloads(
    source=df_source,
    template=template,
    config=config,
    output_format="json_string"  # or "dict"
)

# 5. Verify zero data loss with mathematical reconciliation
audit_report = pld.reconcile(df_payloads, expected_rows=len(df_source))
print(audit_report.summary())
```

---

## Output Structure

The output DataFrame provides complete transparency:

| Column | Description |
|---|---|
| `index` | Incremental payload identifier |
| `running_total` | Cumulative count of source rows packed (verifies zero data loss) |
| `payload` | Complete JSON payload ready to dispatch or paste into Postman |
| `source_filename` | Origin file name for multi-file traceability |

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
