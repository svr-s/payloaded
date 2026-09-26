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

## Syntax Standards: Where to Use Curly Braces `{}`

To keep configurations clean, predictable, and avoid syntax errors, `payloaded` maintains a strict distinction between **Declarations** (plain strings) and **Dynamic Substitutions** (curly braces `{}`):

* **Declarations & Identifiers (NO `{}`)**: Schema keys, column pointers, and hierarchy paths must be plain text:
  - `"payload_key": "order_id"` *(declares the placeholder identifier)*
  - `"source_key": "Order_ID"` *(points to the source column name or 0-based column index)*
  - `"condition_source_key": "Status"` *(points to routing column name or index)*
  - `"path": "orders.line_items"` *(structural tree coordinate)*
  - `"group_by": ["order_id"]` *(grouping key identifiers)*

* **Dynamic Placeholders & References (MUST USE `{}`)**:
  - `payload_template`: `"order_id": "{order_id}"` *(signals a placeholder to be hydrated)*
  - `formula`: `"lower({Location}) & '-' & {Batch_Number}"` *(signals columns to be evaluated)*

### Why Formulas Require Braces `{col}`
Enclosing column names in `{}` within formulas is essential:
1. **Prevents Function Name Collisions**: If your CSV has a column literally named `strip`, `date`, `int`, or `round`, writing `strip({strip})` explicitly distinguishes the function `strip()` from the data column `{strip}`.
2. **Supports Spaces and Symbols**: Allows referencing columns with spaces like `{Batch Number}` or `{Order-ID}` without syntax errors.

> **Note**: `payloaded` strictly adheres to these definitions. Misplaced braces (e.g. putting `{}` in `payload_key` or `source_key`) are not silently altered and will be treated as literal text.

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
                        {"payload_key": "qty", "formula": "int(coalesce({Qty}, 0))"},
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

## Formula & Expression Engine

`payloaded` includes a sandboxed, zero-dependency AST formula engine (no `eval()`) that allows you to transform, concatenate, slice, and generate values directly in your configuration:

```json
{
  "payload_key": "tracking_code",
  "formula": "lower(strip({Location}))[1:5] & '-' & replace(strip({Batch_Number}), ':', '')"
}
```

### 1. Built-in Function Reference

| Function | Description | Example |
|---|---|---|
| `upper(val)` / `lower(val)` | Case transformations | `upper({status})` |
| `strip(val)` / `trim(val)` | Whitespace trimming | `strip({code})` |
| `replace(val, old, new)` | Substring replacement | `replace({phone}, '-', '')` |
| `slice(val, start, end)` or `[start:end]` | Native slicing | `{sku}[0:4]` |
| `lpad(val, len, char)` / `rpad(...)` | String padding | `lpad({id}, 5, '0')` $\rightarrow$ `'00042'` |
| `coalesce(a, b, ...)` | First non-empty value | `coalesce({alt_phone}, {phone}, 'N/A')` |
| `date_format(val, in_fmt, out_fmt)` | Date format conversion | `date_format({dt}, '%Y-%m-%d', '%d/%m/%Y')` |
| `now(format)` | UTC timestamp generator | `now('%Y-%m-%dT%H:%M:%SZ')` |
| `uuid()` | Generates unique UUID v4 | `uuid()` |
| `int(val)` / `float(val)` / `str(val)` | Type casting | `int({qty})` |

---

## Sequence Counters & Scoping

Child entities in API payloads (e.g. invoice lines, order items) frequently require auto-incrementing line numbers that don't exist in source CSVs. `payloaded` provides stateful sequence generators with explicit scoping:

```python
sequence(start=1, step=1, scope="parent")   # Default: Resets per parent entity
sequence(start=100, step=1, scope="global") # Never resets: counts continuously across all payloads
```

### Scoping Behavior

| Scope | Under `line_items` | Under `orders` | Across Split Chunks |
|---|---|---|---|
| **`parent`** *(default)* | Resets to `start` for **each Order** | Resets to `start` for **each Batch** | **Continues across chunks** (e.g. Order 101 chunk 1 has lines 1–40; chunk 2 continues with 41–80; next Order 102 resets to 1) |
| **`global`** | Counts continuously across all rows | Counts continuously across all rows | Never resets (e.g. 100, 101, 102...) |

#### Example: Line Item Numbering
```json
{
  "path": "orders.line_items",
  "mappings": [
    {
      "payload_key": "line_number",
      "formula": "sequence(start=1, scope='parent')"
    },
    {
      "payload_key": "sku",
      "source_key": "SKU"
    }
  ]
}
```

---

## Blank & Optional Field Handling (`omit_if_blank`)

By default, missing or `NaN` values in your source data resolve to JSON `null`:
```json
{"sku": "A1", "discount_code": null, "notes": null}
```

If an upstream API schema rejects `null` or empty fields for optional keys, use **`omit_if_blank: true`**.

### Behavior
- **Default**: `false` (optional field, does not need to be specified).
- **When `true`**: Evaluates whether the rendered value is `None`, `NaN`, empty string `""`, or whitespace-only `"   "`. If blank, the key is **completely omitted** from the hydrated JSON object.
- **Valid 0 / False preservation**: Numeric `0`, `0.0`, and boolean `false` are considered valid data and are **never** omitted.
- **Inheritance**: Can be configured at the **field level** or at the **entity level** (where all fields in that entity inherit the setting unless overridden).

#### Example: Field-Level & Entity-Level Omission
```json
{
  "path": "orders.line_items",
  "omit_if_blank": false,
  "mappings": [
    {
      "payload_key": "sku",
      "source_key": "SKU"
    },
    {
      "payload_key": "promo_code",
      "source_key": "Promo_Code",
      "omit_if_blank": true
    },
    {
      "payload_key": "gift_message",
      "source_key": "Gift_Message",
      "omit_if_blank": true
    }
  ]
}
```
If a row has `"SKU": "A1"` and empty cells for `Promo_Code` and `Gift_Message`, the output payload becomes:
```json
{
  "sku": "A1"
}
```
*(No `promo_code` or `gift_message` keys are present)*

---

## Wide-to-Long Wildcard Unpivot (`*`)

Tabular exports often store repeating items horizontally across columns (wide format) rather than normalized across rows:
```csv
location,batchnumber,orderid,ordername,ordervalue,orderid2,ordername2,ordervalue2,orderid3,ordervalue3
```

`payloaded` supports automatic wide-to-nested unpivoting using **wildcard asterisks (`*`)** in `source_key`:

```json
{
  "path": "orders",
  "mappings": [
    {
      "payload_key": "id",
      "source_key": "orderid*"
    },
    {
      "payload_key": "name",
      "source_key": "ordername*",
      "omit_if_blank": true
    },
    {
      "payload_key": "value",
      "source_key": "ordervalue*"
    }
  ]
}
```

### Key Advantages
1. **Dynamic Token Discovery**: No need to hardcode `orderid1`, `orderid2`, ..., `orderid42`. It scans the DataFrame headers and discovers all present suffixes/tokens dynamically (e.g. `""`, `"2"`, `"3"`, ..., `"42"`).
2. **Flexible Wildcard Placement**: Works for prefix, suffix, or infixed patterns (e.g. `orderid*`, `item_*_sku`, `line_*`).
3. **Resilient to Missing/Jagged Columns**: If an export generated `orderid3` and `ordervalue3` but completely omitted the column `ordername3`, `payloaded` will **not crash**. It safely resolves missing columns to `null` (or omits them if `omit_if_blank: true`).
4. **Empty Slot Pruning**: If an entity slot is completely empty for a row (e.g. only 2 orders present in a 5-slot file), blank entries are discarded automatically.
5. **Universal Placement**: Can be used on **any entity level** across your payload hierarchy (child items, grandchildren, or root items).

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


