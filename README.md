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
import payloaded as pld

# Coming soon in v0.1.0!
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
