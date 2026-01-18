# dbt Macro: source_onelake

Read CSV and Parquet files from Microsoft Fabric OneLake directly in dbt models via `OPENROWSET` statements.

## Features

- ✅ CSV and Parquet formats
- ✅ Configurable CSV delimiters, line endings, row skipping
- ✅ OneLake path auto-construction from metadata
- ✅ Column type enforcement
- ✅ Case-sensitive path validation

## Setup

Copy `source_onelake.sql` to your `macros/` folder.

## Quick Start

### 1. Source Definition (YAML)

```yaml
sources:
  - name: my_onelake_source
    description: "Data files in OneLake"
    
    meta:
      onelake:
        onelake_path: https://onelake.dfs.fabric.microsoft.com/
        target_workspace: <workspace-guid>
        target_lakehouse: <lakehouse-guid>
        files_root: Files

    tables:
      - name: my_data_csv
        meta:
          onelake:
            filename: data.csv
            folder: input
            format: CSV
            first_row: 2
            field_terminator: ';'
            row_terminator: '0x0A'
        columns:
          - name: id
            data_type: int
          - name: name
            data_type: varchar(100)

      - name: my_data_parquet
        meta:
          onelake:
            filename: data.parquet
            folder: input
            format: Parquet
        columns:
          - name: id
            data_type: int
          - name: name
            data_type: varchar(100)
```

> **⚠️ Required**: Column definitions with data types are mandatory to prevent dbt reading target files to infer metadata during compilation causing compilation errors when files are missing. For rubuustness in all scenario's a WITH clause with given table layout is included in the OPENROWSET statement.

### 2. Use in Model

```sql
select *
from {{ source_onelake('my_onelake_source', 'my_data_csv') }}
```

### 3. Run

```powershell
dbt run --select my_model
```

## Metadata Reference

| Level | Key | Required | Default |
|-------|-----|----------|---------|
| **Source** | `onelake_path` | Yes | — |
| — | `target_workspace` | Yes | — |
| — | `target_lakehouse` | Yes | — |
| — | `files_root` | No | `Files` |
| **Table** | `filename` | Yes | — |
| — | `folder` | No | (empty) |
| — | `format` | No | `CSV` |
| — | `first_row` | No | `2` |
| — | `field_terminator` | No | `;` |
| — | `row_terminator` | No | `0x0A` |

## Examples

- [example_onelake_source.yml](example_onelake_source.yml) — CSV and Parquet source definitions
- [example_model_csv.sql](example_model_csv.sql) — CSV example
- [example_model_parquet.sql](example_model_parquet.sql) — Parquet example

Run examples:
```powershell
dbt build --select example_model_csv
dbt build --select example_model_parquet
```

The macro auto-handles format differences:
- **CSV**: FIRSTROW, FIELDTERMINATOR, ROWTERMINATOR options
- **Parquet**: No CSV options; auto-quotes column names with spaces

## Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| "Accessing storage path ... is not supported" | Path casing mismatch | Paths are **case-sensitive**. Verify exact folder names in OneLake |
| "File not found" | Incorrect filename or folder | Check dbt logs for full path; verify in OneLake |
| "Not authorized" | Missing workspace/lakehouse access | Verify SQL endpoint has read permissions |
| Compilation error (missing columns) | Column definitions not provided | Add all expected columns to source definition |
| Parser errors | Wrong `field_terminator` or `first_row` | Inspect file in OneLake; adjust CSV options |

## Debugging

Check logs with:
```powershell
dbt build --select my_model -d  # Debug mode
```

Output shows file format and full OneLake path:
```
Reading CSV my_source.my_data_csv from https://onelake.dfs.fabric.microsoft.com/.../data.csv
Reading PARQUET my_source.my_data_parquet from https://onelake.dfs.fabric.microsoft.com/.../data.parquet
```

## Requirements

- dbt 1.0+
- SQL backend with `OPENROWSET` (SQL Server 2019+, Synapse, Fabric SQL Endpoint)
- Microsoft Fabric workspace and OneLake access
- Workspace ID, Lakehouse ID, and workspace/lakehouse credentials

## License

GNU 