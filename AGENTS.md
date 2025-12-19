# Agent Instructions

## DuckDB / DuckLake Gotchas

### ALTER TABLE ADD COLUMN with DEFAULT
DuckDB/DuckLake does not support expressions like `FALSE` or `TRUE` as default values in `ALTER TABLE ADD COLUMN` statements. Only literals are supported.

**This will fail:**
```sql
ALTER TABLE my_table ADD COLUMN IF NOT EXISTS is_flag BOOLEAN DEFAULT FALSE
```

**This works:**
```sql
ALTER TABLE my_table ADD COLUMN IF NOT EXISTS is_flag BOOLEAN
```

If you need to set a default value for existing rows, do it in a separate UPDATE statement:
```sql
ALTER TABLE my_table ADD COLUMN IF NOT EXISTS is_flag BOOLEAN;
UPDATE my_table SET is_flag = false WHERE is_flag IS NULL;
```
