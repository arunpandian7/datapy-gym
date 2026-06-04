# CLAUDE.md — datapy-gym

Local coding gym for data engineering interview practice. Implements a **PySpark track** (e-commerce domain) and a **SQL/DuckDB track** (fintech/payments domain); designed to support additional stacks (pandas, polars, etc.).

---

## Package manager

Always use `uv`. Never use `pip` directly.

```
uv sync                    # install deps from uv.lock
uv add <package>           # add a new dependency
uv run <command>           # run a command in the managed venv
```

---

## Architecture — two-layer design

```
pyspark/
  templates/NN_topic.py    ← SOURCE OF TRUTH: Python template files
  sessions/YYYY-MM-DD/     ← generated .ipynb files (committed, never edited directly)
  reset.py                 ← converts templates → notebooks via nbformat
  utils/__init__.py        ← get_spark(), check()
  data/                    ← CSVs (gitignored) + generate_data.py
```

### How reset.py works

`reset.py` imports each template module, calls its `cells()` function, and writes a `.ipynb` into `pyspark/sessions/YYYY-MM-DD/` using `nbformat`. The `cells()` function returns `list[tuple[str, str]]` where each tuple is `("markdown" | "code", source_string)`.

**Never edit `.ipynb` files directly.** Edit the `.py` template in `templates/`, then re-run `reset.py` to regenerate.

```bash
uv run python pyspark/reset.py          # regenerate all notebooks for today
uv run python pyspark/reset.py 03       # regenerate only notebooks matching "03"
uv run python pyspark/reset.py 03 --force  # skip overwrite prompt
```

Target matching: any arg that is a prefix of or substring of a notebook name matches it.

---

## Dataset

4 tables, synthetic e-commerce data. Generate with:

```bash
uv run python pyspark/data/generate_data.py
```

| Table | Rows | Key columns |
|-------|------|-------------|
| customers | 500 | customer_id, name, email, city, country, tier, signup_date |
| products | 100 | product_id, name, category, brand, price |
| orders | 8,000 | order_id, customer_id, order_date, status, total_amount |
| order_items | ~16,000 | item_id, order_id, product_id, quantity, unit_price |

**Intentional skew:** `customer_id=1` holds exactly 2,400 orders (30% of total). Used to exercise salting and skew-handling exercises.

CSVs are gitignored (`pyspark/data/*.csv`). Regenerate them before starting a new session if they are absent.

---

## check() API

`from utils import check`

```python
check(
    actual,           # DataFrame produced by the student
    expected,         # reference DataFrame
    problem="",       # optional label shown in HTML output
    ordered=False,    # if True, row order is significant
    precision=0.01,   # tolerance passed to chispa.assert_approx_df_equality
) -> bool
```

Fail-fast sequence:
1. `actual is None` → immediate failure with a descriptive message
2. Column names mismatch → report missing/extra columns and return
3. Row count mismatch → report counts and return
4. `chispa.assert_approx_df_equality` with `ignore_row_order=not ordered` → report first mismatch line

Renders colored HTML in Jupyter via `IPython.display`.

---

## Adding a new PySpark topic

1. Create `pyspark/templates/NN_topic.py` with a `cells()` function returning `list[tuple[str, str]]`.
2. Add `"NN_topic"` to the `NOTEBOOKS` list in `pyspark/reset.py`.
3. Regenerate:
   ```bash
   uv run python pyspark/reset.py NN
   ```

Template conventions observed in existing templates:
- First cell: markdown title
- Second cell: setup block that locates `pyspark/utils/`, loads data, caches all four DataFrames
- Problems alternate: markdown spec cell → `solution_N = None` cell → hidden `_expected_N` + `check()` cell

---

## Adding a new tech stack

Create a parallel directory tree alongside `pyspark/`:

```
STACK/
  utils/__init__.py        # get_X() session factory + check()
  data/generate_data.py    # dataset generator
  templates/NN_topic.py    # template files with cells() — source of truth, never edit .ipynb directly
  sessions/                # generated .ipynb files, one dated subdirectory per day
  reset.py                 # same pattern as pyspark/reset.py
```

Key points:
- `templates/` holds `.py` files only — these define problems and are the source of truth
- `sessions/` holds generated `.ipynb` files — learners work here, never in `templates/`
- `reset.py` imports each template, calls `cells()`, and writes the `.ipynb` via `nbformat`
- Add a `.gitkeep` inside `sessions/` so the directory is tracked when empty

Add stack-specific dependencies:
```bash
uv add <package>
```

No changes needed to `pyproject.toml` beyond `uv add`; uv manages it.

---

## Session git workflow

- `*/templates/*.py` — templates, source of truth, always committed
- `*/sessions/YYYY-MM-DD/*.ipynb` — generated session notebooks, committed (so practice work is saved)
- `*/data/*.csv` — gitignored, regenerated on demand
- `*/sessions/.gitkeep` — keeps the sessions/ directory tracked when empty

---

## Key file map

### PySpark track (e-commerce domain)

| File | Purpose |
|------|---------|
| `pyspark/reset.py` | CLI: converts `.py` templates → dated `.ipynb` session files |
| `pyspark/templates/NN_*.py` | Template source for each topic; defines `cells()` |
| `pyspark/sessions/YYYY-MM-DD/*.ipynb` | Active practice notebooks; committed |
| `pyspark/utils/__init__.py` | `get_spark()` and `check()` used inside every notebook |
| `pyspark/data/generate_data.py` | Generates the four CSV files with fixed seed (42) |
| `pyspark/data/*.csv` | Gitignored data files; recreate if missing |

### SQL track (fintech/payments domain, DuckDB backend)

| File | Purpose |
|------|---------|
| `sql/reset.py` | CLI: converts `.py` templates → dated `.ipynb` session files |
| `sql/templates/NN_*.py` | Template source (01–05); defines `cells()` |
| `sql/sessions/YYYY-MM-DD/*.ipynb` | Active practice notebooks; committed |
| `sql/utils/__init__.py` | `get_conn(data_dir)` and `check(sql_str, expected_df, conn, ...)` |
| `sql/utils/checks/TOPIC.py` | Per-topic `Checker` class with `p1`–`p5` methods |
| `sql/data/generate_data.py` | Generates users/merchants/accounts/transactions CSVs (seed 42) |
| `sql/data/*.csv` | Gitignored; regenerate if missing |

**SQL topics:** 01 aggregations · 02 window functions · 03 joins · 04 CTEs · 05 time series  
**Student answer format:** `solution_N = "SELECT ..."` (SQL string), checked via `checker.pN(solution_N)`

| File | Purpose |
|------|---------|
| `pyproject.toml` | Project deps (pyspark, duckdb, jupyterlab, pandas, numpy, nbformat) |
| `uv.lock` | Locked dependency tree |

---

## Running the project

### PySpark track

```bash
uv sync
uv run python pyspark/data/generate_data.py   # if CSVs missing
uv run python pyspark/reset.py
uv run jupyter lab pyspark/sessions/YYYY-MM-DD/
```

### SQL track

```bash
uv sync
uv run python sql/data/generate_data.py       # if CSVs missing
uv run python sql/reset.py
uv run jupyter lab sql/sessions/YYYY-MM-DD/
```

Use the actual date directory produced by `reset.py` in the `jupyter lab` command.
