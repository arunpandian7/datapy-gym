import math
from pathlib import Path

import duckdb
import pandas as pd
from IPython.display import HTML, display


def get_conn(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with all four tables loaded as views."""
    conn = duckdb.connect()
    for table in ("users", "merchants", "accounts", "transactions"):
        csv_path = str(data_dir / f"{table}.csv")
        conn.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}', header=True)"
        )
    return conn


# ── HTML helpers (same style as pyspark/utils) ────────────────────────────────

def _row(status: bool, msg: str) -> str:
    icon  = "✓" if status else "✗"
    color = "#1e7a1e" if status else "#c0392b"
    return (
        f'<div style="margin:2px 0;font-family:monospace;color:{color};">'
        f"{icon} {msg}</div>"
    )


def _summary(ok: bool) -> str:
    text  = "All checks passed!" if ok else "Some checks failed — review your solution."
    color = "#1e7a1e" if ok else "#c0392b"
    return f'<div style="margin-top:8px;font-weight:bold;color:{color};">{text}</div>'


# ── check() ───────────────────────────────────────────────────────────────────

def check(
    actual_sql,
    expected_df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    problem: str = "",
    ordered: bool = False,
    precision: float = 0.01,
) -> bool:
    rows = []

    if problem:
        rows.append(f'<div style="font-weight:bold;margin-bottom:6px;">{problem}</div>')

    if actual_sql is None:
        rows.append(_row(False, "solution is None — assign your SQL string to the variable first"))
        display(HTML("".join(rows) + _summary(False)))
        return False

    # Execute student SQL
    try:
        actual_df = conn.execute(actual_sql).df()
    except Exception as exc:
        first_line = str(exc).splitlines()[0][:300]
        rows.append(_row(False, f"SQL error — {first_line}"))
        display(HTML("".join(rows) + _summary(False)))
        return False

    ok = True

    # Column check
    actual_cols   = list(actual_df.columns)
    expected_cols = list(expected_df.columns)
    if actual_cols != expected_cols:
        missing = sorted(set(expected_cols) - set(actual_cols))
        extra   = sorted(set(actual_cols) - set(expected_cols))
        parts   = []
        if missing:
            parts.append(f"missing: {missing}")
        if extra:
            parts.append(f"extra: {extra}")
        rows.append(_row(False, "columns mismatch — " + "; ".join(parts)))
        ok = False
        display(HTML("".join(rows) + _summary(ok)))
        return False

    rows.append(_row(True, f"columns match: {actual_cols}"))

    # Row count check
    if len(actual_df) != len(expected_df):
        rows.append(
            _row(False, f"row count mismatch — actual: {len(actual_df)}, expected: {len(expected_df)}")
        )
        ok = False
        display(HTML("".join(rows) + _summary(ok)))
        return False

    rows.append(_row(True, f"row count: {len(actual_df)}"))

    # Value check
    try:
        a, e = actual_df.copy(), expected_df.copy()
        if not ordered:
            decimals = max(0, math.ceil(-math.log10(precision)))
            for col in a.select_dtypes(include="float").columns:
                a[col] = a[col].round(decimals)
            for col in e.select_dtypes(include="float").columns:
                e[col] = e[col].round(decimals)
            a = a.sort_values(list(a.columns)).reset_index(drop=True)
            e = e.sort_values(list(e.columns)).reset_index(drop=True)
        pd.testing.assert_frame_equal(
            a.reset_index(drop=True),
            e.reset_index(drop=True),
            check_exact=False,
            atol=precision,
        )
        rows.append(_row(True, "values match"))
    except AssertionError as exc:
        first_line = str(exc).splitlines()[0][:300]
        rows.append(_row(False, f"values mismatch — {first_line}"))
        ok = False

    display(HTML("".join(rows) + _summary(ok)))
    return ok
