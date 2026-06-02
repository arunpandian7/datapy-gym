from pyspark.sql import SparkSession
from IPython.display import display, HTML
import chispa


def get_spark(app_name: str = "DataPyGym") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.repl.eagerEval.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _row(status: bool, msg: str) -> str:
    icon = "✓" if status else "✗"
    color = "#1e7a1e" if status else "#c0392b"
    return (
        f'<div style="margin:2px 0;font-family:monospace;color:{color};">'
        f"{icon} {msg}</div>"
    )


def _summary(ok: bool) -> str:
    text = "All checks passed!" if ok else "Some checks failed — review your solution."
    color = "#1e7a1e" if ok else "#c0392b"
    return (
        f'<div style="margin-top:8px;font-weight:bold;color:{color};">{text}</div>'
    )


def check(
    actual,
    expected,
    problem: str = "",
    ordered: bool = False,
    precision: float = 0.01,
) -> bool:
    rows = []

    if problem:
        rows.append(
            f'<div style="font-weight:bold;margin-bottom:6px;">{problem}</div>'
        )

    if actual is None:
        rows.append(
            _row(False, "solution is None — assign your DataFrame to the variable first")
        )
        display(HTML("".join(rows) + _summary(False)))
        return False

    ok = True

    actual_cols = list(actual.columns)
    expected_cols = list(expected.columns)
    if actual_cols != expected_cols:
        missing = set(expected_cols) - set(actual_cols)
        extra = set(actual_cols) - set(expected_cols)
        parts = []
        if missing:
            parts.append(f"missing: {sorted(missing)}")
        if extra:
            parts.append(f"extra: {sorted(extra)}")
        rows.append(_row(False, "columns mismatch — " + "; ".join(parts)))
        ok = False
        display(HTML("".join(rows) + _summary(ok)))
        return False

    rows.append(_row(True, f"columns match: {actual_cols}"))

    actual_count = actual.count()
    expected_count = expected.count()
    if actual_count != expected_count:
        rows.append(
            _row(
                False,
                f"row count mismatch — actual: {actual_count}, expected: {expected_count}",
            )
        )
        ok = False
        display(HTML("".join(rows) + _summary(ok)))
        return False

    rows.append(_row(True, f"row count: {actual_count}"))

    try:
        chispa.assert_approx_df_equality(
            actual, expected, precision=precision, ignore_row_order=not ordered
        )
        rows.append(_row(True, "values match"))
    except Exception as exc:
        first_line = str(exc).splitlines()[0][:300]
        rows.append(_row(False, f"values mismatch — {first_line}"))
        ok = False

    display(HTML("".join(rows) + _summary(ok)))
    return ok
