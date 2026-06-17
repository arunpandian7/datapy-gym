#!/usr/bin/env python3
import argparse
import importlib.util
import sys
from datetime import date
from pathlib import Path

import nbformat
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook

PYSPARK_DIR = Path(__file__).parent
TEMPLATES_DIR = PYSPARK_DIR / "templates"
SESSIONS_DIR = PYSPARK_DIR / "sessions"

NOTEBOOKS = [
    "01_aggregations",
    "02_window_functions",
    "03_joins",
    "04_partitioning",
    "05_salting",
    "06_broadcast_and_aqe",
    "07_complex_types_and_udfs",
    "08_sessionization_and_scd",
]


def resolve_targets(args: list[str]) -> list[str]:
    if not args:
        return NOTEBOOKS

    matched = []
    for arg in args:
        hits = [n for n in NOTEBOOKS if n.startswith(arg) or arg in n]
        for h in hits:
            if h not in matched:
                matched.append(h)

    if not matched:
        print(f"error: no notebooks match {args}", file=sys.stderr)
        sys.exit(1)

    return matched


def load_cells(name: str) -> list[tuple[str, str]]:
    module_path = TEMPLATES_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.cells()


def build_notebook(cells: list[tuple[str, str]]) -> nbformat.NotebookNode:
    nb_cells = []
    for kind, source in cells:
        if kind == "markdown":
            nb_cells.append(new_markdown_cell(source))
        else:
            nb_cells.append(new_code_cell(source))

    nb = new_notebook(cells=nb_cells)
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    nb.metadata["language_info"] = {
        "name": "python",
        "version": "3.13.0",
    }
    return nb


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset PySpark practice sessions.")
    parser.add_argument(
        "targets",
        nargs="*",
        help="Notebook numbers or name substrings (default: all)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing notebooks without prompting",
    )
    args = parser.parse_args()

    targets = resolve_targets(args.targets)

    today = date.today().isoformat()
    session_dir = SESSIONS_DIR / today
    session_dir.mkdir(parents=True, exist_ok=True)

    if not args.force:
        existing = list(session_dir.glob("*.ipynb"))
        if existing:
            answer = input(
                f"Session {today} has {len(existing)} notebook(s). Overwrite? [y/N] "
            ).strip().lower()
            if answer != "y":
                print("Aborted.")
                sys.exit(0)

    created = []
    for name in targets:
        cells = load_cells(name)
        nb = build_notebook(cells)
        out_path = session_dir / f"{name}.ipynb"
        with open(out_path, "w", encoding="utf-8") as f:
            nbformat.write(nb, f)
        created.append(name)

    print(f"\nCreated session: pyspark/sessions/{today}/")
    for name in created:
        print(f"  ✓ {name}.ipynb")

    print(f"\nLaunch with:")
    print(f"  uv run jupyter lab pyspark/sessions/{today}/")


if __name__ == "__main__":
    main()
