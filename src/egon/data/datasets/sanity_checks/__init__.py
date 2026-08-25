"""Shared building blocks for per-TaskGroup sanity checks.

Each TaskGroup in `egon.data.airflow.dags.pipeline` gets its own sanity
checks in a dedicated module here, named after the TaskGroup's
`group_id` (e.g. `sanity_checks.electricity_demand` for the
`electricity_demand` TaskGroup). This module holds the primitives every
such module needs: running a comparison, evaluating it against a
tolerance, and rendering the results as a compact table for the logs.

Deviations are meant to be reported via `logger.warning` rather than by
raising, so a single inconsistency does not block the whole pipeline
run - the intent is to make problems visible to whoever ran the
pipeline, not to fail the DAG. Each TaskGroup module decides that for
itself though; nothing here enforces it.
"""

from egon.data import db, logger

# Default relative deviation between an actual and an expected value
# above which a comparison counts as failed ("WARN").
REL_TOLERANCE = 0.05

# Columns of the table rendered by `render_table`. Kept narrow on
# purpose: `egon.data`'s log sink hard-wraps every line at 72
# characters (see `egon.data.echo`), and - since it colorizes output -
# wraps the whole message in ANSI codes that count toward that width
# even though they're invisible, so a comfortable margin below 72 is
# needed rather than cutting it exactly at 72.
_COLUMNS = [
    ("Check", 10, "<"),
    ("Act[TWh]", 8, ">"),
    ("Exp[TWh]", 8, ">"),
    ("DiffTWh", 7, ">"),
    ("Diff%", 6, ">"),
    ("St.", 4, "<"),
]


def sql_sum(table, where, column="demand"):
    """Return `SUM(column)` from `table` filtered by `where` as a float
    (0.0 if the query returns no rows or NULL)."""

    value = db.select_dataframe(
        f"SELECT SUM({column}) AS s FROM {table} WHERE {where}",
        warning=False,
    )["s"][0]
    return 0.0 if value is None else float(value)


def sql_grouped(sql):
    """Run `sql`, which must return one row per group with (at least)
    an `actual` and an `expected` column, and return it as a
    DataFrame."""

    return db.select_dataframe(sql, warning=False)


def evaluate(name, actual, expected, rtol=REL_TOLERANCE):
    """Build one table row for a single (e.g. national-total)
    comparison of `actual` against `expected`."""

    diff = actual - expected
    pct = (diff / expected * 100) if expected else float("nan")
    bad = (not expected and actual) or abs(pct) > rtol * 100
    status = "WARN" if bad else "OK"
    return (name, actual, expected, diff, pct, status)


def evaluate_grouped(name, df, rtol=REL_TOLERANCE):
    """Build one table row out of a DataFrame with one row per group
    (e.g. one row per NUTS-3 region), each with an `actual` and an
    `expected` column.

    Finer-grained than a single call to :func:`evaluate` on the
    aggregated totals: two errors of opposite sign can cancel out in a
    national total but will still be caught here. The returned row
    represents the single worst-case group (largest relative
    deviation), so its Diff/Diff%/status in the table are consistent
    with each other; `n_bad`/`n_total` report how many groups were
    checked and how many of them failed.
    """

    if df.empty:
        row = (name, float("nan"), float("nan"), float("nan"), float("nan"), "WARN")
        return row, 0, 0

    deviation = (df["actual"] - df["expected"]).abs() / df[
        "expected"
    ].replace(0, float("nan"))
    worst = deviation.idxmax()
    n_bad = int((deviation > rtol).sum())

    actual = float(df.loc[worst, "actual"])
    expected = float(df.loc[worst, "expected"])
    row = (
        name,
        actual,
        expected,
        actual - expected,
        float(deviation.loc[worst]) * 100,
        "WARN" if n_bad else "OK",
    )
    return row, n_bad, len(df)


def render_table(rows, unit=1e6):
    """Render `rows` (as returned by :func:`evaluate`/
    :func:`evaluate_grouped`) as a compact fixed-width table, one row
    per line. `unit` converts the MWh values to the table's TWh
    columns."""

    header = " | ".join(
        f"{title:{align}{width}}" for title, width, align in _COLUMNS
    )
    separator = "-" * len(header)

    lines = [header, separator]
    for name, actual, expected, diff, pct, status in rows:
        pct_str = "n/a" if pct != pct else f"{pct:.2f}"  # pct != pct: NaN
        cells = [
            f"{name[:10]:<10}",
            f"{actual / unit:>8.3f}",
            f"{expected / unit:>8.3f}",
            f"{diff / unit:>7.3f}",
            f"{pct_str:>6}",
            f"{status:<4}",
        ]
        lines.append(" | ".join(cells))

    return "\n".join(lines)


def log_table(tag, title, rows):
    """Log `rows` as a table, prefixed with `[tag] title`. Logged as a
    warning if any row's status is "WARN", as info otherwise."""

    message = f"[{tag}] {title}\n{render_table(rows)}"
    if any(row[-1] == "WARN" for row in rows):
        logger.warning(message)
    else:
        logger.info(message)
