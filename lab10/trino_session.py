"""Lab 10: connection to Trino and helpers, shared by the other files of lab10.

From the root of the project:
    export TRINO_HOST=trino TRINO_PORT=8080 TRINO_USER=$KUBERNETES_NAMESPACE
    uv run python
    >>> exec(open("lab10/trino_session.py").read())
"""
import os

import pandas as pd
import trino

CATALOG = "lakehouse"
pd.set_option("display.width", 250)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 110)

conn = trino.dbapi.connect(
    host=os.environ.get("TRINO_HOST", "trino"),
    port=int(os.environ.get("TRINO_PORT", "8080")),
    user=os.environ.get("TRINO_USER", os.environ["KUBERNETES_NAMESPACE"]),
    http_scheme="http",
    catalog=CATALOG,
)


def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()  # the query only completes when its results are fetched
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def show(title, statement):
    print(f"=== {title} ===")
    print(sql(statement).to_string(index=False))
    print()


show("Trino version", "SELECT version()")