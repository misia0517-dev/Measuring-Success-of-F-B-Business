import os
import sys
from pathlib import Path
import snowflake.connector


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if value.startswith(("'", '"')) and value.endswith(("'", '"')):
            value = value[1:-1]

        # Keep already-exported env vars as source of truth.
        os.environ.setdefault(key, value)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def run_query(cur, label: str, query: str) -> None:
    print(f"\n{'=' * 80}", flush=True)
    print(f"START: {label}", flush=True)
    print(f"QUERY:\n{query.strip()}", flush=True)
    print(f"{'=' * 80}", flush=True)

    cur.execute(query)

    print(f"STATUS: {cur.sfqid}", flush=True)

    try:
        rows = cur.fetchall()
        if rows:
            print(f"RESULT ({len(rows)} rows):", flush=True)
            for row in rows:
                print(row, flush=True)
        else:
            print("RESULT: Query returned no rows.", flush=True)
    except Exception:
        print("RESULT: No fetchable result set for this statement.", flush=True)

    print(f"END: {label}", flush=True)


def load_sql_statements(sql_path: Path) -> list[tuple[str, str]]:
    sql_text = sql_path.read_text(encoding="utf-8")

    statements: list[tuple[str, str]] = []
    for idx, statement in enumerate(sql_text.split(";"), start=1):
        query = statement.strip()
        if not query:
            continue
        first_line = query.splitlines()[0].strip()
        label = f"sql_{idx:02d}_{first_line[:40].replace(' ', '_').lower()}"
        statements.append((label, query))

    return statements


def main() -> None:
    conn = None
    cur = None

    try:
        repo_root = Path(__file__).resolve().parent.parent
        load_env_file(repo_root / ".env")

        print("Reading Snowflake credentials from environment variables...", flush=True)
        user = get_required_env("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD") or get_required_env("SNOWFLAKE_PAT")
        account = get_required_env("SNOWFLAKE_ACCOUNT")
        role = get_required_env("SNOWFLAKE_ROLE")
        warehouse = get_required_env("SNOWFLAKE_WAREHOUSE")
        database = get_required_env("SNOWFLAKE_DATABASE")
        schema = get_required_env("SNOWFLAKE_SCHEMA")

        print(f"Connecting to Snowflake account: {account}", flush=True)
        print(f"Using Snowflake user: {user}", flush=True)

        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
        )
        cur = conn.cursor()

        print("Connected to Snowflake successfully.", flush=True)

        queries = [
            ("use_role", f"USE ROLE {role}"),
            ("use_warehouse", f"USE WAREHOUSE {warehouse}"),
            ("use_database", f"USE DATABASE {database}"),
            ("use_schema", f"USE SCHEMA {schema}"),
        ]

        for label, query in queries:
            run_query(cur, label, query)

        sql_file = repo_root / "snowflake" / "snowflake_silver_to_gold.sql"
        object_queries = load_sql_statements(sql_file)
        for label, query in object_queries:
            run_query(cur, label, query)

        print("\nAll Snowflake queries finished successfully.", flush=True)

    except Exception as e:
        print("\nERROR OCCURRED DURING SNOWFLAKE LOAD", flush=True)
        print(repr(e), flush=True)
        sys.exit(1)

    finally:
        if cur is not None:
            print("Closing Snowflake cursor...", flush=True)
            cur.close()
        if conn is not None:
            print("Closing Snowflake connection...", flush=True)
            conn.close()
        print("Script finished.", flush=True)


if __name__ == "__main__":
    main()

