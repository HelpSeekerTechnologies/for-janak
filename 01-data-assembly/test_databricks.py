"""
test_databricks.py
==================
Connectivity and access test for the Databricks workspace at
<YOUR_DATABRICKS_HOST> using the databricks-sdk.

Tests performed
---------------
1. Authenticate with each PAT token
2. List Unity Catalog catalogs
3. List schemas in dbw_unitycatalog_test
4. List tables  in dbw_unitycatalog_test.default
5. List files/volumes at /Volumes/dbw_unitycatalog_test/uploads/uploaded_files/
6. Run a sample SQL query via the SQL warehouse

Both tokens are tried; results are reported side by side.
"""

import sys
import textwrap
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ListCatalogsResponse,
    SchemaInfo,
    TableInfo,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HOST = "https://<YOUR_DATABRICKS_HOST>"
TOKENS = {
    "Token1 (pull_databricks.py)": "<YOUR_DATABRICKS_TOKEN>",
    "Token2 (databricks_chestermere_validation.py)": "<YOUR_DATABRICKS_TOKEN>",
}
CATALOG = "dbw_unitycatalog_test"
SCHEMA = "default"
VOLUME_PATH = "/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/"
WAREHOUSE_HTTP_PATH = "<YOUR_DATABRICKS_SQL_WAREHOUSE>"
WAREHOUSE_ID = "a7e9ada5cd37e1c7"
SAMPLE_QUERY = f"SELECT * FROM {CATALOG}.{SCHEMA}.ab_org_risk_flags LIMIT 5"

SEPARATOR = "=" * 72


def section(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


def try_list_catalogs(w: WorkspaceClient) -> bool:
    """Test 1 -- list catalogs."""
    section("TEST 1: List catalogs")
    try:
        catalogs = list(w.catalogs.list())
        print(f"  Found {len(catalogs)} catalog(s):")
        for c in catalogs:
            print(f"    - {c.name}")
        return True
    except Exception as exc:
        print(f"  FAILED: {exc}")
        return False


def try_list_schemas(w: WorkspaceClient) -> bool:
    """Test 2 -- list schemas in the target catalog."""
    section(f"TEST 2: List schemas in '{CATALOG}'")
    try:
        schemas = list(w.schemas.list(catalog_name=CATALOG))
        print(f"  Found {len(schemas)} schema(s):")
        for s in schemas:
            print(f"    - {s.name}")
        return True
    except Exception as exc:
        print(f"  FAILED: {exc}")
        return False


def try_list_tables(w: WorkspaceClient) -> bool:
    """Test 3 -- list tables in catalog.schema."""
    section(f"TEST 3: List tables in '{CATALOG}.{SCHEMA}'")
    try:
        tables = list(w.tables.list(catalog_name=CATALOG, schema_name=SCHEMA))
        print(f"  Found {len(tables)} table(s):")
        for t in tables:
            ttype = getattr(t, "table_type", "?")
            print(f"    - {t.name}  (type={ttype})")
        return True
    except Exception as exc:
        print(f"  FAILED: {exc}")
        return False


def try_list_volumes(w: WorkspaceClient) -> bool:
    """Test 4 -- list files in the Volumes path."""
    section(f"TEST 4: List files at '{VOLUME_PATH}'")
    try:
        files = list(w.files.list_directory_contents(VOLUME_PATH))
        file_list = list(files)
        print(f"  Found {len(file_list)} file(s)/folder(s):")
        for f in file_list[:30]:  # cap display at 30
            print(f"    - {f.path}  (size={getattr(f, 'file_size', '?')})")
        if len(file_list) > 30:
            print(f"    ... and {len(file_list) - 30} more")
        return True
    except Exception as exc:
        print(f"  FAILED: {exc}")
        return False


def try_sql_query(w: WorkspaceClient) -> bool:
    """Test 5 -- execute a SQL query via the statement-execution API."""
    section(f"TEST 5: SQL query via warehouse {WAREHOUSE_ID}")
    print(f"  Query: {SAMPLE_QUERY}")
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=SAMPLE_QUERY,
            wait_timeout="30s",
        )
        status = result.status
        print(f"  Status: {status.state if status else 'unknown'}")
        if status and "SUCCEEDED" not in str(status.state):
            err = getattr(status, "error", None)
            if err:
                print(f"  Error detail: {err}")
            return False

        manifest = result.manifest
        data = result.result
        if manifest and manifest.schema and manifest.schema.columns:
            col_names = [c.name for c in manifest.schema.columns]
            print(f"  Columns ({len(col_names)}): {col_names}")
        if data and data.data_array:
            print(f"  Rows returned: {len(data.data_array)}")
            for i, row in enumerate(data.data_array[:5]):
                print(f"    Row {i}: {row}")
        else:
            print("  (no rows returned)")
        return True
    except Exception as exc:
        print(f"  FAILED: {exc}")
        return False


def run_all_tests(label: str, token: str) -> dict:
    """Run every test with the given token and return a summary dict."""
    print(f"\n{'#' * 72}")
    print(f"# TESTING WITH: {label}")
    print(f"# Host: {HOST}")
    print(f"# Token: {token[:10]}...{token[-4:]}")
    print(f"{'#' * 72}")

    results = {}
    try:
        w = WorkspaceClient(host=HOST, token=token)
        # Quick auth check -- list current user
        section("AUTH CHECK: Current user")
        try:
            me = w.current_user.me()
            print(f"  Authenticated as: {me.user_name}  (display: {me.display_name})")
            results["auth"] = True
        except Exception as exc:
            print(f"  AUTH FAILED: {exc}")
            results["auth"] = False
            return results  # no point continuing

        results["catalogs"] = try_list_catalogs(w)
        results["schemas"] = try_list_schemas(w)
        results["tables"] = try_list_tables(w)
        results["volumes"] = try_list_volumes(w)
        results["sql_query"] = try_sql_query(w)

    except Exception as exc:
        print(f"\n  FATAL ERROR creating WorkspaceClient: {exc}")
        results["auth"] = False

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    all_results = {}
    for label, token in TOKENS.items():
        all_results[label] = run_all_tests(label, token)

    # ---- Summary ----
    print(f"\n\n{'#' * 72}")
    print("#  SUMMARY")
    print(f"{'#' * 72}")
    for label, res in all_results.items():
        print(f"\n  {label}:")
        for test_name, passed in res.items():
            status = "PASS" if passed else "FAIL"
            print(f"    {test_name:15s} : {status}")

    # Return 0 if at least one token passed auth
    if any(r.get("auth") for r in all_results.values()):
        sys.exit(0)
    else:
        print("\n  ** Neither token could authenticate. **")
        sys.exit(1)


if __name__ == "__main__":
    main()
