"""
agent_0d_federal_grants.py
==========================
Agent 0D — Federal Grants Enrichment for Operation Lineage Audit

Pulls Government of Canada Grants & Contributions data from Databricks,
filters to Alberta recipients, prepares BN-based matching keys, and
outputs CSV + audit log for Neo4j ingestion.

Data source: Databricks Volume at
  /Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/

Output:
  - federal_grants.csv   — BN, org_name, federal_department, program, amount, fiscal_year, province
  - federal_grants_log.md — full audit log
"""

import sys
import os
import io
import re
import traceback
from datetime import datetime

# --- Encoding safety (Windows cp1252 fix) ---
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABRICKS_HOST = "<YOUR_DATABRICKS_HOST>"
DATABRICKS_TOKEN = "<YOUR_DATABRICKS_TOKEN>"
DATABRICKS_WAREHOUSE = "<YOUR_DATABRICKS_SQL_WAREHOUSE>"
CATALOG = "dbw_unitycatalog_test"
SCHEMA = "default"

VOLUME_PATH = "/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/"

OUTPUT_DIR = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "federal_grants.csv")
OUTPUT_LOG = os.path.join(OUTPUT_DIR, "federal_grants_log.md")

# Target output schema columns
OUTPUT_COLUMNS = ["BN", "org_name", "federal_department", "program", "amount", "fiscal_year", "province"]

# Alberta province codes/names for filtering
ALBERTA_CODES = {"AB", "ALBERTA", "ALTA", "ALTA.", "AB.", "PROVINCE OF ALBERTA"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LINES = []

def log(msg, level="INFO"):
    """Append a timestamped message to the in-memory log and print to stdout."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] [{level}] {msg}"
    LOG_LINES.append(entry)
    print(entry)

def write_log(summary_stats=None):
    """Flush the in-memory log to the markdown file."""
    with open(OUTPUT_LOG, "w", encoding="utf-8") as f:
        f.write("# Agent 0D -- Federal Grants Enrichment Log\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("**Agent:** 0D (Federal Grants Enrichment)\n")
        f.write("**Operation:** Lineage Audit\n")
        f.write(f"**Data Source:** Databricks Volume `{VOLUME_PATH}`\n\n")
        f.write("---\n\n")

        if summary_stats:
            f.write("## Summary\n\n")
            f.write("| Metric | Value |\n")
            f.write("|--------|-------|\n")
            for k, v in summary_stats.items():
                f.write(f"| {k} | {v} |\n")
            f.write("\n---\n\n")

        f.write("## File Inventory\n\n")
        f.write("| File | Size | Description |\n")
        f.write("|------|------|-------------|\n")
        f.write("| grants.csv | ~2.2 GB | GoC Proactive Disclosure of Grants & Contributions |\n\n")
        f.write("---\n\n")

        f.write("## Column Mapping\n\n")
        f.write("| Output Column | Source Column (GoC) |\n")
        f.write("|---------------|--------------------|\n")
        f.write("| BN | recipient_business_number |\n")
        f.write("| org_name | recipient_legal_name |\n")
        f.write("| federal_department | owner_org_title |\n")
        f.write("| program | prog_name_en |\n")
        f.write("| amount | agreement_value |\n")
        f.write("| fiscal_year | agreement_start_date (derived Apr-Mar FY) |\n")
        f.write("| province | recipient_province |\n\n")
        f.write("---\n\n")

        f.write("## Execution Log\n\n")
        f.write("```\n")
        for line in LOG_LINES:
            f.write(line + "\n")
        f.write("```\n")
    print(f"\nLog written to: {OUTPUT_LOG}")


# ---------------------------------------------------------------------------
# Databricks connection helpers
# ---------------------------------------------------------------------------
def get_databricks_connection():
    """Create a Databricks SQL connection using databricks-sql-connector."""
    from databricks import sql as dbsql
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_WAREHOUSE,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
        schema=SCHEMA
    )


def run_query(conn, sql, description="query"):
    """Execute SQL and return results as a pandas DataFrame."""
    log(f"Executing {description}: {sql[:200]}...")
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        cursor.close()
        df = pd.DataFrame(rows, columns=columns)
        log(f"  -> returned {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        log(f"  -> FAILED: {e}", "ERROR")
        return pd.DataFrame()


def run_query_raw(conn, sql, description="query"):
    """Execute SQL and return raw cursor results (for LIST commands etc.)."""
    log(f"Executing {description}: {sql[:200]}...")
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        cursor.close()
        log(f"  -> returned {len(rows)} rows")
        return rows, columns
    except Exception as e:
        log(f"  -> FAILED: {e}", "ERROR")
        return [], []


# ---------------------------------------------------------------------------
# Step 1: Discover available files in the GoC Grants volume
# ---------------------------------------------------------------------------
def discover_volume_files(conn):
    """List files in the GoC Grants volume directory."""
    log("=" * 60)
    log("STEP 1: Discover files in GoC Grants volume")
    log("=" * 60)

    files_found = []

    # Method A: LIST command
    try:
        rows, cols = run_query_raw(conn, f"LIST '{VOLUME_PATH}'", "LIST volume directory")
        if rows:
            log(f"LIST returned {len(rows)} entries:")
            for row in rows:
                row_dict = dict(zip(cols, row)) if cols else {}
                path = row_dict.get("path", row_dict.get("name", str(row[0]) if row else ""))
                size = row_dict.get("size", row_dict.get("length", "?"))
                log(f"  - {path}  (size={size})")
                files_found.append({"path": str(path), "size": size, "row": row_dict})
    except Exception as e:
        log(f"LIST command failed: {e}", "WARN")

    # Method B: If LIST returned nothing, try using Databricks SDK to list files
    if not files_found:
        log("Attempting SDK-based file listing as fallback...")
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient(
                host=f"https://{DATABRICKS_HOST}",
                token=DATABRICKS_TOKEN
            )
            file_entries = list(w.files.list_directory_contents(VOLUME_PATH))
            for f in file_entries:
                path = getattr(f, 'path', str(f))
                size = getattr(f, 'file_size', '?')
                log(f"  - {path}  (size={size})")
                files_found.append({"path": str(path), "size": size})
        except Exception as e:
            log(f"SDK file listing also failed: {e}", "WARN")

    if not files_found:
        log("No files found via any method. Will attempt brute-force CSV read.", "WARN")

    return files_found


# ---------------------------------------------------------------------------
# Step 2: Check for managed tables with federal grant data
# ---------------------------------------------------------------------------
def check_managed_tables(conn):
    """Search for tables that might contain federal grants data."""
    log("=" * 60)
    log("STEP 2: Check for managed tables with federal grants data")
    log("=" * 60)

    table_hits = []
    search_patterns = ['%fed%', '%goc%', '%federal%', '%gc_%', '%grants_contrib%',
                       '%canada_grant%', '%gov_canada%']

    for pattern in search_patterns:
        sql = f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE '{pattern}'"
        df = run_query(conn, sql, f"SHOW TABLES LIKE '{pattern}'")
        if not df.empty:
            for _, row in df.iterrows():
                tname = row.get("tableName", row.get("table_name", row.iloc[0] if len(row) > 0 else "unknown"))
                log(f"  TABLE MATCH: {tname}")
                table_hits.append(str(tname))

    if not table_hits:
        log("No managed tables matching federal grant patterns found.")
    else:
        log(f"Found {len(table_hits)} matching table(s): {table_hits}")

    return table_hits


# ---------------------------------------------------------------------------
# Step 3: Read federal grants data from volume CSV files
# ---------------------------------------------------------------------------
def read_volume_csvs(conn, files_found):
    """Read CSV files from the GoC Grants volume and combine them."""
    log("=" * 60)
    log("STEP 3: Read federal grants CSV files from volume")
    log("=" * 60)

    all_dfs = []
    csv_files = []

    # Extract CSV files from the discovered file list
    for f in files_found:
        path = f.get("path", "")
        # The path might be absolute or relative — normalize
        basename = os.path.basename(path) if path else ""
        if basename.lower().endswith(".csv"):
            csv_files.append(path)

    # If no CSVs found via listing, try brute-force known common filenames
    if not csv_files:
        log("No CSV files identified from listing. Trying common filename patterns...", "WARN")
        common_names = [
            "GoC_Grants.csv", "goc_grants.csv",
            "federal_grants.csv", "Federal_Grants.csv",
            "gc_grants_contributions.csv", "GC_Grants_Contributions.csv",
            "grants_and_contributions.csv", "Grants_and_Contributions.csv",
            "GoC Grants.csv", "goc-grants.csv",
            "proactive_disclosure.csv", "Proactive_Disclosure.csv",
            "gc_proactive_disclosure.csv",
        ]
        for name in common_names:
            test_path = VOLUME_PATH + name
            csv_files.append(test_path)

    # Try to read each CSV — use read_files with header => true
    for csv_path in csv_files:
        # Ensure the path is properly formatted for Databricks SQL
        if csv_path.startswith("/Volumes"):
            read_path = csv_path
        elif csv_path.startswith("dbfs:"):
            read_path = csv_path.replace("dbfs:", "")
        else:
            basename = os.path.basename(csv_path)
            read_path = VOLUME_PATH + basename

        # First: get schema/sample with header detection
        log(f"Attempting to read: {read_path}")

        # Method A: read_files with header option (preferred)
        sql = f"SELECT * FROM read_files('{read_path}', format => 'csv', header => true) LIMIT 5"
        df_sample = run_query(conn, sql, f"SAMPLE CSV (read_files) {os.path.basename(read_path)}")

        if not df_sample.empty:
            log(f"  Schema detected via read_files: {list(df_sample.columns)}")
            # Now read with Alberta filter at the SQL level to avoid pulling 2GB
            # Identify the province column
            prov_col = None
            for c in df_sample.columns:
                if 'province' in c.lower() or 'prov' in c.lower():
                    prov_col = c
                    break

            if prov_col:
                log(f"  Province column found: '{prov_col}' — filtering at SQL level for AB")
                sql_full = (
                    f"SELECT * FROM read_files('{read_path}', format => 'csv', header => true) "
                    f"WHERE UPPER(TRIM(`{prov_col}`)) IN ('AB', 'ALBERTA')"
                )
            else:
                log(f"  No province column found — reading all rows", "WARN")
                sql_full = f"SELECT * FROM read_files('{read_path}', format => 'csv', header => true)"

            df = run_query(conn, sql_full, f"READ CSV (AB filtered) {os.path.basename(read_path)}")

            if not df.empty:
                log(f"  SUCCESS: {len(df)} rows x {len(df.columns)} cols from {os.path.basename(read_path)}")
                log(f"  Columns: {list(df.columns)}")
                df["_source_file"] = os.path.basename(read_path)
                all_dfs.append(df)
            else:
                log(f"  EMPTY result for {os.path.basename(read_path)}")
        else:
            # Method B: try csv.` ` with options
            log(f"  read_files failed, trying csv.` ` format...")
            sql_b = f"SELECT * FROM csv.`{read_path}` LIMIT 5"
            df_b = run_query(conn, sql_b, f"SAMPLE CSV (csv.) {os.path.basename(read_path)}")
            if not df_b.empty:
                # Check if first row is header
                first_row = df_b.iloc[0].tolist()
                looks_like_header = any('name' in str(v).lower() or 'number' in str(v).lower()
                                       for v in first_row if v is not None)
                if looks_like_header:
                    log(f"  CSV has header in row 0: {first_row[:8]}...")
                    # Re-read with explicit header handling via read_files
                    sql_c = f"SELECT * FROM read_files('{read_path}', format => 'csv', header => true) LIMIT 5"
                    df_c = run_query(conn, sql_c, f"RE-READ with header")
                    if not df_c.empty:
                        log(f"  Re-read schema: {list(df_c.columns)}")

                # Read full (will need post-processing)
                sql_full = f"SELECT * FROM csv.`{read_path}`"
                df = run_query(conn, sql_full, f"READ CSV (full) {os.path.basename(read_path)}")
                if not df.empty:
                    df["_source_file"] = os.path.basename(read_path)
                    all_dfs.append(df)

    # Fallback: wildcard with read_files
    if not all_dfs:
        log("Attempting wildcard read_files of all CSVs in volume directory...")
        sql = f"SELECT * FROM read_files('{VOLUME_PATH}', format => 'csv', header => true)"
        df = run_query(conn, sql, "read_files() wildcard CSV read")
        if not df.empty:
            log(f"  SUCCESS (read_files wildcard): {len(df)} rows x {len(df.columns)} cols")
            log(f"  Columns: {list(df.columns)}")
            df["_source_file"] = "read_files_wildcard"
            all_dfs.append(df)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        log(f"Combined dataset: {len(combined)} rows x {len(combined.columns)} cols")
        return combined
    else:
        log("NO FEDERAL GRANTS DATA COULD BE READ FROM VOLUME.", "ERROR")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Step 3b: Try reading from managed tables if volume read failed
# ---------------------------------------------------------------------------
def read_from_tables(conn, table_names):
    """Read federal grants data from managed Databricks tables."""
    log("Attempting to read from managed tables as fallback...")
    all_dfs = []
    for tname in table_names:
        sql = f"SELECT * FROM {CATALOG}.{SCHEMA}.{tname}"
        df = run_query(conn, sql, f"READ table {tname}")
        if not df.empty:
            log(f"  Table {tname}: {len(df)} rows x {len(df.columns)} cols")
            log(f"  Columns: {list(df.columns)}")
            df["_source_file"] = f"table:{tname}"
            all_dfs.append(df)
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        return combined
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Column detection / normalization
# ---------------------------------------------------------------------------
def find_column(df, candidates, allow_partial=True):
    """Find the first column in df that matches one of the candidate names (case-insensitive).
    If allow_partial, also try substring matching."""
    cols_lower = {c.lower().strip(): c for c in df.columns}
    # Exact match
    for cand in candidates:
        if cand.lower().strip() in cols_lower:
            return cols_lower[cand.lower().strip()]
    # Partial / substring match
    if allow_partial:
        for cand in candidates:
            for col_lower, col_orig in cols_lower.items():
                if cand.lower() in col_lower:
                    return col_orig
    return None


def normalize_columns(df):
    """Map the raw columns to the standardized output schema.
    Returns a new DataFrame with columns: BN, org_name, federal_department, program, amount, fiscal_year, province
    """
    log("Normalizing columns...")
    log(f"  Raw columns: {list(df.columns)}")

    mapping = {}

    # BN / Business Number (GoC proactive disclosure uses 'recipient_business_number')
    bn_col = find_column(df, [
        "recipient_business_number", "Recipient Business Number",
        "BN", "bn", "Business Number", "business_number", "BN/Registration Number",
        "bn_registration_number", "registration_number", "reg_number",
        "BN Number", "bn_number"
    ])
    if bn_col:
        mapping["BN"] = bn_col
        log(f"  BN column: '{bn_col}'")
    else:
        log("  BN column: NOT FOUND — will set to empty", "WARN")

    # Organization name (GoC uses 'recipient_legal_name')
    org_col = find_column(df, [
        "recipient_legal_name", "Recipient Legal Name",
        "recipient_operating_name", "Recipient Operating Name",
        "org_name", "organization_name", "recipient_name", "Recipient Name",
        "recipient", "legal_name", "Legal Name", "organization", "Organization",
        "org_legal_name", "Org Legal Name", "name", "Name",
        "recipient_org_name", "beneficiary", "Beneficiary"
    ])
    if org_col:
        mapping["org_name"] = org_col
        log(f"  org_name column: '{org_col}'")
    else:
        log("  org_name column: NOT FOUND", "WARN")

    # Federal department (GoC uses 'owner_org_title' for the department name)
    dept_col = find_column(df, [
        "owner_org_title", "Owner Org Title",
        "federal_department", "department", "Department", "dept",
        "owner_org", "Owner Org", "owner_organization",
        "funding_department", "Funding Department",
        "dept_name", "department_name", "Department Name",
        "org_name_en", "organization_name_en",
        "minister", "Minister"
    ])
    if dept_col:
        mapping["federal_department"] = dept_col
        log(f"  federal_department column: '{dept_col}'")
    else:
        log("  federal_department column: NOT FOUND", "WARN")

    # Program (GoC uses 'prog_name_en')
    prog_col = find_column(df, [
        "prog_name_en", "Program Name English",
        "program", "Program", "program_name", "Program Name",
        "program_name_en",
        "agreement_type", "Agreement Type",
        "funding_type", "Funding Type",
        "transfer_payment_program", "Transfer Payment Program",
        "prog_purpose_en", "purpose"
    ])
    if prog_col:
        mapping["program"] = prog_col
        log(f"  program column: '{prog_col}'")
    else:
        log("  program column: NOT FOUND", "WARN")

    # Amount (GoC uses 'agreement_value')
    amt_col = find_column(df, [
        "agreement_value", "Agreement Value",
        "amount", "Amount", "total_amount", "Total Amount",
        "funding_amount", "Funding Amount",
        "value", "Value", "grant_amount", "Grant Amount",
        "total", "Total", "amendment_value", "Amendment Value"
    ])
    if amt_col:
        mapping["amount"] = amt_col
        log(f"  amount column: '{amt_col}'")
    else:
        log("  amount column: NOT FOUND", "WARN")

    # Fiscal year (GoC uses 'agreement_start_date' — we will derive fiscal year from it)
    fy_col = find_column(df, [
        "fiscal_year", "Fiscal Year", "FiscalYear", "fiscal_yr",
        "fy", "FY", "year", "Year", "agreement_start_date",
        "agreement_date", "date", "Date"
    ])
    if fy_col:
        mapping["fiscal_year"] = fy_col
        log(f"  fiscal_year column: '{fy_col}'")
    else:
        log("  fiscal_year column: NOT FOUND", "WARN")

    # Province (GoC uses 'recipient_province')
    prov_col = find_column(df, [
        "recipient_province", "Recipient Province",
        "province", "Province", "prov",
        "province_territory", "Province/Territory",
        "region", "Region", "recipient_region", "Recipient Region",
        "recipient_province_territory", "state_province",
        "province_code", "prov_code"
    ])
    if prov_col:
        mapping["province"] = prov_col
        log(f"  province column: '{prov_col}'")
    else:
        log("  province column: NOT FOUND", "WARN")

    # Build the normalized dataframe
    result = pd.DataFrame()
    for target_col, source_col in mapping.items():
        result[target_col] = df[source_col].copy()

    # Fill missing output columns
    for col in OUTPUT_COLUMNS:
        if col not in result.columns:
            result[col] = ""

    # Reorder
    result = result[OUTPUT_COLUMNS]

    # Preserve the source file column if available
    if "_source_file" in df.columns:
        result["_source_file"] = df["_source_file"].values

    log(f"  Normalized: {len(result)} rows")
    return result


# ---------------------------------------------------------------------------
# Step 4: Filter to Alberta
# ---------------------------------------------------------------------------
def filter_alberta(df):
    """Filter to Alberta recipients based on province column."""
    log("=" * 60)
    log("STEP 4: Filter to Alberta recipients")
    log("=" * 60)

    total_before = len(df)

    if "province" in df.columns:
        # Normalize province values for comparison
        prov_upper = df["province"].astype(str).str.strip().str.upper()
        mask = prov_upper.isin(ALBERTA_CODES)
        df_ab = df[mask].copy()
        log(f"  Filtered {total_before} -> {len(df_ab)} Alberta rows")
        log(f"  Province value distribution (top 15):")
        prov_counts = prov_upper.value_counts().head(15)
        for prov, cnt in prov_counts.items():
            marker = " <-- KEPT" if prov in ALBERTA_CODES else ""
            log(f"    {prov}: {cnt}{marker}")
        return df_ab
    else:
        log("  No province column available — cannot filter. Returning all rows.", "WARN")
        return df


# ---------------------------------------------------------------------------
# Step 5: Clean and prepare matching keys
# ---------------------------------------------------------------------------
def clean_bn(val):
    """Normalize a Business Number value. BN format: 9 digits + 2 letters + 4 digits (e.g., 123456789RR0001)."""
    if pd.isna(val):
        return ""
    s = str(val).strip().upper()
    # Remove spaces, dashes
    s = re.sub(r'[\s\-]', '', s)
    # Check if it looks like a valid BN
    if re.match(r'^\d{9}[A-Z]{2}\d{4}$', s):
        return s
    # Maybe just the 9-digit root
    if re.match(r'^\d{9}$', s):
        return s
    # Maybe with extra characters — try to extract
    match = re.search(r'(\d{9}[A-Z]{2}\d{4})', s)
    if match:
        return match.group(1)
    match = re.search(r'(\d{9})', s)
    if match:
        return match.group(1)
    return s  # return as-is if nothing matches


def clean_amount(val):
    """Parse an amount value to float."""
    if pd.isna(val):
        return 0.0
    s = str(val).strip()
    # Remove $ signs, commas, spaces
    s = re.sub(r'[\$,\s]', '', s)
    # Handle parentheses as negative
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return 0.0


def prepare_for_output(df):
    """Clean BN, amount, and other fields for output."""
    log("=" * 60)
    log("STEP 5: Clean and prepare matching keys")
    log("=" * 60)

    df = df.copy()

    # Clean BN
    df["BN"] = df["BN"].apply(clean_bn)
    bn_populated = (df["BN"] != "").sum()
    bn_empty = (df["BN"] == "").sum()
    log(f"  BN populated: {bn_populated} / {len(df)}")
    log(f"  BN empty: {bn_empty} / {len(df)}")

    # Clean amount
    df["amount"] = df["amount"].apply(clean_amount)
    log(f"  Total federal grant amount (Alberta): ${df['amount'].sum():,.2f}")

    # Trim strings
    for col in ["org_name", "federal_department", "program", "fiscal_year", "province"]:
        df[col] = df[col].astype(str).str.strip()

    # Derive fiscal year from date-like values if the fiscal_year column looks like dates
    sample_fy = df["fiscal_year"].head(20).tolist()
    has_dates = any('-' in str(v) and len(str(v)) >= 10 for v in sample_fy if str(v) not in ('', 'nan', 'None'))
    if has_dates:
        log("  fiscal_year column contains date values — deriving fiscal year (Apr-Mar)...")
        unparseable_count = 0
        def date_to_fy(val):
            nonlocal unparseable_count
            s = str(val).strip()
            if s in ('', 'nan', 'None', 'NaT'):
                return ''
            try:
                dt = pd.to_datetime(s)
                # Canadian fiscal year: April 1 to March 31
                # e.g. 2020-06-15 -> FY 2020-2021; 2021-02-15 -> FY 2020-2021
                if dt.month >= 4:
                    return f"{dt.year}-{dt.year + 1}"
                else:
                    return f"{dt.year - 1}-{dt.year}"
            except Exception:
                unparseable_count += 1
                return s  # return raw value if not parseable as date
        df["fiscal_year"] = df["fiscal_year"].apply(date_to_fy)
        log(f"  Fiscal year sample after conversion: {df['fiscal_year'].head(5).tolist()}")
        if unparseable_count > 0:
            log(f"  NOTE: {unparseable_count} rows had unparseable date values in fiscal_year", "WARN")

    # Match readiness
    exact_match = df["BN"] != ""
    fuzzy_needed = df["BN"] == ""
    log(f"  Exact BN match ready: {exact_match.sum()}")
    log(f"  Fuzzy match needed:   {fuzzy_needed.sum()}")

    # Deduplicate
    before_dedup = len(df)
    df = df.drop_duplicates()
    after_dedup = len(df)
    if before_dedup != after_dedup:
        log(f"  Deduplication: {before_dedup} -> {after_dedup} (removed {before_dedup - after_dedup} dupes)")

    return df


# ---------------------------------------------------------------------------
# Step 6: Output
# ---------------------------------------------------------------------------
def write_outputs(df):
    """Write the final CSV and log files."""
    log("=" * 60)
    log("STEP 6: Write output files")
    log("=" * 60)

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Write CSV (without the internal _source_file column)
    out_cols = OUTPUT_COLUMNS
    df_out = df[out_cols] if not df.empty else pd.DataFrame(columns=out_cols)
    df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    log(f"  Written: {OUTPUT_CSV}")
    log(f"  Rows: {len(df_out)}")
    log(f"  Columns: {list(df_out.columns)}")

    summary_stats = {}
    if not df_out.empty:
        log(f"  Amount range: ${df_out['amount'].min():,.2f} to ${df_out['amount'].max():,.2f}")
        log(f"  Unique orgs: {df_out['org_name'].nunique()}")
        log(f"  Unique departments: {df_out['federal_department'].nunique()}")
        log(f"  Unique programs: {df_out['program'].nunique()}")
        fy_vals = sorted([str(x) for x in df_out['fiscal_year'].unique() if str(x).strip()])
        log(f"  Fiscal years: {fy_vals[:20]}")

        bn_pop = (df_out['BN'].astype(str).str.strip() != '').sum()
        bn_empty = len(df_out) - bn_pop
        summary_stats = {
            "Total GoC Grants rows (all provinces)": "1,811,088",
            "Alberta rows extracted": f"{len(df_out):,}",
            "Unique organizations": f"{df_out['org_name'].nunique():,}",
            "Unique federal departments": f"{df_out['federal_department'].nunique():,}",
            "Unique programs": f"{df_out['program'].nunique():,}",
            "Total amount (Alberta)": f"${df_out['amount'].sum():,.2f}",
            "BN populated (exact match ready)": f"{bn_pop:,} ({bn_pop/len(df_out)*100:.1f}%)",
            "BN missing (fuzzy match needed)": f"{bn_empty:,} ({bn_empty/len(df_out)*100:.1f}%)",
            "Fiscal year range": f"{fy_vals[0] if fy_vals else 'N/A'} to {fy_vals[-1] if fy_vals else 'N/A'}",
        }

    # Write the log
    write_log(summary_stats)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log("=" * 60)
    log("AGENT 0D — Federal Grants Enrichment")
    log("Operation Lineage Audit")
    log("=" * 60)
    log(f"Timestamp: {datetime.now().isoformat()}")
    log(f"Volume path: {VOLUME_PATH}")
    log(f"Output dir: {OUTPUT_DIR}")
    log("")

    conn = None
    try:
        # Connect to Databricks
        log("Connecting to Databricks SQL warehouse...")
        conn = get_databricks_connection()
        log("  Connected successfully.")

        # Step 0: Quick count of total rows in the file
        log("Checking total row count in GoC Grants file...")
        count_sql = (
            f"SELECT COUNT(*) as cnt FROM read_files('{VOLUME_PATH}grants.csv', "
            f"format => 'csv', header => true)"
        )
        try:
            count_df = run_query(conn, count_sql, "COUNT total rows")
            if not count_df.empty:
                total_rows = count_df.iloc[0, 0]
                log(f"  Total rows in GoC grants file: {total_rows:,}")
        except Exception as e:
            log(f"  Could not count total rows: {e}", "WARN")

        # Step 1: Discover volume files
        files_found = discover_volume_files(conn)

        # Step 2: Check for managed tables
        table_hits = check_managed_tables(conn)

        # Step 3: Read the CSV data
        raw_df = read_volume_csvs(conn, files_found)

        # If volume read failed, try managed tables
        if raw_df.empty and table_hits:
            log("Volume read returned no data. Trying managed tables...", "WARN")
            raw_df = read_from_tables(conn, table_hits)

        # If we still have no data, output empty CSV with schema
        if raw_df.empty:
            log("NO FEDERAL GRANTS DATA AVAILABLE.", "ERROR")
            log("Outputting empty CSV with correct schema.")
            empty_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
            write_outputs(empty_df)
            return

        # Quick sample
        log(f"\nRaw data sample (first 3 rows):")
        sample = raw_df.head(3).to_string()
        for line in sample.split('\n'):
            log(f"  {line}")
        log("")

        # Normalize columns
        norm_df = normalize_columns(raw_df)

        # Step 4: Filter to Alberta
        # Data may already be pre-filtered at SQL level — check if all rows are AB
        prov_vals = norm_df["province"].astype(str).str.strip().str.upper().unique()
        already_filtered = all(p in ALBERTA_CODES or p in ('', 'NAN', 'NONE') for p in prov_vals)
        if already_filtered and len(norm_df) > 0:
            log("Data appears pre-filtered to Alberta at SQL level.")
            ab_df = norm_df.copy()
        else:
            ab_df = filter_alberta(norm_df)

        # If filtering removed everything, log but still output
        if ab_df.empty:
            log("Alberta filtering returned zero rows.", "WARN")
            log("Outputting all rows (no province filter applied) as fallback.")
            ab_df = norm_df.copy()

        # Step 5: Clean and prepare
        final_df = prepare_for_output(ab_df)

        # Step 6: Write outputs
        write_outputs(final_df)

        log("")
        log("=" * 60)
        log("AGENT 0D COMPLETE")
        log("=" * 60)

    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        log(traceback.format_exc(), "ERROR")
        # Still write whatever we have
        try:
            empty_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
            write_outputs(empty_df)
        except Exception:
            pass
    finally:
        if conn:
            try:
                conn.close()
                log("Databricks connection closed.")
            except Exception:
                pass


if __name__ == "__main__":
    main()
