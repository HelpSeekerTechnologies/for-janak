"""
agent_0d_federal_grants_v2.py
=============================
Agent 0D — Federal Grants Enrichment for Operation Lineage Audit (v2 fix)

Fixes the v1 issue where all columns came back as _c0, _c1, etc.
This version does SERVER-SIDE filtering and column aliasing on Databricks
so we only pull Alberta rows with the columns we need.

Approach:
  1. Try read_files() with header => true and filter province = 'AB'
  2. If columns still come back as _c0, fall back to positional aliasing
     with manual header skip (WHERE _c0 != 'ref_number')

Data source: Databricks Volume at
  /Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/grants.csv

Output:
  - federal_grants.csv   — BN, org_name, federal_department, program, amount,
                            agreement_start_date, province
  - federal_grants_log.md — full audit log
"""

import sys
import os
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
CSV_FILE = VOLUME_PATH + "grants.csv"

OUTPUT_DIR = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "federal_grants.csv")
OUTPUT_LOG = os.path.join(OUTPUT_DIR, "federal_grants_log.md")

# Target output columns
OUTPUT_COLUMNS = [
    "BN", "org_name", "federal_department", "program",
    "amount", "agreement_start_date", "province"
]

# Column position map from the v1 log (row 0 = header row in the raw CSV):
#   _c0  = ref_number
#   _c1  = amendment_number
#   _c5  = recipient_business_number  (BN for matching)
#   _c6  = recipient_legal_name       (org name)
#   _c7  = recipient_operating_name
#   _c9  = recipient_country
#   _c10 = recipient_province         (FILTER FOR AB)
#   _c11 = recipient_city
#   _c16 = prog_name_en               (program)
#   _c22 = agreement_value            (amount)
#   _c25 = agreement_start_date
#   _c26 = agreement_end_date
#   _c36 = owner_org                  (department code)
#   _c37 = owner_org_title            (department name)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LINES = []


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] [{level}] {msg}"
    LOG_LINES.append(entry)
    print(entry)


def write_log_file(df):
    """Write the markdown audit log."""
    with open(OUTPUT_LOG, "w", encoding="utf-8") as f:
        f.write("# Agent 0D -- Federal Grants Enrichment Log (v2)\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("---\n\n")

        # Summary stats
        f.write("## Summary Statistics\n\n")
        if not df.empty:
            f.write(f"- **Total Alberta rows:** {len(df):,}\n")
            f.write(f"- **Unique BNs (non-empty):** {df[df['BN'] != '']['BN'].nunique():,}\n")
            f.write(f"- **Unique organizations:** {df['org_name'].nunique():,}\n")
            f.write(f"- **Unique federal departments:** {df['federal_department'].nunique():,}\n")
            f.write(f"- **Unique programs:** {df['program'].nunique():,}\n")
            total_amt = df['amount'].sum()
            f.write(f"- **Total agreement value:** ${total_amt:,.2f}\n")
            f.write(f"- **Amount range:** ${df['amount'].min():,.2f} to ${df['amount'].max():,.2f}\n")
            f.write(f"- **Median amount:** ${df['amount'].median():,.2f}\n")
            f.write(f"- **BN populated:** {(df['BN'] != '').sum():,} / {len(df):,}\n")
            f.write(f"- **BN empty:** {(df['BN'] == '').sum():,} / {len(df):,}\n\n")

            # Top departments
            f.write("### Top 15 Federal Departments (by row count)\n\n")
            dept_counts = df['federal_department'].value_counts().head(15)
            for dept, cnt in dept_counts.items():
                f.write(f"- {dept}: {cnt:,}\n")
            f.write("\n")

            # Amount distribution by department
            f.write("### Amount by Department (top 15)\n\n")
            dept_amt = df.groupby('federal_department')['amount'].sum().sort_values(ascending=False).head(15)
            for dept, amt in dept_amt.items():
                f.write(f"- {dept}: ${amt:,.2f}\n")
            f.write("\n")
        else:
            f.write("- **No data extracted.**\n\n")

        # Full execution log
        f.write("---\n\n")
        f.write("## Execution Log\n\n")
        f.write("```\n")
        for line in LOG_LINES:
            f.write(line + "\n")
        f.write("```\n")

    print(f"\nLog written to: {OUTPUT_LOG}")


# ---------------------------------------------------------------------------
# Databricks connection
# ---------------------------------------------------------------------------
def get_databricks_connection():
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
    log(f"Executing {description}...")
    log(f"  SQL: {sql[:300]}{'...' if len(sql) > 300 else ''}")
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        cursor.close()
        df = pd.DataFrame(rows, columns=columns)
        log(f"  -> {len(df):,} rows, {len(df.columns)} columns")
        if columns:
            log(f"  -> Columns: {columns[:15]}{'...' if len(columns) > 15 else ''}")
        return df
    except Exception as e:
        log(f"  -> FAILED: {e}", "ERROR")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# BN cleaning
# ---------------------------------------------------------------------------
def clean_bn(val):
    """Normalize a Business Number value."""
    if pd.isna(val) or str(val).strip().lower() in ('', 'none', 'null', 'nan'):
        return ""
    s = str(val).strip().upper()
    s = re.sub(r'[\s\-]', '', s)
    # Full BN: 9 digits + 2 letters + 4 digits
    if re.match(r'^\d{9}[A-Z]{2}\d{4}$', s):
        return s
    # 9-digit root
    if re.match(r'^\d{9}$', s):
        return s
    # Try to extract
    match = re.search(r'(\d{9}[A-Z]{2}\d{4})', s)
    if match:
        return match.group(1)
    match = re.search(r'(\d{9})', s)
    if match:
        return match.group(1)
    return s


def clean_amount(val):
    """Parse an amount value to float."""
    if pd.isna(val) or str(val).strip().lower() in ('', 'none', 'null', 'nan'):
        return 0.0
    s = str(val).strip()
    s = re.sub(r'[\$,\s]', '', s)
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Main extraction logic
# ---------------------------------------------------------------------------
def main():
    log("=" * 60)
    log("AGENT 0D v2 -- Federal Grants Enrichment")
    log("Operation Lineage Audit")
    log("=" * 60)
    log(f"Timestamp: {datetime.now().isoformat()}")
    log(f"CSV file: {CSV_FILE}")
    log(f"Output dir: {OUTPUT_DIR}")
    log("")

    conn = None
    final_df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    try:
        # ---------------------------------------------------------------
        # Connect
        # ---------------------------------------------------------------
        log("Connecting to Databricks SQL warehouse...")
        conn = get_databricks_connection()
        log("  Connected successfully.")

        # ---------------------------------------------------------------
        # APPROACH 1: read_files with header => true
        # ---------------------------------------------------------------
        log("")
        log("=" * 60)
        log("APPROACH 1: read_files() with header => true")
        log("=" * 60)

        # First test with LIMIT 5 to check if headers are real
        sample_sql = (
            f"SELECT * FROM read_files('{CSV_FILE}', "
            f"format => 'csv', header => true) LIMIT 5"
        )
        df_sample = run_query(conn, sample_sql, "SAMPLE read_files with header")

        approach1_ok = False
        if not df_sample.empty:
            cols = list(df_sample.columns)
            log(f"  Sample columns: {cols[:15]}")

            # Check if columns are real names or still _c0, _c1, etc.
            has_real_headers = any(
                c.lower() in ('ref_number', 'recipient_business_number',
                               'recipient_legal_name', 'recipient_province',
                               'agreement_value', 'owner_org_title', 'prog_name_en')
                for c in cols
            )

            if has_real_headers:
                log("  Headers detected correctly! Using approach 1.")
                approach1_ok = True

                # Now run the full query with Alberta filter, server-side
                full_sql = (
                    f"SELECT "
                    f"  recipient_business_number AS BN, "
                    f"  recipient_legal_name AS org_name, "
                    f"  owner_org_title AS federal_department, "
                    f"  prog_name_en AS program, "
                    f"  CAST(agreement_value AS DOUBLE) AS amount, "
                    f"  agreement_start_date, "
                    f"  recipient_province AS province "
                    f"FROM read_files('{CSV_FILE}', "
                    f"format => 'csv', header => true) "
                    f"WHERE UPPER(TRIM(recipient_province)) = 'AB'"
                )
                final_df = run_query(conn, full_sql, "FULL Alberta extract (approach 1)")
            else:
                log("  Columns are still positional (_c0 etc.). Approach 1 failed.")
                log(f"  First 5 columns: {cols[:5]}")

        if not approach1_ok:
            # -----------------------------------------------------------
            # APPROACH 2: csv.`` with positional aliasing + header skip
            # -----------------------------------------------------------
            log("")
            log("=" * 60)
            log("APPROACH 2: positional column aliasing with header skip")
            log("=" * 60)

            # Verify the header row content first
            verify_sql = (
                f"SELECT _c0, _c5, _c6, _c10, _c16, _c22, _c25, _c37 "
                f"FROM csv.`{CSV_FILE}` "
                f"WHERE _c0 = 'ref_number' LIMIT 1"
            )
            df_verify = run_query(conn, verify_sql, "VERIFY header row")
            if not df_verify.empty:
                log(f"  Header row confirmed: {df_verify.iloc[0].tolist()}")
            else:
                log("  Could not find header row with _c0 = 'ref_number'", "WARN")
                log("  Proceeding anyway with positional mapping...")

            # Full extraction with Alberta filter, server-side
            full_sql = (
                f"SELECT "
                f"  _c5 AS BN, "
                f"  _c6 AS org_name, "
                f"  _c37 AS federal_department, "
                f"  _c16 AS program, "
                f"  CAST(_c22 AS DOUBLE) AS amount, "
                f"  _c25 AS agreement_start_date, "
                f"  _c10 AS province "
                f"FROM csv.`{CSV_FILE}` "
                f"WHERE _c0 != 'ref_number' "
                f"AND UPPER(TRIM(_c10)) = 'AB'"
            )
            final_df = run_query(conn, full_sql, "FULL Alberta extract (approach 2)")

        # ---------------------------------------------------------------
        # Get total row count for logging (lightweight COUNT)
        # ---------------------------------------------------------------
        log("")
        log("Getting total row counts for audit...")

        # Total rows in file
        count_sql = (
            f"SELECT COUNT(*) AS cnt "
            f"FROM csv.`{CSV_FILE}` "
            f"WHERE _c0 != 'ref_number'"
        )
        df_count = run_query(conn, count_sql, "COUNT total rows (excl header)")
        total_rows = 0
        if not df_count.empty:
            total_rows = int(df_count.iloc[0, 0])
            log(f"  Total rows in CSV (excl header): {total_rows:,}")

        # ---------------------------------------------------------------
        # Process results
        # ---------------------------------------------------------------
        log("")
        log("=" * 60)
        log("POST-PROCESSING")
        log("=" * 60)

        if final_df.empty:
            log("NO DATA RETURNED. Writing empty output.", "ERROR")
        else:
            log(f"Raw Alberta rows returned: {len(final_df):,}")
            log(f"Columns: {list(final_df.columns)}")

            # Show sample
            log("")
            log("Sample (first 5 rows):")
            for idx, row in final_df.head(5).iterrows():
                log(f"  [{idx}] BN={row.get('BN','?')} | org={str(row.get('org_name','?'))[:50]} | "
                    f"dept={str(row.get('federal_department','?'))[:40]} | "
                    f"amt={row.get('amount','?')} | prog={str(row.get('program','?'))[:40]}")

            # Clean BN
            log("")
            log("Cleaning BN values...")
            final_df["BN"] = final_df["BN"].apply(clean_bn)
            bn_populated = (final_df["BN"] != "").sum()
            bn_empty = (final_df["BN"] == "").sum()
            log(f"  BN populated: {bn_populated:,} / {len(final_df):,}")
            log(f"  BN empty:     {bn_empty:,} / {len(final_df):,}")
            log(f"  Unique BNs (non-empty): {final_df[final_df['BN'] != '']['BN'].nunique():,}")

            # Clean amount
            log("Cleaning amount values...")
            final_df["amount"] = final_df["amount"].apply(clean_amount)
            total_amount = final_df["amount"].sum()
            log(f"  Total agreement value (Alberta): ${total_amount:,.2f}")
            log(f"  Amount range: ${final_df['amount'].min():,.2f} to ${final_df['amount'].max():,.2f}")
            log(f"  Median: ${final_df['amount'].median():,.2f}")
            log(f"  Mean:   ${final_df['amount'].mean():,.2f}")

            # Amount distribution buckets
            log("")
            log("Amount distribution:")
            buckets = [
                (0, 0, "$0 (zero)"),
                (0.01, 10000, "$0.01 - $10K"),
                (10000, 100000, "$10K - $100K"),
                (100000, 1000000, "$100K - $1M"),
                (1000000, 10000000, "$1M - $10M"),
                (10000000, float('inf'), "$10M+"),
            ]
            for lo, hi, label in buckets:
                if lo == 0 and hi == 0:
                    cnt = (final_df["amount"] == 0).sum()
                else:
                    cnt = ((final_df["amount"] > lo) & (final_df["amount"] <= hi)).sum()
                log(f"  {label}: {cnt:,}")

            # Trim string columns
            for col in ["org_name", "federal_department", "program",
                        "agreement_start_date", "province"]:
                if col in final_df.columns:
                    final_df[col] = final_df[col].astype(str).str.strip()

            # Unique departments
            log("")
            log(f"Unique federal departments: {final_df['federal_department'].nunique()}")
            dept_counts = final_df["federal_department"].value_counts().head(20)
            for dept, cnt in dept_counts.items():
                log(f"  {dept}: {cnt:,}")

            # Unique programs
            log("")
            log(f"Unique programs: {final_df['program'].nunique()}")
            prog_counts = final_df["program"].value_counts().head(15)
            for prog, cnt in prog_counts.items():
                log(f"  {prog}: {cnt:,}")

            # Dedup
            before_dedup = len(final_df)
            final_df = final_df.drop_duplicates()
            after_dedup = len(final_df)
            if before_dedup != after_dedup:
                log(f"\nDeduplication: {before_dedup:,} -> {after_dedup:,} "
                    f"(removed {before_dedup - after_dedup:,} exact duplicates)")

        # ---------------------------------------------------------------
        # Write output CSV
        # ---------------------------------------------------------------
        log("")
        log("=" * 60)
        log("WRITING OUTPUT")
        log("=" * 60)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Ensure correct column order
        for col in OUTPUT_COLUMNS:
            if col not in final_df.columns:
                final_df[col] = ""
        final_df = final_df[OUTPUT_COLUMNS]

        final_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        log(f"Written: {OUTPUT_CSV}")
        log(f"Rows: {len(final_df):,}")
        log(f"File size: {os.path.getsize(OUTPUT_CSV):,} bytes")

        # ---------------------------------------------------------------
        # Final summary
        # ---------------------------------------------------------------
        log("")
        log("=" * 60)
        log("FINAL SUMMARY")
        log("=" * 60)
        log(f"Total rows in GoC grants CSV:    {total_rows:,}")
        log(f"Alberta rows extracted:           {len(final_df):,}")
        if total_rows > 0:
            pct = len(final_df) / total_rows * 100
            log(f"Alberta as % of total:           {pct:.1f}%")
        log(f"Unique BNs (non-empty):          {final_df[final_df['BN'] != '']['BN'].nunique() if not final_df.empty else 0:,}")
        log(f"Unique organizations:            {final_df['org_name'].nunique() if not final_df.empty else 0:,}")
        log(f"Unique federal departments:      {final_df['federal_department'].nunique() if not final_df.empty else 0:,}")
        log(f"Unique programs:                 {final_df['program'].nunique() if not final_df.empty else 0:,}")
        if not final_df.empty:
            log(f"Total agreement value:           ${final_df['amount'].sum():,.2f}")

        log("")
        log("AGENT 0D v2 COMPLETE")
        log("=" * 60)

        # Write the log file
        write_log_file(final_df)

    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        log(traceback.format_exc(), "ERROR")
        # Still write empty output
        try:
            empty_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
            empty_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
            write_log_file(empty_df)
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
