"""
agent_0a_grant_linker.py
========================
Agent 0A (Grant-Ministry Linker) for Operation Lineage Audit.

Pulls GOA grants from Databricks, tags with political eras, joins with
ministry lineage data, aggregates to Org x Ministry x FiscalYear, and
outputs CSVs ready for Neo4j ingestion.

All data sourced from Databricks (D009). Heavy aggregation done server-side
via SQL to avoid pulling 1.8M rows locally.

Table schema (discovered):
  goa_grants_disclosure: Ministry(str), BUName(str), Recipient(str),
      Program(str), Amount(str), Lottery(str), PaymentDate(str),
      FiscalYear(str), DisplayFiscalYear(str), Fiscal_Year(str),
      _rescued_data(str)
  goa_cra_matched: goa_name(str), n_ministries(bigint), goa_total(dbl),
      n_grants(bigint), ministries(array<string>), bn(str), cra_name(str),
      + various risk flags

Volume files (Ministry Data/):
  org_entities.csv  (canonical_id, name, level, status, ...)
  transform_events.csv  (event_id, event_type, event_date, ...)
"""

import sys
import os
import time
from datetime import datetime

# Handle Unicode on Windows (cp1252 breaks KGL glyphs)
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import pandas as pd
from databricks import sql as dbsql

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABRICKS_HOST = "<YOUR_DATABRICKS_HOST>"
DATABRICKS_TOKEN = "<YOUR_DATABRICKS_TOKEN>"
DATABRICKS_WAREHOUSE = "<YOUR_DATABRICKS_SQL_WAREHOUSE>"
CATALOG = "dbw_unitycatalog_test"
SCHEMA = "default"

OUTPUT_DIR = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly"

# Volume paths (space in "Ministry Data" requires read_files() not csv.``)
VOLUME_BASE = "/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data"
ORG_ENTITIES_PATH = f"{VOLUME_BASE}/org_entities.csv"
TRANSFORM_EVENTS_PATH = f"{VOLUME_BASE}/transform_events.csv"

# Political Era Boundaries (D002)
POLITICAL_ERAS = {
    'PC':         ('1900-01-01', '2015-05-23'),
    'NDP':        ('2015-05-24', '2019-04-29'),
    'UCP_Kenney': ('2019-04-30', '2022-10-10'),
    'UCP_Smith':  ('2022-10-11', '2099-12-31'),
}

LOG_LINES = []


def log(msg):
    """Print and accumulate log messages."""
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_LINES.append(line)


def get_connection():
    """Create a Databricks SQL connection."""
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_WAREHOUSE,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
        schema=SCHEMA
    )


def run_query(conn, sql, description="query"):
    """Execute SQL via cursor (avoids pandas SQLAlchemy warning) and return DataFrame."""
    log(f"Running: {description}")
    sql_preview = sql.strip().replace('\n', ' ')[:200]
    log(f"  SQL: {sql_preview}{'...' if len(sql.strip()) > 200 else ''}")
    t0 = time.time()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    df = pd.DataFrame(rows, columns=columns)
    elapsed = time.time() - t0
    log(f"  Returned {len(df):,} rows, {len(df.columns)} cols in {elapsed:.1f}s")
    return df


def main():
    log("=" * 70)
    log("Agent 0A (Grant-Ministry Linker) starting")
    log(f"Output directory: {OUTPUT_DIR}")
    log("=" * 70)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = get_connection()
    log("Databricks connection established")

    # ------------------------------------------------------------------
    # STEP 1: Verify table schemas
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 1: Verify table schemas ---")

    df_grants_schema = run_query(
        conn,
        f"DESCRIBE {CATALOG}.{SCHEMA}.goa_grants_disclosure",
        "DESCRIBE goa_grants_disclosure"
    )
    log("goa_grants_disclosure columns:")
    for _, row in df_grants_schema.iterrows():
        col_name = str(row.iloc[0])
        col_type = str(row.iloc[1])
        if not col_name.startswith('#'):
            log(f"  {col_name:40s} {col_type}")

    df_cra_schema = run_query(
        conn,
        f"DESCRIBE {CATALOG}.{SCHEMA}.goa_cra_matched",
        "DESCRIBE goa_cra_matched"
    )
    log("goa_cra_matched columns:")
    for _, row in df_cra_schema.iterrows():
        col_name = str(row.iloc[0])
        col_type = str(row.iloc[1])
        if not col_name.startswith('#'):
            log(f"  {col_name:40s} {col_type}")

    # Total row count
    df_count = run_query(
        conn,
        f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.goa_grants_disclosure",
        "COUNT grants"
    )
    total_grants = int(df_count['cnt'].iloc[0])
    log(f"Total grants rows: {total_grants:,}")

    # ------------------------------------------------------------------
    # STEP 2: Read Ministry Data from Databricks Volumes
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 2: Read Ministry Data from Databricks Volumes ---")

    # Use read_files() which handles spaces in Volume paths
    df_org_entities = run_query(
        conn,
        f"SELECT * FROM read_files('{ORG_ENTITIES_PATH}', format => 'csv', header => true)",
        "org_entities.csv from Volume"
    )
    log(f"org_entities: {len(df_org_entities)} rows, columns: {list(df_org_entities.columns)}")

    df_transform_events = run_query(
        conn,
        f"SELECT * FROM read_files('{TRANSFORM_EVENTS_PATH}', format => 'csv', header => true)",
        "transform_events.csv from Volume"
    )
    log(f"transform_events: {len(df_transform_events)} rows, columns: {list(df_transform_events.columns)}")

    # Save ministry data locally
    # Save as entity_mapping.csv (the canonical name for downstream agents)
    entity_mapping_path = os.path.join(OUTPUT_DIR, "entity_mapping.csv")
    df_org_entities.to_csv(entity_mapping_path, index=False, encoding='utf-8')
    log(f"Saved entity_mapping.csv ({len(df_org_entities)} rows) [source: org_entities.csv]")

    transform_events_path = os.path.join(OUTPUT_DIR, "transform_events.csv")
    df_transform_events.to_csv(transform_events_path, index=False, encoding='utf-8')
    log(f"Saved transform_events.csv ({len(df_transform_events)} rows)")

    # ------------------------------------------------------------------
    # STEP 3: Count NULL/blank ministry rows
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 3: Count NULL/blank ministry rows ---")

    df_null_ministry = run_query(
        conn,
        f"""
        SELECT COUNT(*) as cnt
        FROM {CATALOG}.{SCHEMA}.goa_grants_disclosure
        WHERE Ministry IS NULL OR TRIM(Ministry) = ''
        """,
        "Count NULL/blank Ministry rows"
    )
    null_ministry_count = int(df_null_ministry['cnt'].iloc[0])
    log(f"NULL/blank Ministry rows: {null_ministry_count:,} (will be EXCLUDED from aggregation)")

    # Also count NULL PaymentDate
    df_null_date = run_query(
        conn,
        f"""
        SELECT COUNT(*) as cnt
        FROM {CATALOG}.{SCHEMA}.goa_grants_disclosure
        WHERE PaymentDate IS NULL OR TRIM(PaymentDate) = ''
        """,
        "Count NULL/blank PaymentDate rows"
    )
    null_date_count = int(df_null_date['cnt'].iloc[0])
    log(f"NULL/blank PaymentDate rows: {null_date_count:,} (will be EXCLUDED from aggregation)")

    # ------------------------------------------------------------------
    # STEP 4: Main aggregation query (server-side on Databricks)
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 4: Main aggregation (server-side on Databricks) ---")
    log("Columns: Ministry, Recipient, FiscalYear, Amount(str->double), PaymentDate(str->date)")

    # Amount is stored as string (e.g. "-1125000.000") -- CAST to DOUBLE
    # PaymentDate is stored as string (e.g. "2014-04-02") -- CAST to DATE
    # FiscalYear is stored as string (e.g. "2014")
    aggregation_sql = f"""
    SELECT
        Recipient                           AS recipient,
        Ministry                            AS ministry,
        FiscalYear                          AS fiscal_year,
        CASE
            WHEN CAST(PaymentDate AS DATE) < DATE '2015-05-24'
                THEN 'PC'
            WHEN CAST(PaymentDate AS DATE) >= DATE '2015-05-24'
                 AND CAST(PaymentDate AS DATE) <= DATE '2019-04-29'
                THEN 'NDP'
            WHEN CAST(PaymentDate AS DATE) >= DATE '2019-04-30'
                 AND CAST(PaymentDate AS DATE) <= DATE '2022-10-10'
                THEN 'UCP_Kenney'
            WHEN CAST(PaymentDate AS DATE) >= DATE '2022-10-11'
                THEN 'UCP_Smith'
            ELSE 'UNKNOWN'
        END                                 AS political_era,
        SUM(CAST(Amount AS DOUBLE))         AS total_amount,
        COUNT(*)                            AS n_payments,
        MIN(CAST(PaymentDate AS DATE))      AS earliest_payment,
        MAX(CAST(PaymentDate AS DATE))      AS latest_payment
    FROM {CATALOG}.{SCHEMA}.goa_grants_disclosure
    WHERE Ministry IS NOT NULL
      AND TRIM(Ministry) <> ''
      AND PaymentDate IS NOT NULL
      AND TRIM(PaymentDate) <> ''
    GROUP BY
        Recipient,
        Ministry,
        FiscalYear,
        CASE
            WHEN CAST(PaymentDate AS DATE) < DATE '2015-05-24'
                THEN 'PC'
            WHEN CAST(PaymentDate AS DATE) >= DATE '2015-05-24'
                 AND CAST(PaymentDate AS DATE) <= DATE '2019-04-29'
                THEN 'NDP'
            WHEN CAST(PaymentDate AS DATE) >= DATE '2019-04-30'
                 AND CAST(PaymentDate AS DATE) <= DATE '2022-10-10'
                THEN 'UCP_Kenney'
            WHEN CAST(PaymentDate AS DATE) >= DATE '2022-10-11'
                THEN 'UCP_Smith'
            ELSE 'UNKNOWN'
        END
    ORDER BY total_amount DESC
    """

    df_agg = run_query(conn, aggregation_sql, "Main grants aggregation (server-side)")
    log(f"Aggregated result: {len(df_agg):,} rows (Org x Ministry x FY x Era)")

    # ------------------------------------------------------------------
    # STEP 5: Political era distribution statistics
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 5: Political era distribution ---")

    era_stats = df_agg.groupby('political_era').agg(
        groups=('recipient', 'count'),
        total_amount=('total_amount', 'sum'),
        total_payments=('n_payments', 'sum')
    ).reset_index()

    log("Political era distribution (aggregated groups):")
    log(f"  {'Era':15s} | {'Groups':>10s} | {'Total Amount':>18s} | {'Payments':>12s}")
    log(f"  {'-'*15}-+-{'-'*10}-+-{'-'*18}-+-{'-'*12}")
    for _, row in era_stats.iterrows():
        log(f"  {row['political_era']:15s} | "
            f"{int(row['groups']):>10,} | "
            f"${row['total_amount']:>17,.2f} | "
            f"{int(row['total_payments']):>12,}")

    # ------------------------------------------------------------------
    # STEP 6: Join with org_entities for canonical_ministry_id
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 6: Join with org_entities for canonical_ministry_id ---")

    # org_entities columns: canonical_id, name, level, status, start_date,
    #   end_date, normalized_name, aliases, jurisdiction, kgl_sequence, _rescued_data
    em_cols = list(df_org_entities.columns)
    log(f"org_entities columns: {em_cols}")

    # The join key: org_entities.name (uppercase ministry name) -> grants.ministry
    # Also try normalized_name and aliases for broader matching
    em_lookup = df_org_entities[['canonical_id', 'name', 'normalized_name', 'aliases']].copy()

    # Build a lookup dict: normalized ministry name -> canonical_id
    name_to_id = {}
    for _, row in em_lookup.iterrows():
        cid = row['canonical_id']
        # Primary name
        if pd.notna(row['name']):
            name_to_id[str(row['name']).strip().upper()] = cid
        # Normalized name
        if pd.notna(row['normalized_name']):
            name_to_id[str(row['normalized_name']).strip().upper()] = cid
        # Aliases (comma-separated or semicolon-separated)
        if pd.notna(row['aliases']):
            for alias in str(row['aliases']).replace(';', ',').split(','):
                alias = alias.strip().upper()
                if alias:
                    name_to_id[alias] = cid

    log(f"Built ministry name lookup: {len(name_to_id)} name variants -> canonical_id")

    # Apply lookup
    df_agg['canonical_ministry_id'] = (
        df_agg['ministry']
        .astype(str)
        .str.strip()
        .str.upper()
        .map(name_to_id)
    )

    matched = df_agg['canonical_ministry_id'].notna().sum()
    total = len(df_agg)
    log(f"Ministry name match: {matched:,} / {total:,} rows ({matched/total*100:.1f}%)")

    # Log unmatched ministry names for debugging
    unmatched_ministries = (
        df_agg[df_agg['canonical_ministry_id'].isna()]['ministry']
        .str.strip()
        .str.upper()
        .value_counts()
        .head(20)
    )
    if len(unmatched_ministries) > 0:
        log(f"Top unmatched ministry names ({len(unmatched_ministries)} shown):")
        for name, cnt in unmatched_ministries.items():
            log(f"  {name}: {cnt:,} groups")

    # ------------------------------------------------------------------
    # STEP 7: Save aggregated grants CSV
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 7: Save grants_aggregated.csv ---")

    grants_agg_path = os.path.join(OUTPUT_DIR, "grants_aggregated.csv")
    df_agg.to_csv(grants_agg_path, index=False, encoding='utf-8')
    log(f"Saved grants_aggregated.csv ({len(df_agg):,} rows)")
    log(f"  Columns: {list(df_agg.columns)}")

    # ------------------------------------------------------------------
    # STEP 8: Pull goa_cra_matched table (gold standard)
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 8: Pull goa_cra_matched (gold standard org matching) ---")

    df_cra_matched = run_query(
        conn,
        f"SELECT * FROM {CATALOG}.{SCHEMA}.goa_cra_matched",
        "goa_cra_matched full pull"
    )
    log(f"goa_cra_matched: {len(df_cra_matched):,} rows")
    log(f"  Columns: {list(df_cra_matched.columns)}")

    # The 'ministries' column is an array<string> -- convert to string for CSV
    if 'ministries' in df_cra_matched.columns:
        df_cra_matched['ministries'] = df_cra_matched['ministries'].apply(
            lambda x: '|'.join(x) if isinstance(x, (list, tuple)) else str(x) if pd.notna(x) else ''
        )
        log("  Converted 'ministries' array column to pipe-delimited string")

    cra_matched_path = os.path.join(OUTPUT_DIR, "goa_cra_matched.csv")
    df_cra_matched.to_csv(cra_matched_path, index=False, encoding='utf-8')
    log(f"Saved goa_cra_matched.csv ({len(df_cra_matched):,} rows)")

    # ------------------------------------------------------------------
    # STEP 9: Close connection
    # ------------------------------------------------------------------
    conn.close()
    log("Databricks connection closed")

    # ------------------------------------------------------------------
    # STEP 10: Write assembly log
    # ------------------------------------------------------------------
    log("")
    log("--- STEP 10: Writing grant_assembly_log.md ---")

    log_path = os.path.join(OUTPUT_DIR, "grant_assembly_log.md")
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write("# Grant Assembly Log (Agent 0A)\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Agent:** 0A (Grant-Ministry Linker)\n")
        f.write(f"**Source:** Databricks `{CATALOG}.{SCHEMA}`\n\n")

        f.write("## Data Sources\n\n")
        f.write(f"| Source | Rows | Notes |\n")
        f.write(f"|--------|------|-------|\n")
        f.write(f"| `goa_grants_disclosure` | {total_grants:,} | All GOA grants 2014-2025 |\n")
        f.write(f"| `goa_cra_matched` | {len(df_cra_matched):,} | Gold standard GOA-CRA name matches |\n")
        f.write(f"| `org_entities.csv` (Volume) | {len(df_org_entities):,} | Ministry entity mapping |\n")
        f.write(f"| `transform_events.csv` (Volume) | {len(df_transform_events):,} | Ministry restructuring events |\n\n")

        f.write("## Political Era Boundaries (D002)\n\n")
        f.write("| Era | Start | End | Premier |\n")
        f.write("|-----|-------|-----|---------|\n")
        f.write("| PC | pre-2015 | 2015-05-23 | Various PCs |\n")
        f.write("| NDP | 2015-05-24 | 2019-04-29 | Rachel Notley |\n")
        f.write("| UCP_Kenney | 2019-04-30 | 2022-10-10 | Jason Kenney |\n")
        f.write("| UCP_Smith | 2022-10-11 | present | Danielle Smith |\n\n")

        f.write("## Political Era Distribution (Aggregated)\n\n")
        f.write("| Era | Agg Groups | Total Amount | Total Payments |\n")
        f.write("|-----|-----------|-------------|----------------|\n")
        for _, row in era_stats.iterrows():
            f.write(f"| {row['political_era']} | "
                    f"{int(row['groups']):,} | "
                    f"${row['total_amount']:,.2f} | "
                    f"{int(row['total_payments']):,} |\n")
        f.write("\n")

        f.write("## Data Quality Notes\n\n")
        f.write(f"- NULL/blank Ministry rows excluded: **{null_ministry_count:,}**\n")
        f.write(f"- NULL/blank PaymentDate rows excluded: **{null_date_count:,}**\n")
        f.write(f"- Entity mapping match rate: **{matched:,} / {total:,}** "
                f"({matched/total*100:.1f}%)\n")
        f.write(f"- Aggregation performed server-side on Databricks SQL Warehouse "
                f"(1.8M rows never pulled locally)\n")
        f.write(f"- Amount column is stored as STRING in source; "
                f"CAST to DOUBLE for aggregation\n")
        f.write(f"- PaymentDate stored as STRING; CAST to DATE for era assignment\n")
        if len(unmatched_ministries) > 0:
            f.write(f"- {len(unmatched_ministries)} unmatched ministry name variants found "
                    f"(see log for details)\n")
        f.write("\n")

        f.write("## Output Files\n\n")
        f.write("| File | Rows | Description |\n")
        f.write("|------|------|-------------|\n")
        f.write(f"| `grants_aggregated.csv` | {len(df_agg):,} | "
                f"Org x Ministry x FY x Era (main output) |\n")
        f.write(f"| `goa_cra_matched.csv` | {len(df_cra_matched):,} | "
                f"Gold standard GOA-CRA name matches |\n")
        f.write(f"| `entity_mapping.csv` | {len(df_org_entities):,} | "
                f"Ministry entity mapping (from org_entities.csv) |\n")
        f.write(f"| `transform_events.csv` | {len(df_transform_events):,} | "
                f"Ministry restructuring events |\n\n")

        f.write("## grants_aggregated.csv Schema\n\n")
        f.write("| Column | Description |\n")
        f.write("|--------|-------------|\n")
        f.write("| `recipient` | GOA grant recipient name |\n")
        f.write("| `ministry` | Granting ministry name |\n")
        f.write("| `fiscal_year` | Fiscal year (e.g. 2014) |\n")
        f.write("| `political_era` | PC / NDP / UCP_Kenney / UCP_Smith |\n")
        f.write("| `total_amount` | Sum of grant amounts (DOUBLE) |\n")
        f.write("| `n_payments` | Count of individual payment records |\n")
        f.write("| `earliest_payment` | First payment date in group |\n")
        f.write("| `latest_payment` | Last payment date in group |\n")
        f.write("| `canonical_ministry_id` | Joined from org_entities (e.g. EM-001) |\n\n")

        f.write("## Execution Log\n\n")
        f.write("```\n")
        for line in LOG_LINES:
            f.write(line + "\n")
        f.write("```\n")

    log(f"Saved grant_assembly_log.md")

    # ------------------------------------------------------------------
    # Final Summary
    # ------------------------------------------------------------------
    log("")
    log("=" * 70)
    log("Agent 0A (Grant-Ministry Linker) COMPLETE")
    log(f"  grants_aggregated.csv  : {len(df_agg):,} rows")
    log(f"  goa_cra_matched.csv    : {len(df_cra_matched):,} rows")
    log(f"  entity_mapping.csv     : {len(df_org_entities):,} rows")
    log(f"  transform_events.csv   : {len(df_transform_events):,} rows")
    log(f"  grant_assembly_log.md  : written")
    log(f"  NULL Ministry excluded : {null_ministry_count:,}")
    log(f"  NULL PaymentDate excl. : {null_date_count:,}")
    log(f"  Ministry match rate    : {matched:,}/{total:,} ({matched/total*100:.1f}%)")
    log("=" * 70)


if __name__ == "__main__":
    main()
