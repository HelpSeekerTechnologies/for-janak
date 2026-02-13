"""
agent_0b_director_network.py
============================
Agent 0B — Director Network Builder for Operation Lineage Audit.

Pulls director network data from Databricks and prepares CSVs ready
for Neo4j ingestion:
  1. multi_board_directors     (19,156 rows — directors on 3+ boards)
  2. org_clusters_strong       (4,636 rows — cluster assignments)
  3. ab_org_risk_flags          (9,145 rows — risk flags)
  4. org_network_edges_filtered (154,015 rows — org-to-org edges)

Outputs go to: 01-data-assembly/
  - multi_board_directors.csv
  - org_clusters.csv
  - org_risk_flags.csv
  - org_network_edges.csv
  - director_assembly_log.md
"""

import sys
import os
import time
from datetime import datetime, timezone

# Handle Windows Unicode issues
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

# Tables to pull
TABLES = {
    "multi_board_directors": {
        "full_name": f"{CATALOG}.{SCHEMA}.multi_board_directors",
        "output_csv": "multi_board_directors.csv",
        "expected_rows": 19156,
        "description": "Directors sitting on 3+ charity boards",
    },
    "org_clusters_strong": {
        "full_name": f"{CATALOG}.{SCHEMA}.org_clusters_strong",
        "output_csv": "org_clusters.csv",
        "expected_rows": 4636,
        "description": "Pre-computed governance clusters",
    },
    "ab_org_risk_flags": {
        "full_name": f"{CATALOG}.{SCHEMA}.ab_org_risk_flags",
        "output_csv": "org_risk_flags.csv",
        "expected_rows": 9145,
        "description": "Organizations with risk flags",
    },
    "org_network_edges_filtered": {
        "full_name": f"{CATALOG}.{SCHEMA}.org_network_edges_filtered",
        "output_csv": "org_network_edges.csv",
        "expected_rows": 154015,
        "description": "Org-to-org shared director edges",
    },
}

# Optional table to probe
OPTIONAL_TABLE = f"{CATALOG}.{SCHEMA}.ab_master_profile"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log_lines = []  # accumulate log for MD output


def log(msg: str) -> None:
    """Print and buffer a log line."""
    print(msg)
    log_lines.append(msg)


def get_connection():
    """Return a fresh Databricks SQL connection."""
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_WAREHOUSE,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
        schema=SCHEMA,
    )


def describe_table(conn, table_full_name: str) -> pd.DataFrame | None:
    """Run DESCRIBE on a table; return DataFrame or None on error."""
    try:
        df = pd.read_sql(f"DESCRIBE TABLE {table_full_name}", conn)
        return df
    except Exception as exc:
        log(f"  WARNING: DESCRIBE failed for {table_full_name}: {exc}")
        return None


def pull_table(conn, table_full_name: str) -> pd.DataFrame | None:
    """SELECT * from a table; return DataFrame or None on error."""
    try:
        t0 = time.time()
        df = pd.read_sql(f"SELECT * FROM {table_full_name}", conn)
        elapsed = time.time() - t0
        log(f"  Pulled {len(df):,} rows in {elapsed:.1f}s")
        return df
    except Exception as exc:
        log(f"  ERROR pulling {table_full_name}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    run_start = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log(f"Agent 0B — Director Network Builder")
    log(f"Run started: {run_start}")
    log(f"Output directory: {OUTPUT_DIR}")
    log("")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = get_connection()
    log("Connected to Databricks SQL warehouse.")
    log("")

    # ------------------------------------------------------------------
    # Step 1: DESCRIBE each table
    # ------------------------------------------------------------------
    log("=" * 60)
    log("STEP 1: Explore table schemas (DESCRIBE)")
    log("=" * 60)

    schemas_info = {}
    all_tables_to_describe = list(TABLES.keys()) + ["ab_master_profile"]

    for tname in all_tables_to_describe:
        if tname in TABLES:
            full = TABLES[tname]["full_name"]
        else:
            full = OPTIONAL_TABLE
        log(f"\n--- DESCRIBE {full} ---")
        desc_df = describe_table(conn, full)
        if desc_df is not None:
            schemas_info[tname] = desc_df
            for _, row in desc_df.iterrows():
                col_name = row.iloc[0] if len(row) > 0 else "?"
                col_type = row.iloc[1] if len(row) > 1 else "?"
                log(f"  {col_name:40s}  {col_type}")
        else:
            schemas_info[tname] = None
            log(f"  (table not accessible or does not exist)")

    log("")

    # ------------------------------------------------------------------
    # Steps 2-5: Pull each table
    # ------------------------------------------------------------------
    dataframes = {}

    for step_num, (tname, tinfo) in enumerate(TABLES.items(), start=2):
        log("=" * 60)
        log(f"STEP {step_num}: Pull {tname}")
        log(f"  Table: {tinfo['full_name']}")
        log(f"  Description: {tinfo['description']}")
        log(f"  Expected rows: ~{tinfo['expected_rows']:,}")
        log("=" * 60)

        df = pull_table(conn, tinfo["full_name"])
        if df is not None:
            dataframes[tname] = df
            csv_path = os.path.join(OUTPUT_DIR, tinfo["output_csv"])
            df.to_csv(csv_path, index=False, encoding="utf-8")
            log(f"  Saved to: {csv_path}")
            log(f"  Columns: {list(df.columns)}")
            log(f"  Shape: {df.shape}")
        else:
            dataframes[tname] = None
            log(f"  SKIPPED — could not pull data.")
        log("")

    conn.close()
    log("Databricks connection closed.")
    log("")

    # ------------------------------------------------------------------
    # Step 6: Build summary statistics
    # ------------------------------------------------------------------
    log("=" * 60)
    log("STEP 6: Summary Statistics")
    log("=" * 60)
    log("")

    # --- 6a. Multi-board directors stats ---
    df_dirs = dataframes.get("multi_board_directors")
    if df_dirs is not None and len(df_dirs) > 0:
        log("--- Multi-Board Directors ---")
        log(f"  Total rows: {len(df_dirs):,}")

        # Try to find director identifier column
        dir_col = None
        for candidate in ["director_name", "normalized_name", "clean_name_no_initial",
                          "clean_name", "name", "director_id",
                          "full_name", "director", "Director", "DIRECTOR_NAME"]:
            if candidate in df_dirs.columns:
                dir_col = candidate
                break
        # Fallback: if columns contain 'last' and 'first', build a name
        if dir_col is None:
            last_col = next((c for c in df_dirs.columns if "last" in c.lower()), None)
            first_col = next((c for c in df_dirs.columns if "first" in c.lower()), None)
            if last_col and first_col:
                df_dirs["_director_name"] = (
                    df_dirs[last_col].astype(str).str.strip() + ", " +
                    df_dirs[first_col].astype(str).str.strip()
                )
                dir_col = "_director_name"
                log(f"  Built director name from [{last_col}] + [{first_col}]")

        if dir_col:
            n_unique_dirs = df_dirs[dir_col].nunique()
            log(f"  Unique directors: {n_unique_dirs:,}  (column: {dir_col})")
        else:
            log(f"  Could not identify a director name column. Columns: {list(df_dirs.columns)}")

        # Try to find org/BN column
        org_col = None
        for candidate in ["bn", "BN", "org_bn", "business_number", "charity_bn",
                          "org_name", "legal_name", "organization"]:
            if candidate in df_dirs.columns:
                org_col = candidate
                break
        if org_col:
            n_unique_orgs_dirs = df_dirs[org_col].nunique()
            log(f"  Unique organizations (in director table): {n_unique_orgs_dirs:,}  (column: {org_col})")
        else:
            log(f"  Could not identify an org column. Columns: {list(df_dirs.columns)}")

        # Boards-per-director distribution
        boards_col = None
        for candidate in ["n_boards", "board_count", "num_boards", "boards"]:
            if candidate in df_dirs.columns:
                boards_col = candidate
                break
        if boards_col is None and dir_col:
            # compute from data
            board_counts = df_dirs.groupby(dir_col).size()
            log(f"  Boards-per-director distribution (computed):")
            log(f"    min={board_counts.min()}, max={board_counts.max()}, "
                f"median={board_counts.median():.1f}, mean={board_counts.mean():.2f}")
        elif boards_col:
            bc = df_dirs[boards_col]
            log(f"  Boards-per-director ({boards_col}):")
            log(f"    min={bc.min()}, max={bc.max()}, "
                f"median={bc.median():.1f}, mean={bc.mean():.2f}")

        log("")
    else:
        log("--- Multi-Board Directors: NO DATA ---")
        log("")

    # --- 6b. Clusters stats ---
    df_clusters = dataframes.get("org_clusters_strong")
    if df_clusters is not None and len(df_clusters) > 0:
        log("--- Governance Clusters ---")
        log(f"  Total rows: {len(df_clusters):,}")
        log(f"  Columns: {list(df_clusters.columns)}")

        # Find cluster ID column
        cluster_col = None
        for candidate in ["cluster_id", "cluster", "component_id", "component",
                          "cluster_label", "group_id"]:
            if candidate in df_clusters.columns:
                cluster_col = candidate
                break
        if cluster_col is None:
            # pick first column that looks numeric or has 'id' in name
            for c in df_clusters.columns:
                if "id" in c.lower() or "cluster" in c.lower():
                    cluster_col = c
                    break

        if cluster_col:
            cluster_sizes = df_clusters.groupby(cluster_col).size()
            n_clusters = cluster_sizes.shape[0]
            log(f"  Unique clusters: {n_clusters:,}  (column: {cluster_col})")
            log(f"  Cluster size distribution:")
            log(f"    min={cluster_sizes.min()}, max={cluster_sizes.max()}, "
                f"median={cluster_sizes.median():.1f}, mean={cluster_sizes.mean():.2f}")
            log("")
            log(f"  Top 10 largest clusters by member count:")
            top10 = cluster_sizes.nlargest(10)
            for rank, (cid, size) in enumerate(top10.items(), 1):
                log(f"    #{rank}: cluster {cid} — {size} members")
        else:
            log(f"  Could not identify cluster column.")

        # Unique orgs in clusters
        org_col_c = None
        for candidate in ["bn", "BN", "org_bn", "business_number", "charity_bn",
                          "org_name", "legal_name"]:
            if candidate in df_clusters.columns:
                org_col_c = candidate
                break
        if org_col_c:
            log(f"  Unique organizations in clusters: {df_clusters[org_col_c].nunique():,}")

        log("")
    else:
        log("--- Governance Clusters: NO DATA ---")
        log("")

    # --- 6c. Risk flags stats ---
    df_risk = dataframes.get("ab_org_risk_flags")
    if df_risk is not None and len(df_risk) > 0:
        log("--- Risk Flags ---")
        log(f"  Total rows: {len(df_risk):,}")
        log(f"  Columns: {list(df_risk.columns)}")

        # Find risk flag type column
        flag_col = None
        for candidate in ["flag_type", "risk_flag", "flag", "risk_type",
                          "flag_name", "category", "risk_category"]:
            if candidate in df_risk.columns:
                flag_col = candidate
                break

        if flag_col:
            flag_dist = df_risk[flag_col].value_counts()
            log(f"  Risk flag type distribution (column: {flag_col}):")
            for ftype, count in flag_dist.items():
                log(f"    {ftype}: {count:,}")
        else:
            # Check for boolean flag columns (common pattern: multiple flag columns)
            bool_cols = [c for c in df_risk.columns
                         if df_risk[c].dtype in ['bool', 'int64', 'float64']
                         and df_risk[c].isin([0, 1, True, False]).all()
                         and "flag" in c.lower() or "risk" in c.lower()]
            if not bool_cols:
                # broader search: any boolean-ish column
                bool_cols = [c for c in df_risk.columns
                             if c.lower() not in ('bn', 'org_name', 'legal_name')
                             and df_risk[c].dtype in ['bool', 'int64', 'float64']
                             and set(df_risk[c].dropna().unique()).issubset({0, 1, True, False, 0.0, 1.0})]
            if bool_cols:
                log(f"  Risk flag columns detected (boolean pattern):")
                for c in bool_cols:
                    count_flagged = df_risk[c].sum()
                    log(f"    {c}: {int(count_flagged):,} flagged")
            else:
                log(f"  Could not identify risk flag type column. Attempting value_counts on all columns...")
                # Show unique-value counts for columns with <20 uniques
                for c in df_risk.columns:
                    nu = df_risk[c].nunique()
                    if nu <= 20 and nu > 0:
                        log(f"  Column '{c}' ({nu} unique values):")
                        for val, cnt in df_risk[c].value_counts().head(20).items():
                            log(f"    {val}: {cnt:,}")

        # Unique orgs with risk flags
        org_col_r = None
        for candidate in ["bn", "BN", "org_bn", "business_number", "charity_bn"]:
            if candidate in df_risk.columns:
                org_col_r = candidate
                break
        if org_col_r:
            log(f"  Unique organizations with risk flags: {df_risk[org_col_r].nunique():,}")

        log("")
    else:
        log("--- Risk Flags: NO DATA ---")
        log("")

    # --- 6d. Network edges stats ---
    df_edges = dataframes.get("org_network_edges_filtered")
    if df_edges is not None and len(df_edges) > 0:
        log("--- Network Edges ---")
        log(f"  Total edges: {len(df_edges):,}")
        log(f"  Columns: {list(df_edges.columns)}")

        # Find source/target columns
        src_col = None
        tgt_col = None
        for candidate in [("org1", "org2"), ("source", "target"),
                          ("bn1", "bn2"), ("org_bn_1", "org_bn_2"),
                          ("from", "to"), ("source_bn", "target_bn"),
                          ("org1_bn", "org2_bn")]:
            if candidate[0] in df_edges.columns and candidate[1] in df_edges.columns:
                src_col, tgt_col = candidate
                break
        if src_col is None:
            # Try partial matching
            cols_lower = {c.lower(): c for c in df_edges.columns}
            for s, t in [("source", "target"), ("org1", "org2"), ("from", "to")]:
                s_match = next((cols_lower[k] for k in cols_lower if s in k), None)
                t_match = next((cols_lower[k] for k in cols_lower if t in k), None)
                if s_match and t_match:
                    src_col, tgt_col = s_match, t_match
                    break

        if src_col and tgt_col:
            all_nodes = set(df_edges[src_col].unique()) | set(df_edges[tgt_col].unique())
            log(f"  Unique organizations in edges: {len(all_nodes):,}")
            log(f"  Source column: {src_col}, Target column: {tgt_col}")

            # Shared directors weight column
            weight_col = None
            for candidate in ["shared_directors", "weight", "n_shared", "shared_count",
                              "num_shared_directors", "shared"]:
                if candidate in df_edges.columns:
                    weight_col = candidate
                    break
            if weight_col:
                w = df_edges[weight_col]
                log(f"  Shared directors per edge ({weight_col}):")
                log(f"    min={w.min()}, max={w.max()}, "
                    f"median={w.median():.1f}, mean={w.mean():.2f}")
        else:
            log(f"  Could not identify source/target columns.")

        log("")
    else:
        log("--- Network Edges: NO DATA ---")
        log("")

    # ------------------------------------------------------------------
    # Grand totals across all tables
    # ------------------------------------------------------------------
    log("=" * 60)
    log("GRAND TOTALS")
    log("=" * 60)

    # Unique directors (from multi_board_directors)
    if df_dirs is not None and dir_col and dir_col in df_dirs.columns:
        log(f"  Unique directors (multi-board): {df_dirs[dir_col].nunique():,}")
    elif df_dirs is not None and "_director_name" in df_dirs.columns:
        log(f"  Unique directors (multi-board): {df_dirs['_director_name'].nunique():,}")

    # Unique organizations (union across all tables)
    all_org_ids = set()
    for tname, df in dataframes.items():
        if df is None:
            continue
        for candidate in ["bn", "BN", "org_bn", "business_number", "charity_bn"]:
            if candidate in df.columns:
                all_org_ids.update(df[candidate].dropna().unique())
                break
    if all_org_ids:
        log(f"  Unique organizations (union across all tables): {len(all_org_ids):,}")
    else:
        log(f"  Could not compute unique org count (no BN column found).")

    # Table row counts
    log("")
    log("  Table row counts:")
    for tname, df in dataframes.items():
        if df is not None:
            expected = TABLES[tname]["expected_rows"]
            delta = len(df) - expected
            delta_str = f"+{delta}" if delta >= 0 else str(delta)
            log(f"    {tname:40s}  {len(df):>10,} rows  (expected ~{expected:,}, delta {delta_str})")
        else:
            log(f"    {tname:40s}  FAILED TO PULL")

    log("")

    # ------------------------------------------------------------------
    # Step 7: Write assembly log
    # ------------------------------------------------------------------
    log_path = os.path.join(OUTPUT_DIR, "director_assembly_log.md")
    log(f"Writing assembly log to: {log_path}")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("# Agent 0B - Director Network Assembly Log\n\n")
        f.write(f"**Run timestamp:** {run_start}\n\n")
        f.write(f"**Agent:** 0B (Director Network Builder)\n\n")
        f.write(f"**Operation:** Lineage Audit\n\n")
        f.write("---\n\n")
        f.write("## Raw Output\n\n")
        f.write("```\n")
        for line in log_lines:
            f.write(line + "\n")
        f.write("```\n\n")
        f.write("---\n\n")

        # Summary table
        f.write("## Output Files\n\n")
        f.write("| File | Rows | Description |\n")
        f.write("|------|------|-------------|\n")
        for tname, tinfo in TABLES.items():
            df = dataframes.get(tname)
            rows = f"{len(df):,}" if df is not None else "FAILED"
            f.write(f"| `{tinfo['output_csv']}` | {rows} | {tinfo['description']} |\n")
        f.write(f"| `director_assembly_log.md` | - | This file |\n")
        f.write("\n")

        # CSV column inventories
        f.write("## Column Inventories\n\n")
        for tname, tinfo in TABLES.items():
            df = dataframes.get(tname)
            if df is not None:
                f.write(f"### {tinfo['output_csv']}\n\n")
                f.write("| Column | Dtype | Non-Null | Unique |\n")
                f.write("|--------|-------|----------|--------|\n")
                for col in df.columns:
                    dtype = str(df[col].dtype)
                    non_null = df[col].notna().sum()
                    nunique = df[col].nunique()
                    f.write(f"| `{col}` | {dtype} | {non_null:,} | {nunique:,} |\n")
                f.write("\n")

    log(f"Assembly log written.")
    log("")
    log("=" * 60)
    log("Agent 0B COMPLETE. All outputs written.")
    log("=" * 60)


if __name__ == "__main__":
    main()
