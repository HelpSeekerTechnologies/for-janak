#!/usr/bin/env python
"""
Agent 1 — Federal Grants Ingestion into Neo4j Aura
Operation Lineage Audit

Ingests federal_grants.csv (109,583 rows) into the graph:
  - Filters to rows with non-null BN (~52K rows)
  - Filters to valid fiscal_year format (YYYY-YYYY)
  - Filters to non-null federal_department
  - Aggregates by BN x federal_department x fiscal_year
  - MERGEs FederalDepartment nodes
  - MERGEs FUNDED_BY_FED relationships (only where Organization node exists)

Addresses cleaning decisions C14 and DQ9.
Uses MERGE exclusively for idempotent ingestion.
"""

import sys, os, csv, time, re
from datetime import datetime
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from neo4j import GraphDatabase

# -- Configuration --------------------------------------------------------
NEO4J_URI      = "<YOUR_NEO4J_AURA_URI>"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"
BATCH_SIZE     = 500

DATA_DIR   = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly"
OUTPUT_DIR = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\02-graph-build"
CSV_PATH   = os.path.join(DATA_DIR, "federal_grants.csv")

LOG_LINES = []
LOG_PATH  = os.path.join(OUTPUT_DIR, "federal_ingestion_log.md")

VALID_FY_RE = re.compile(r'^\d{4}-\d{4}$')

# -- Helpers --------------------------------------------------------------

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)

def flush_log():
    """Write log to disk so we can monitor progress."""
    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        f.write("# Federal Grants Ingestion Log\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("```\n")
        for line in LOG_LINES:
            f.write(line + "\n")
        f.write("```\n")

def safe_float(val):
    if val is None or val == '' or val == 'NA':
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def batched(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]

def read_federal_csv():
    """Read federal_grants.csv with utf-8-sig to handle BOM, fallback to cp1252."""
    try:
        with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except UnicodeDecodeError:
        with open(CSV_PATH, 'r', encoding='cp1252') as f:
            reader = csv.DictReader(f)
            return list(reader)

# -- Main -----------------------------------------------------------------

def main():
    t_start = time.time()
    log("=" * 72)
    log("FEDERAL GRANTS INGESTION -- START")
    log("  Addresses: C14 (federal grants not ingested), DQ9")
    log("=" * 72)

    # ================================================================
    # STEP 1: Load and filter CSV
    # ================================================================
    log("")
    log("-- STEP 1: Load and Filter CSV --")
    t0 = time.time()

    raw_rows = read_federal_csv()
    log(f"  Total rows loaded: {len(raw_rows)}")

    # Filter: BN must be non-null and non-empty
    rows_with_bn = [r for r in raw_rows if r.get('BN') and r['BN'].strip()]
    log(f"  Rows with BN: {len(rows_with_bn)} ({100*len(rows_with_bn)/len(raw_rows):.1f}%)")
    log(f"  Rows without BN (skipped): {len(raw_rows) - len(rows_with_bn)}")

    # Filter: federal_department must be non-null and not the literal string "None"
    rows_with_dept = [r for r in rows_with_bn
                      if r.get('federal_department')
                      and r['federal_department'].strip()
                      and r['federal_department'].strip() != 'None']
    log(f"  Rows with BN + valid department: {len(rows_with_dept)}")
    log(f"  Rows with BN but no/None department (skipped): {len(rows_with_bn) - len(rows_with_dept)}")

    # Filter: fiscal_year must match YYYY-YYYY pattern
    rows_valid_fy = [r for r in rows_with_dept if VALID_FY_RE.match(r.get('fiscal_year', '').strip())]
    log(f"  Rows with BN + dept + valid fiscal_year: {len(rows_valid_fy)}")
    log(f"  Rows with invalid fiscal_year (skipped): {len(rows_with_dept) - len(rows_valid_fy)}")

    # Show invalid fiscal_year samples
    invalid_fy_samples = set()
    for r in rows_with_dept:
        fy = r.get('fiscal_year', '').strip()
        if not VALID_FY_RE.match(fy):
            invalid_fy_samples.add(fy)
    if invalid_fy_samples:
        log(f"  Invalid fiscal_year samples: {sorted(list(invalid_fy_samples))[:10]}")

    # Unique BNs and departments
    unique_bns = set(r['BN'].strip() for r in rows_valid_fy)
    unique_depts = set(r['federal_department'].strip() for r in rows_valid_fy)
    unique_fys = sorted(set(r['fiscal_year'].strip() for r in rows_valid_fy))
    log(f"  Unique BNs: {len(unique_bns)}")
    log(f"  Unique departments: {len(unique_depts)}")
    log(f"  Unique fiscal years: {unique_fys}")

    log(f"  Step 1 completed in {time.time()-t0:.1f}s")
    flush_log()

    # ================================================================
    # STEP 2: Aggregate by BN x department x fiscal_year
    # ================================================================
    log("")
    log("-- STEP 2: Aggregate by BN x Department x Fiscal Year --")
    t0 = time.time()

    agg = defaultdict(lambda: {'amount': 0.0, 'n_grants': 0})
    for r in rows_valid_fy:
        bn = r['BN'].strip()
        dept = r['federal_department'].strip()
        fy = r['fiscal_year'].strip()
        key = (bn, dept, fy)
        agg[key]['amount'] += safe_float(r.get('amount'))
        agg[key]['n_grants'] += 1

    agg_records = []
    for (bn, dept, fy), vals in agg.items():
        agg_records.append({
            'bn': bn,
            'dept': dept,
            'fy': fy,
            'amount': round(vals['amount'], 2),
            'n_grants': vals['n_grants'],
        })

    log(f"  Aggregated edges: {len(agg_records)}")
    log(f"  (from {len(rows_valid_fy)} raw rows)")

    # Stats
    total_amount = sum(r['amount'] for r in agg_records)
    total_grants = sum(r['n_grants'] for r in agg_records)
    log(f"  Total funding amount: ${total_amount:,.2f}")
    log(f"  Total grant count: {total_grants}")

    log(f"  Step 2 completed in {time.time()-t0:.1f}s")
    flush_log()

    # ================================================================
    # STEP 3: Connect to Neo4j and check existing Organization nodes
    # ================================================================
    log("")
    log("-- STEP 3: Connect to Neo4j / Check Existing Org Nodes --")
    t0 = time.time()

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    log("  Connected to Neo4j Aura")

    # Get all existing Organization BNs from the graph
    with driver.session() as s:
        result = s.run("""
            MATCH (o:Organization)
            WHERE o.bn IS NOT NULL
            RETURN o.bn AS bn
        """)
        existing_bns = set(record['bn'] for record in result)

    log(f"  Existing Organization nodes with BN: {len(existing_bns)}")

    # Filter agg_records to only those where BN exists in graph
    matched_records = [r for r in agg_records if r['bn'] in existing_bns]
    unmatched_bns = set(r['bn'] for r in agg_records if r['bn'] not in existing_bns)

    log(f"  Aggregated edges matching existing Org nodes: {len(matched_records)}")
    log(f"  BNs in CSV but NOT in graph: {len(unmatched_bns)}")
    log(f"  BNs in CSV that ARE in graph: {len(set(r['bn'] for r in matched_records))}")

    matched_amount = sum(r['amount'] for r in matched_records)
    matched_grants = sum(r['n_grants'] for r in matched_records)
    log(f"  Matched funding amount: ${matched_amount:,.2f}")
    log(f"  Matched grant count: {matched_grants}")

    # Unique departments in matched set
    matched_depts = sorted(set(r['dept'] for r in matched_records))
    log(f"  Unique departments in matched set: {len(matched_depts)}")

    log(f"  Step 3 completed in {time.time()-t0:.1f}s")
    flush_log()

    # ================================================================
    # STEP 4: Schema DDL — FederalDepartment constraint + index
    # ================================================================
    log("")
    log("-- STEP 4: Schema DDL --")
    t0 = time.time()

    schema_statements = [
        "CREATE CONSTRAINT fed_dept_name IF NOT EXISTS FOR (fd:FederalDepartment) REQUIRE fd.name IS UNIQUE",
        "CREATE INDEX fed_dept_idx IF NOT EXISTS FOR (fd:FederalDepartment) ON (fd.name)",
    ]
    with driver.session() as s:
        for stmt in schema_statements:
            try:
                s.run(stmt)
                log(f"  OK: {stmt[:60]}")
            except Exception as e:
                log(f"  WARN: {stmt[:60]}... => {e}")

    log(f"  Step 4 completed in {time.time()-t0:.1f}s")
    flush_log()

    # ================================================================
    # STEP 5: MERGE FederalDepartment nodes
    # ================================================================
    log("")
    log("-- STEP 5: MERGE FederalDepartment Nodes --")
    t0 = time.time()

    dept_params = [{'name': d} for d in matched_depts]
    log(f"  FederalDepartment nodes to MERGE: {len(dept_params)}")

    n_depts = 0
    with driver.session() as s:
        for batch in batched(dept_params, BATCH_SIZE):
            s.run("""
                UNWIND $items AS p
                MERGE (fd:FederalDepartment {name: p.name})
                SET fd.data_source = 'GoC_Grants',
                    fd.kgl         = 'program',
                    fd.kgl_handle  = 'program'
            """, items=batch)
            n_depts += len(batch)

    log(f"  Merged {n_depts} FederalDepartment nodes")

    with driver.session() as s:
        cnt = s.run("MATCH (fd:FederalDepartment) RETURN count(fd) AS c").single()['c']
        log(f"  VALIDATE: {cnt} FederalDepartment nodes in graph")

    log(f"  Step 5 completed in {time.time()-t0:.1f}s")
    flush_log()

    # ================================================================
    # STEP 6: MERGE FUNDED_BY_FED relationships
    # ================================================================
    log("")
    log("-- STEP 6: MERGE FUNDED_BY_FED Relationships --")
    t0 = time.time()

    log(f"  FUNDED_BY_FED edges to MERGE: {len(matched_records)}")

    n_edges = 0
    n_batches = 0
    with driver.session() as s:
        for batch in batched(matched_records, BATCH_SIZE):
            s.run("""
                UNWIND $items AS p
                MATCH (o:Organization {bn: p.bn})
                MERGE (fd:FederalDepartment {name: p.dept})
                MERGE (o)-[r:FUNDED_BY_FED {fiscal_year: p.fy}]->(fd)
                SET r.amount   = p.amount,
                    r.n_grants = p.n_grants
            """, items=batch)
            n_edges += len(batch)
            n_batches += 1
            if n_edges % 5000 == 0 or n_batches == 1:
                log(f"    ... {n_edges}/{len(matched_records)} FUNDED_BY_FED edges merged")
                flush_log()

    log(f"  Merged {n_edges} FUNDED_BY_FED edges in {time.time()-t0:.1f}s")

    with driver.session() as s:
        cnt = s.run("MATCH ()-[r:FUNDED_BY_FED]->() RETURN count(r) AS c").single()['c']
        log(f"  VALIDATE: {cnt} FUNDED_BY_FED relationships in graph")

    flush_log()

    # ================================================================
    # STEP 7: Verification / Spot Checks
    # ================================================================
    log("")
    log("-- STEP 7: Verification & Spot Checks --")
    t0 = time.time()

    with driver.session() as s:
        # Count FUNDED_BY_FED relationships
        cnt_rels = s.run("MATCH ()-[r:FUNDED_BY_FED]->() RETURN count(r) AS c").single()['c']
        log(f"  Total FUNDED_BY_FED relationships: {cnt_rels}")

        # Count FederalDepartment nodes
        cnt_depts = s.run("MATCH (fd:FederalDepartment) RETURN count(fd) AS c").single()['c']
        log(f"  Total FederalDepartment nodes: {cnt_depts}")

        # Top departments by number of funded organizations
        top_depts = s.run("""
            MATCH (o:Organization)-[r:FUNDED_BY_FED]->(fd:FederalDepartment)
            RETURN fd.name AS dept,
                   count(DISTINCT o) AS n_orgs,
                   count(r) AS n_edges,
                   sum(r.amount) AS total_amount
            ORDER BY n_orgs DESC
            LIMIT 10
        """).data()
        log("  Top 10 federal departments by number of funded AB orgs:")
        for r in top_depts:
            amt = r['total_amount'] if r['total_amount'] else 0
            log(f"    {r['dept'][:60]}: {r['n_orgs']} orgs, {r['n_edges']} edges, ${amt:,.0f}")

        # Orgs receiving BOTH GOA grants (RECEIVED_GRANT) and federal (FUNDED_BY_FED)
        n_dual_funded = s.run("""
            MATCH (o:Organization)-[:RECEIVED_GRANT]->(m:OrgEntity)
            WHERE EXISTS { (o)-[:FUNDED_BY_FED]->(:FederalDepartment) }
            RETURN count(DISTINCT o) AS n_dual_funded
        """).single()['n_dual_funded']
        log(f"  Organizations receiving BOTH GOA and federal grants: {n_dual_funded}")

        # Sample dual-funded orgs
        dual_samples = s.run("""
            MATCH (o:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
            WHERE EXISTS { (o)-[:FUNDED_BY_FED]->(:FederalDepartment) }
            WITH o, count(DISTINCT m) AS n_ministries
            MATCH (o)-[f:FUNDED_BY_FED]->(fd:FederalDepartment)
            WITH o, n_ministries,
                 count(DISTINCT fd) AS n_fed_depts,
                 sum(f.amount) AS fed_total
            RETURN o.name AS org_name,
                   o.bn AS bn,
                   n_ministries AS goa_ministries,
                   n_fed_depts AS fed_departments,
                   fed_total AS fed_funding_total
            ORDER BY fed_funding_total DESC
            LIMIT 10
        """).data()
        log("  Top 10 dual-funded orgs (GOA + Federal):")
        for r in dual_samples:
            amt = r['fed_funding_total'] if r['fed_funding_total'] else 0
            log(f"    {r['org_name']} (BN {r['bn']}): "
                f"{r['goa_ministries']} GOA ministries, "
                f"{r['fed_departments']} fed depts, "
                f"${amt:,.0f} fed total")

        # Orgs with only federal funding (no GOA)
        fed_only = s.run("""
            MATCH (o:Organization)-[:FUNDED_BY_FED]->(:FederalDepartment)
            WHERE NOT EXISTS { (o)-[:RECEIVED_GRANT]->(:OrgEntity) }
              AND o.bn IS NOT NULL
            RETURN count(DISTINCT o) AS cnt
        """).single()['cnt']
        log(f"  Organizations with federal funding ONLY (no GOA): {fed_only}")

        # Orgs with only GOA funding (no federal)
        goa_only = s.run("""
            MATCH (o:Organization)-[:RECEIVED_GRANT]->(:OrgEntity)
            WHERE NOT EXISTS { (o)-[:FUNDED_BY_FED]->(:FederalDepartment) }
              AND o.bn IS NOT NULL
            RETURN count(DISTINCT o) AS cnt
        """).single()['cnt']
        log(f"  Organizations with GOA funding ONLY (no federal): {goa_only}")

        # Fiscal year distribution of FUNDED_BY_FED
        fy_dist = s.run("""
            MATCH ()-[r:FUNDED_BY_FED]->()
            RETURN r.fiscal_year AS fy, count(r) AS cnt, sum(r.amount) AS total
            ORDER BY r.fiscal_year
        """).data()
        log("  FUNDED_BY_FED by fiscal year:")
        for r in fy_dist:
            amt = r['total'] if r['total'] else 0
            log(f"    {r['fy']}: {r['cnt']} edges, ${amt:,.0f}")

        # Risk-flagged orgs that receive federal funding
        flagged_fed = s.run("""
            MATCH (o:Organization)-[:FUNDED_BY_FED]->(:FederalDepartment)
            WHERE EXISTS { (o)-[:FLAGGED_AS]->(:RiskFlag) }
            RETURN count(DISTINCT o) AS cnt
        """).single()['cnt']
        log(f"  Risk-flagged orgs receiving federal funding: {flagged_fed}")

    log(f"  Step 7 completed in {time.time()-t0:.1f}s")

    # ================================================================
    # DONE
    # ================================================================
    driver.close()

    elapsed = time.time() - t_start
    log("")
    log(f"Total elapsed time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log("=" * 72)
    log("FEDERAL GRANTS INGESTION -- COMPLETE")
    log("=" * 72)

    flush_log()
    print(f"\nLog written to: {LOG_PATH}")


if __name__ == "__main__":
    main()
