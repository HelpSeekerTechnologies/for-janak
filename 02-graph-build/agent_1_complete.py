#!/usr/bin/env python
"""
Agent 1A/1B -- Completion script for Step 10 + Final Validation
Steps 1-9 completed. Step 10 (LOCATED_IN) was interrupted at ~8000/9145.
Since all operations use MERGE, this is fully idempotent.
"""

import sys, os, csv, time
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from neo4j import GraphDatabase

NEO4J_URI      = "<YOUR_NEO4J_AURA_URI>"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"
BATCH_SIZE     = 500

DATA_DIR   = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly"
OUTPUT_DIR = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\02-graph-build"
LOG_PATH   = os.path.join(OUTPUT_DIR, "ingestion_log.md")

LOG_LINES = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)

def append_to_log():
    """Append completion lines to existing log without duplicating."""
    existing_content = ""
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, 'r', encoding='utf-8') as f:
            existing_content = f.read()

    # Remove trailing ``` if present so we can append inside the code block
    if existing_content.rstrip().endswith('```'):
        existing_content = existing_content.rstrip()[:-3].rstrip()

    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        f.write(existing_content + "\n\n")
        f.write("[COMPLETION - Finishing Step 10 + Final Validation]\n\n")
        for line in LOG_LINES:
            f.write(line + "\n")
        f.write("```\n")

def read_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            return list(csv.DictReader(f))
    except UnicodeDecodeError:
        with open(path, 'r', encoding='cp1252') as f:
            return list(csv.DictReader(f))

def safe_int(val):
    if val is None or val == '' or val == 'NA':
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None

def batched(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]

def main():
    t_start = time.time()
    log("=" * 72)
    log("AGENT 1A/1B COMPLETION -- Step 10 + Final Validation")
    log("=" * 72)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    log("Connected to Neo4j Aura")

    # Quick status check
    with driver.session() as s:
        for rel in ['SITS_ON', 'RECEIVED_GRANT', 'FLAGGED_AS', 'LOCATED_IN', 'SHARED_DIRECTORS']:
            cnt = s.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()['c']
            log(f"  {rel}: {cnt}")

    # Load org_risk_flags for step 10
    org_risk_data = read_csv("org_risk_flags.csv")
    log(f"  org_risk_flags loaded: {len(org_risk_data)} rows")

    # ── STEP 10: LOCATED_IN edges (idempotent MERGE) ──────────────────
    log("")
    log("-- STEP 10: LOCATED_IN Edges (completing) --")
    t0 = time.time()

    located_params = []
    for row in org_risk_data:
        bn   = row.get('bn', '').strip()
        city = row.get('City', '').strip()
        if bn and city:
            located_params.append({'bn': bn, 'city': city})

    log(f"  LOCATED_IN edges to process (MERGE): {len(located_params)}")

    n_located = 0
    for batch in batched(located_params, BATCH_SIZE):
        with driver.session() as s:
            s.run("""
                UNWIND $items AS p
                MATCH (o:Organization {bn: p.bn})
                MERGE (r:Region {name: p.city})
                ON CREATE SET r.kgl = '\u16AA', r.kgl_handle = 'geography'
                MERGE (o)-[:LOCATED_IN]->(r)
            """, items=batch)
        n_located += len(batch)
        if n_located % 2000 == 0:
            log(f"    ... {n_located} LOCATED_IN edges processed")
    log(f"  Merged {n_located} LOCATED_IN edges in {time.time()-t0:.1f}s")

    with driver.session() as s:
        cnt = s.run("MATCH (:Organization)-[r:LOCATED_IN]->(:Region) RETURN count(r) AS c").single()['c']
        log(f"  VALIDATE: {cnt} LOCATED_IN relationships in graph")

    # ── FINAL VALIDATION ─────────────────────────────────────────────
    log("")
    log("-- FINAL VALIDATION (targeted counts only) --")
    with driver.session() as s:
        log("  === Lineage Audit Node Counts ===")
        for label, where in [
            ('Organization', 'WHERE n.bn IS NOT NULL'),
            ('Director', ''),
            ('OrgEntity', ''),
            ('FiscalYear', ''),
            ('Region', ''),
            ('RiskFlag', 'WHERE n.flag_type IS NOT NULL'),
        ]:
            cnt = s.run(f"MATCH (n:{label}) {where} RETURN count(n) AS c").single()['c']
            log(f"    {label}: {cnt}")

        log("  === Lineage Audit Relationship Counts ===")
        for rel in ['RECEIVED_GRANT', 'SITS_ON', 'FLAGGED_AS', 'LOCATED_IN', 'SHARED_DIRECTORS']:
            cnt = s.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()['c']
            log(f"    {rel}: {cnt}")

        # Also count the existing lineage relationships
        log("  === Existing Lineage Relationships ===")
        for rel in ['SOURCE_OF', 'TARGET_OF', 'PARENT_OF', 'EVIDENCED_BY']:
            cnt = s.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()['c']
            log(f"    {rel}: {cnt}")

        # Spot checks
        log("")
        log("-- SPOT CHECKS --")

        top_flags = s.run("""
            MATCH (o:Organization)-[:FLAGGED_AS]->(f:RiskFlag)
            WHERE o.bn IS NOT NULL
            RETURN o.name AS name, o.bn AS bn, count(f) AS n_flags
            ORDER BY n_flags DESC LIMIT 5
        """).data()
        log("  Top 5 orgs by number of risk flags:")
        for r in top_flags:
            log(f"    {r['name']} ({r['bn']}): {r['n_flags']} flags")

        top_dirs = s.run("""
            MATCH (d:Director)-[:SITS_ON]->(o:Organization)
            RETURN d.normalized_name AS name, count(o) AS boards
            ORDER BY boards DESC LIMIT 5
        """).data()
        log("  Top 5 directors by board seats (in graph):")
        for r in top_dirs:
            log(f"    {r['name']}: {r['boards']} boards")

        grant_sample = s.run("""
            MATCH (o:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
            RETURN o.name AS org, m.name AS ministry, g.amount AS amount,
                   g.fiscal_year AS fy, g.political_era AS era
            ORDER BY g.amount DESC LIMIT 5
        """).data()
        log("  Top 5 grants by amount:")
        for r in grant_sample:
            amt = r['amount'] if r['amount'] else 0
            log(f"    {r['org']} <- {r['ministry']}: ${amt:,.0f} ({r['fy']} {r['era']})")

        ministry_conn = s.run("""
            MATCH (m:OrgEntity)<-[g:RECEIVED_GRANT]-(o:Organization)
            RETURN m.name AS ministry, count(DISTINCT o) AS n_orgs,
                   count(g) AS n_grants, sum(g.amount) AS total_amount
            ORDER BY n_orgs DESC LIMIT 5
        """).data()
        log("  Top 5 ministries by connected organizations:")
        for r in ministry_conn:
            amt = r['total_amount'] if r['total_amount'] else 0
            log(f"    {r['ministry']}: {r['n_orgs']} orgs, {r['n_grants']} grants, ${amt:,.0f}")

        cluster_info = s.run("""
            MATCH (o:Organization)
            WHERE o.cluster_id IS NOT NULL
            RETURN count(o) AS n_clustered,
                   count(DISTINCT o.cluster_id) AS n_clusters
        """).data()
        if cluster_info:
            log(f"  Clustered orgs: {cluster_info[0]['n_clustered']} in {cluster_info[0]['n_clusters']} clusters")

        cross = s.run("""
            MATCH (o:Organization)-[:RECEIVED_GRANT]->(m:OrgEntity)
            WHERE o.cluster_id IS NOT NULL
            RETURN count(DISTINCT o) AS n_clustered_grantees,
                   count(DISTINCT m) AS n_ministries
        """).data()
        if cross:
            log(f"  Clustered orgs that received grants: {cross[0]['n_clustered_grantees']} from {cross[0]['n_ministries']} ministries")

        # Cross-check: flagged orgs that also received grants
        flagged_grantees = s.run("""
            MATCH (o:Organization)-[:FLAGGED_AS]->(f:RiskFlag)
            WHERE EXISTS { (o)-[:RECEIVED_GRANT]->(:OrgEntity) }
            RETURN count(DISTINCT o) AS n_flagged_grantees,
                   count(DISTINCT f) AS n_flag_types
        """).data()
        if flagged_grantees:
            log(f"  Flagged orgs that received grants: {flagged_grantees[0]['n_flagged_grantees']} across {flagged_grantees[0]['n_flag_types']} flag types")

        # Region distribution
        region_dist = s.run("""
            MATCH (r:Region)<-[:LOCATED_IN]-(o:Organization)
            RETURN r.name AS region, count(o) AS n_orgs
            ORDER BY n_orgs DESC LIMIT 10
        """).data()
        log("  Top 10 regions by organization count:")
        for r in region_dist:
            log(f"    {r['region']}: {r['n_orgs']} orgs")

    driver.close()

    elapsed = time.time() - t_start
    log("")
    log(f"Completion elapsed time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log("=" * 72)
    log("AGENT 1A/1B -- ALL 10 STEPS COMPLETE")
    log("=" * 72)

    append_to_log()
    print(f"\nLog appended to: {LOG_PATH}")

if __name__ == "__main__":
    main()
