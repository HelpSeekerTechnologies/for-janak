#!/usr/bin/env python
"""
Agent 1A/1B -- Resume script for Steps 8-10 + Final Validation
Session expired during FLAGGED_AS edge creation. Steps 1-7 completed.
"""

import sys, os, csv, time, json
from datetime import datetime
from collections import defaultdict

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

def flush_log():
    # Read existing log and append our new lines
    existing = []
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
            # extract lines between ``` blocks
            if '```' in content:
                parts = content.split('```')
                if len(parts) >= 2:
                    existing = parts[1].strip().split('\n')

    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        f.write("# Ingestion Log -- Agent 1A/1B Graph Builder\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("```\n")
        for line in existing:
            f.write(line + "\n")
        f.write("\n[RESUME - Session expired, restarting from Step 8]\n\n")
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

def safe_float(val):
    if val is None or val == '' or val == 'NA':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

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

def run_with_retry(driver, cypher, params, max_retries=3):
    """Execute a Cypher statement with automatic retry on session expiry."""
    for attempt in range(max_retries):
        try:
            with driver.session() as s:
                result = s.run(cypher, **params)
                return result.consume()
        except Exception as e:
            if attempt < max_retries - 1 and ('SessionExpired' in str(type(e).__name__) or 'ServiceUnavailable' in str(type(e).__name__)):
                log(f"    RETRY {attempt+1}: Connection error, waiting 5s...")
                time.sleep(5)
            else:
                raise

def main():
    t_start = time.time()
    log("=" * 72)
    log("AGENT 1A/1B RESUME -- Steps 8-10 + Final Validation")
    log("=" * 72)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    log("Connected to Neo4j Aura")

    # Quick status check
    with driver.session() as s:
        for rel in ['SITS_ON', 'RECEIVED_GRANT', 'FLAGGED_AS', 'LOCATED_IN', 'SHARED_DIRECTORS']:
            cnt = s.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()['c']
            log(f"  {rel}: {cnt}")

    # Load org_risk_flags for steps 8, 9, 10
    org_risk_data = read_csv("org_risk_flags.csv")
    org_bn_set = set(row.get('bn', '').strip() for row in org_risk_data if row.get('bn', '').strip())
    log(f"  org_risk_flags loaded: {len(org_risk_data)} rows, {len(org_bn_set)} BNs")

    # ── STEP 8: FLAGGED_AS edges ─────────────────────────────────────
    log("")
    log("-- STEP 8: FLAGGED_AS Edges --")
    t0 = time.time()

    flag_col_map = {
        'flag_low_passthrough':     'low_passthrough',
        'flag_salary_mill':         'salary_mill',
        'flag_high_gov_dependency': 'high_gov_dependency',
        'flag_deficit':             'deficit',
        'flag_insolvency_5pct_cut': 'insolvency_5pct_cut',
        'flag_shadow_network':      'shadow_network',
        'flag_in_director_cluster': 'in_director_cluster',
    }

    flag_params = []
    for row in org_risk_data:
        bn = row.get('bn', '').strip()
        if not bn:
            continue
        for col, flag_type in flag_col_map.items():
            val = row.get(col, '').strip()
            if val == '1':
                flag_params.append({'bn': bn, 'flag_type': flag_type})

    log(f"  FLAGGED_AS edges to create: {len(flag_params)}")

    n_flags = 0
    for batch in batched(flag_params, BATCH_SIZE):
        with driver.session() as s:
            s.run("""
                UNWIND $items AS p
                MATCH (o:Organization {bn: p.bn})
                MATCH (f:RiskFlag {flag_type: p.flag_type})
                MERGE (o)-[:FLAGGED_AS]->(f)
            """, items=batch)
        n_flags += len(batch)
        if n_flags % 2000 == 0:
            log(f"    ... {n_flags} FLAGGED_AS edges processed")
    log(f"  Merged {n_flags} FLAGGED_AS edges in {time.time()-t0:.1f}s")

    with driver.session() as s:
        cnt = s.run("MATCH (:Organization)-[r:FLAGGED_AS]->(:RiskFlag) RETURN count(r) AS c").single()['c']
        log(f"  VALIDATE: {cnt} FLAGGED_AS relationships in graph")
    flush_log()

    # ── STEP 9: Cluster properties + SHARED_DIRECTORS edges ──────────
    log("")
    log("-- STEP 9: Cluster Properties & SHARED_DIRECTORS Edges --")
    t0 = time.time()

    # 9a: Set cluster_id on Organization nodes
    cluster_data = read_csv("org_clusters.csv")
    log(f"  Loaded org_clusters.csv: {len(cluster_data)} rows")

    cluster_params = []
    for row in cluster_data:
        bn = row.get('bn', '').strip()
        cid_val = safe_int(row.get('cluster_id'))
        cs  = safe_int(row.get('cluster_size'))
        if bn and cid_val is not None:
            cluster_params.append({'bn': bn, 'cluster_id': cid_val, 'cluster_size': cs})

    for batch in batched(cluster_params, BATCH_SIZE):
        with driver.session() as s:
            s.run("""
                UNWIND $items AS p
                MATCH (o:Organization {bn: p.bn})
                SET o.cluster_id = p.cluster_id, o.cluster_size = p.cluster_size
            """, items=batch)
    log(f"  Set cluster_id on {len(cluster_params)} orgs in {time.time()-t0:.1f}s")

    # 9b: SHARED_DIRECTORS edges from org_network_edges
    t1 = time.time()
    network_data = read_csv("org_network_edges.csv")
    log(f"  Loaded org_network_edges.csv: {len(network_data)} rows")

    shared_dir_params = []
    for row in network_data:
        bn1 = row.get('org1_bn', '').strip()
        bn2 = row.get('org2_bn', '').strip()
        if bn1 in org_bn_set and bn2 in org_bn_set:
            n_shared_val = safe_int(row.get('n_shared_directors'))
            shared_dir_params.append({
                'bn1': bn1,
                'bn2': bn2,
                'n_shared': n_shared_val,
            })

    log(f"  SHARED_DIRECTORS edges (both orgs in set): {len(shared_dir_params)} of {len(network_data)} total")

    n_shared = 0
    for batch in batched(shared_dir_params, BATCH_SIZE):
        with driver.session() as s:
            s.run("""
                UNWIND $items AS p
                MATCH (o1:Organization {bn: p.bn1})
                MATCH (o2:Organization {bn: p.bn2})
                MERGE (o1)-[c:SHARED_DIRECTORS]->(o2)
                SET c.n_shared_directors = p.n_shared
            """, items=batch)
        n_shared += len(batch)
        if n_shared % 10000 == 0:
            log(f"    ... {n_shared} SHARED_DIRECTORS edges processed")
    log(f"  Merged {n_shared} SHARED_DIRECTORS edges in {time.time()-t1:.1f}s")

    with driver.session() as s:
        cnt = s.run("MATCH (:Organization)-[r:SHARED_DIRECTORS]->(:Organization) RETURN count(r) AS c").single()['c']
        log(f"  VALIDATE: {cnt} SHARED_DIRECTORS relationships in graph")
    log(f"  Step 9 total: {time.time()-t0:.1f}s")
    flush_log()

    # ── STEP 10: LOCATED_IN edges ────────────────────────────────────
    log("")
    log("-- STEP 10: LOCATED_IN Edges --")
    t0 = time.time()

    located_params = []
    for row in org_risk_data:
        bn   = row.get('bn', '').strip()
        city = row.get('City', '').strip()
        if bn and city:
            located_params.append({'bn': bn, 'city': city})

    log(f"  LOCATED_IN edges to create: {len(located_params)}")

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
    flush_log()

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
            log(f"  {label}: {cnt}")

        log("  === Lineage Audit Relationship Counts ===")
        for rel in ['RECEIVED_GRANT', 'SITS_ON', 'FLAGGED_AS', 'LOCATED_IN', 'SHARED_DIRECTORS']:
            cnt = s.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()['c']
            log(f"  {rel}: {cnt}")

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

    driver.close()

    elapsed = time.time() - t_start
    log("")
    log(f"Resume elapsed time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log("=" * 72)
    log("AGENT 1A/1B -- COMPLETE")
    log("=" * 72)

    flush_log()
    print(f"\nLog written to: {LOG_PATH}")

if __name__ == "__main__":
    main()
