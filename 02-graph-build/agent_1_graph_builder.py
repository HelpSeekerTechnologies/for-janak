#!/usr/bin/env python
"""
Agent 1A/1B — KGL v1.3-aligned Neo4j Graph Builder
Operation Lineage Audit

Builds and populates the unified knowledge graph on Neo4j Aura.
Preserves existing ministry lineage nodes (OrgEntity label, 142 nodes)
and all broader KGL nodes (~134M+ nodes).
Uses MERGE exclusively for idempotent ingestion.

IMPORTANT DISCOVERIES from graph inspection:
  - Ministry lineage nodes use label :OrgEntity (NOT :Ministry)
  - OrgEntity nodes have canonical_id (e.g., 'EM-001') and name
  - Existing :Organization nodes (189K) have NO bn property
  - Existing :Person nodes (579K) have NO normalized_name property
  - Our new nodes will MERGE by unique keys (bn, normalized_name) so
    they create new nodes alongside existing ones
  - :RiskFlag nodes with flag_type already exist for our 7 types
  - Graph has ~134M+ nodes total — AVOID full graph scans
"""

import sys, os, csv, ast, time, json, math
from datetime import datetime
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from neo4j import GraphDatabase

# ── Configuration ────────────────────────────────────────────────────
NEO4J_URI      = "<YOUR_NEO4J_AURA_URI>"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"
BATCH_SIZE     = 500

DATA_DIR   = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly"
OUTPUT_DIR = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\02-graph-build"

LOG_LINES = []
LOG_PATH  = os.path.join(OUTPUT_DIR, "ingestion_log.md")

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)

def flush_log():
    """Write log to disk incrementally so we can monitor progress."""
    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        f.write("# Ingestion Log -- Agent 1A/1B Graph Builder\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("```\n")
        for line in LOG_LINES:
            f.write(line + "\n")
        f.write("```\n")

def read_csv(filename):
    """Read CSV with utf-8-sig to handle BOM, fallback to cp1252."""
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except UnicodeDecodeError:
        with open(path, 'r', encoding='cp1252') as f:
            reader = csv.DictReader(f)
            return list(reader)

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

def parse_linked_bns(raw):
    """Parse linked_bns column — JSON list of BN strings."""
    if not raw or raw.strip() == '':
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(b).strip() for b in parsed if b]
        return []
    except (json.JSONDecodeError, ValueError):
        pass
    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, list):
            return [str(b).strip() for b in parsed if b]
        return []
    except (ValueError, SyntaxError):
        pass
    return []


# ── Main ─────────────────────────────────────────────────────────────
def main():
    t_start = time.time()
    log("=" * 72)
    log("AGENT 1A/1B -- KGL v1.3 Graph Builder -- START")
    log("=" * 72)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    log("Connected to Neo4j Aura")

    # ── PHASE 0: Inspect existing graph (targeted queries only) ──────
    log("")
    log("-- PHASE 0: Existing Graph State (targeted label counts) --")
    t0 = time.time()
    with driver.session() as s:
        # Only count labels we care about — avoids scanning 134M+ nodes
        target_labels = [
            'OrgEntity', 'TransformEvent', 'SourceDocument',
            'Organization', 'Director', 'Person',
            'FiscalYear', 'Region', 'RiskFlag',
        ]
        for label in target_labels:
            cnt = s.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()['c']
            log(f"  {label}: {cnt}")

        # Target relationship types we care about
        target_rels = [
            'SOURCE_OF', 'TARGET_OF', 'PARENT_OF', 'EVIDENCED_BY',
            'RECEIVED_GRANT', 'SITS_ON', 'FLAGGED_AS',
            'LOCATED_IN', 'SHARED_DIRECTORS', 'CLUSTER_MEMBER',
        ]
        for rel in target_rels:
            cnt = s.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()['c']
            if cnt > 0:
                log(f"  Rel {rel}: {cnt}")

        # Inspect OrgEntity (ministry) nodes
        orgentity_sample = s.run("""
            MATCH (m:OrgEntity)
            RETURN m.canonical_id AS cid, m.name AS name
            ORDER BY m.canonical_id LIMIT 5
        """).data()
        log(f"  OrgEntity sample: {orgentity_sample}")

    log(f"  Phase 0 completed in {time.time()-t0:.1f}s")
    flush_log()

    # ── PHASE 1A: Schema DDL ─────────────────────────────────────────
    log("")
    log("-- PHASE 1A: Schema DDL (Constraints & Indexes) --")
    t0 = time.time()
    schema_statements = [
        # Constraints — Organization nodes keyed by bn (new, distinct from existing)
        "CREATE CONSTRAINT org_bn IF NOT EXISTS FOR (o:Organization) REQUIRE o.bn IS UNIQUE",
        "CREATE CONSTRAINT director_id IF NOT EXISTS FOR (d:Director) REQUIRE d.normalized_name IS UNIQUE",
        "CREATE CONSTRAINT fy_id IF NOT EXISTS FOR (fy:FiscalYear) REQUIRE fy.year IS UNIQUE",
        "CREATE CONSTRAINT region_id IF NOT EXISTS FOR (r:Region) REQUIRE r.name IS UNIQUE",
        "CREATE CONSTRAINT flag_id IF NOT EXISTS FOR (f:RiskFlag) REQUIRE f.flag_type IS UNIQUE",
        # Indexes
        "CREATE INDEX orgentity_canonical IF NOT EXISTS FOR (m:OrgEntity) ON (m.canonical_id)",
        "CREATE INDEX orgentity_name IF NOT EXISTS FOR (m:OrgEntity) ON (m.name)",
        "CREATE INDEX org_name IF NOT EXISTS FOR (o:Organization) ON (o.name)",
        "CREATE INDEX director_name IF NOT EXISTS FOR (d:Director) ON (d.normalized_name)",
    ]
    with driver.session() as s:
        for stmt in schema_statements:
            try:
                s.run(stmt)
                # Extract the constraint/index name from statement
                parts = stmt.split("IF NOT EXISTS")
                label_hint = parts[0].strip().split()[-1] if parts else stmt[:40]
                log(f"  OK: {label_hint}")
            except Exception as e:
                log(f"  WARN: {stmt[:60]}... => {e}")
    log(f"  Schema DDL completed in {time.time()-t0:.1f}s")
    flush_log()

    # ── STEP 1: FiscalYear nodes ─────────────────────────────────────
    log("")
    log("-- STEP 1: FiscalYear Nodes --")
    t0 = time.time()
    grants_data = read_csv("grants_aggregated.csv")
    log(f"  Loaded grants_aggregated.csv: {len(grants_data)} rows")

    fiscal_years = sorted(set(row['fiscal_year'] for row in grants_data if row.get('fiscal_year')))
    log(f"  Unique fiscal years: {fiscal_years}")

    with driver.session() as s:
        fy_params = []
        for fy in fiscal_years:
            fy_val = int(fy) if fy.isdigit() else fy
            fy_params.append({'year': fy_val})
        s.run("""
            UNWIND $items AS p
            MERGE (fy:FiscalYear {year: p.year})
            SET fy.kgl = '\u27F2', fy.kgl_handle = 'timeframe'
        """, items=fy_params)

    with driver.session() as s:
        cnt = s.run("MATCH (fy:FiscalYear) RETURN count(fy) AS c").single()['c']
        log(f"  VALIDATE: {cnt} FiscalYear nodes in graph")
    log(f"  Completed in {time.time()-t0:.1f}s")
    flush_log()

    # ── STEP 2: Region nodes ─────────────────────────────────────────
    log("")
    log("-- STEP 2: Region Nodes --")
    t0 = time.time()
    org_risk_data = read_csv("org_risk_flags.csv")
    log(f"  Loaded org_risk_flags.csv: {len(org_risk_data)} rows")

    cities = sorted(set(
        row['City'].strip() for row in org_risk_data
        if row.get('City') and row['City'].strip()
    ))
    log(f"  Unique cities/regions: {len(cities)}")

    with driver.session() as s:
        for batch in batched(cities, BATCH_SIZE):
            s.run("""
                UNWIND $items AS name
                MERGE (r:Region {name: name})
                SET r.kgl = '\u16AA', r.kgl_handle = 'geography'
            """, items=batch)

    with driver.session() as s:
        cnt = s.run("MATCH (r:Region) RETURN count(r) AS c").single()['c']
        log(f"  VALIDATE: {cnt} Region nodes in graph")
    log(f"  Completed in {time.time()-t0:.1f}s")
    flush_log()

    # ── STEP 3: RiskFlag nodes ───────────────────────────────────────
    log("")
    log("-- STEP 3: RiskFlag Nodes --")
    t0 = time.time()
    flag_types = {
        'low_passthrough':     'Organization passes through <25% of revenue to programs',
        'salary_mill':         'Compensation exceeds 50% of expenditures',
        'high_gov_dependency': 'Government revenue exceeds 80% of total revenue',
        'deficit':             'Organization is in deficit (expenditures > revenue)',
        'insolvency_5pct_cut': 'Organization would be insolvent with 5% revenue cut',
        'shadow_network':      'Organization is part of a shadow governance network',
        'in_director_cluster': 'Organization is in a shared-director cluster',
    }
    with driver.session() as s:
        for ftype, desc in flag_types.items():
            s.run("""
                MERGE (f:RiskFlag {flag_type: $type})
                SET f.kgl = '\u27E1', f.kgl_handle = 'measurement', f.description = $desc
            """, type=ftype, desc=desc)

    with driver.session() as s:
        cnt = s.run("MATCH (f:RiskFlag) WHERE f.flag_type IS NOT NULL RETURN count(f) AS c").single()['c']
        log(f"  VALIDATE: {cnt} RiskFlag nodes with flag_type in graph")
    log(f"  Completed in {time.time()-t0:.1f}s")
    flush_log()

    # ── STEP 4: Organization nodes (9,145 from org_risk_flags) ───────
    log("")
    log("-- STEP 4: Organization Nodes (CRA charities with BN) --")
    t0 = time.time()

    org_params = []
    for row in org_risk_data:
        bn = row.get('bn', '').strip()
        if not bn:
            continue
        org_params.append({
            'bn':           bn,
            'name':         row.get('Legal_name', '').strip(),
            'account_name': row.get('Account_name', '').strip(),
            'city':         row.get('City', '').strip(),
            'category':     row.get('Category_English_Desc', '').strip(),
            'total_revenue':       safe_float(row.get('Total_Revenue')),
            'total_expenditures':  safe_float(row.get('Total_Expenditures')),
            'gov_dependency_pct':  safe_float(row.get('gov_dependency_pct')),
            'program_pct':         safe_float(row.get('program_pct')),
            'admin_pct':           safe_float(row.get('admin_pct')),
            'fundraising_pct':     safe_float(row.get('fundraising_pct')),
            'compensation_pct':    safe_float(row.get('compensation_pct_of_exp')),
            'total_gov_rev':       safe_float(row.get('total_gov_rev')),
            'prov_rev':            safe_float(row.get('prov_rev')),
            'fed_rev':             safe_float(row.get('fed_rev')),
            'total_assets':        safe_float(row.get('Total_Assets')),
            'total_liabilities':   safe_float(row.get('Total_Liabilities')),
            'net_assets':          safe_float(row.get('net_assets')),
        })

    log(f"  Prepared {len(org_params)} Organization params")

    n_org_created = 0
    with driver.session() as s:
        for batch in batched(org_params, BATCH_SIZE):
            s.run("""
                UNWIND $items AS p
                MERGE (o:Organization {bn: p.bn})
                SET o.name              = p.name,
                    o.account_name      = p.account_name,
                    o.city              = p.city,
                    o.category          = p.category,
                    o.total_revenue     = p.total_revenue,
                    o.total_expenditures = p.total_expenditures,
                    o.gov_dependency_pct = p.gov_dependency_pct,
                    o.program_pct       = p.program_pct,
                    o.admin_pct         = p.admin_pct,
                    o.fundraising_pct   = p.fundraising_pct,
                    o.compensation_pct  = p.compensation_pct,
                    o.total_gov_rev     = p.total_gov_rev,
                    o.prov_rev          = p.prov_rev,
                    o.fed_rev           = p.fed_rev,
                    o.total_assets      = p.total_assets,
                    o.total_liabilities = p.total_liabilities,
                    o.net_assets        = p.net_assets,
                    o.kgl               = '\u16B4',
                    o.kgl_handle        = 'organization',
                    o.data_source       = 'CRA_T3010'
            """, items=batch)
            n_org_created += len(batch)
            if n_org_created % 2000 == 0:
                log(f"    ... {n_org_created} Organization nodes merged")
    log(f"  Merged {n_org_created} Organization nodes in {time.time()-t0:.1f}s")

    # Validate — count only orgs with bn (ours)
    with driver.session() as s:
        cnt = s.run("MATCH (o:Organization) WHERE o.bn IS NOT NULL RETURN count(o) AS c").single()['c']
        log(f"  VALIDATE: {cnt} Organization nodes with BN in graph")
    flush_log()

    # Build org BN set for later steps
    org_bn_set = set(p['bn'] for p in org_params)

    # ── STEP 5: Director nodes (19,156) ──────────────────────────────
    log("")
    log("-- STEP 5: Director Nodes --")
    t0 = time.time()
    directors_data = read_csv("multi_board_directors.csv")
    log(f"  Loaded multi_board_directors.csv: {len(directors_data)} rows")

    director_params = []
    director_bns = {}  # name -> list of BNs (for step 6)
    skipped_directors = 0
    for row in directors_data:
        name = row.get('clean_name_no_initial', '').strip()
        if not name:
            skipped_directors += 1
            continue
        n_boards = safe_int(row.get('n_boards'))
        n_nal = safe_int(row.get('n_non_arms_length'))
        bns = parse_linked_bns(row.get('linked_bns', ''))
        director_bns[name] = bns
        director_params.append({
            'name':     name,
            'n_boards': n_boards,
            'n_non_arms_length': n_nal,
        })

    log(f"  Prepared {len(director_params)} Director params (skipped {skipped_directors})")

    n_dir_created = 0
    with driver.session() as s:
        for batch in batched(director_params, BATCH_SIZE):
            s.run("""
                UNWIND $items AS p
                MERGE (d:Director {normalized_name: p.name})
                SET d.n_boards          = p.n_boards,
                    d.n_non_arms_length = p.n_non_arms_length,
                    d.kgl               = '\u25CE',
                    d.kgl_handle        = 'person'
            """, items=batch)
            n_dir_created += len(batch)
            if n_dir_created % 5000 == 0:
                log(f"    ... {n_dir_created} Director nodes merged")
    log(f"  Merged {n_dir_created} Director nodes in {time.time()-t0:.1f}s")

    with driver.session() as s:
        cnt = s.run("MATCH (d:Director) RETURN count(d) AS c").single()['c']
        log(f"  VALIDATE: {cnt} Director nodes in graph")
    flush_log()

    # ── STEP 6: SITS_ON edges (directors -> organizations) ───────────
    log("")
    log("-- STEP 6: SITS_ON Edges --")
    t0 = time.time()

    log(f"  Known Organization BNs: {len(org_bn_set)}")

    sits_on_params = []
    total_bn_refs = 0
    matched_bn_refs = 0
    for name, bns in director_bns.items():
        for bn in bns:
            total_bn_refs += 1
            if bn in org_bn_set:
                matched_bn_refs += 1
                sits_on_params.append({'name': name, 'bn': bn})

    log(f"  Total director->BN references: {total_bn_refs}")
    log(f"  Matched to known orgs: {matched_bn_refs} ({100*matched_bn_refs/max(total_bn_refs,1):.1f}%)")
    log(f"  SITS_ON edges to create: {len(sits_on_params)}")

    n_sits = 0
    with driver.session() as s:
        for batch in batched(sits_on_params, BATCH_SIZE):
            s.run("""
                UNWIND $items AS p
                MATCH (d:Director {normalized_name: p.name})
                MATCH (o:Organization {bn: p.bn})
                MERGE (d)-[:SITS_ON]->(o)
            """, items=batch)
            n_sits += len(batch)
            if n_sits % 5000 == 0:
                log(f"    ... {n_sits} SITS_ON edges processed")
    log(f"  Merged {n_sits} SITS_ON edges in {time.time()-t0:.1f}s")

    with driver.session() as s:
        cnt = s.run("MATCH (:Director)-[r:SITS_ON]->(:Organization) RETURN count(r) AS c").single()['c']
        log(f"  VALIDATE: {cnt} SITS_ON relationships in graph")
    flush_log()

    # ── STEP 7: RECEIVED_GRANT edges (org -> OrgEntity/ministry) ─────
    log("")
    log("-- STEP 7: RECEIVED_GRANT Edges --")
    log("  NOTE: Ministry nodes use :OrgEntity label with canonical_id")
    t0 = time.time()

    # Build lookup: GOA recipient name -> BN (from gold-standard matches)
    goa_matched = read_csv("goa_cra_matched.csv")
    log(f"  Loaded goa_cra_matched.csv: {len(goa_matched)} rows")

    name_to_bn = {}
    for row in goa_matched:
        goa_name = row.get('goa_name', '').strip().upper()
        bn = row.get('bn', '').strip()
        if goa_name and bn:
            name_to_bn[goa_name] = bn

    log(f"  GOA->BN lookup built: {len(name_to_bn)} entries")

    # Also build lookup from org_risk_flags: Legal_name/Account_name -> BN
    for row in org_risk_data:
        bn = row.get('bn', '').strip()
        legal = row.get('Legal_name', '').strip().upper()
        acct  = row.get('Account_name', '').strip().upper()
        if bn:
            if legal and legal not in name_to_bn:
                name_to_bn[legal] = bn
            if acct and acct not in name_to_bn:
                name_to_bn[acct] = bn

    log(f"  Extended name->BN lookup: {len(name_to_bn)} entries (added org_risk_flags names)")

    # Load OrgEntity (ministry) nodes from graph
    with driver.session() as s:
        ministry_info = s.run("""
            MATCH (m:OrgEntity)
            RETURN m.canonical_id AS cid, m.name AS name, m.normalized_name AS norm
            ORDER BY m.name
        """).data()
    log(f"  OrgEntity (ministry) nodes in graph: {len(ministry_info)}")

    # Build ministry lookups
    ministry_cid_set = set()
    ministry_name_upper_map = {}  # UPPER name -> actual name
    for m in ministry_info:
        if m.get('cid'):
            ministry_cid_set.add(m['cid'])
        if m.get('name'):
            ministry_name_upper_map[m['name'].upper()] = m['name']
        if m.get('norm') and m['norm']:
            ministry_name_upper_map[m['norm'].upper()] = m.get('name') or m['norm']

    log(f"  Ministry canonical_ids: {len(ministry_cid_set)}")
    log(f"  Ministry name lookup entries: {len(ministry_name_upper_map)}")

    # Process all 702K grant rows
    grant_edge_params = []
    unmatched_recipients = 0
    unmatched_ministries = 0
    unmatched_ministry_names = defaultdict(int)
    total_grant_rows = len(grants_data)
    matched_grant_rows = 0

    for row in grants_data:
        recipient = row.get('recipient', '').strip()
        ministry  = row.get('ministry', '').strip()
        fy        = row.get('fiscal_year', '').strip()
        era       = row.get('political_era', '').strip()
        amount    = safe_float(row.get('total_amount'))
        n_pay     = safe_int(row.get('n_payments'))
        cid       = row.get('canonical_ministry_id', '').strip()
        earliest  = row.get('earliest_payment', '').strip()
        latest    = row.get('latest_payment', '').strip()

        # Resolve recipient to BN
        bn = name_to_bn.get(recipient.upper())
        if not bn or bn not in org_bn_set:
            unmatched_recipients += 1
            continue

        # Resolve ministry — prefer canonical_id, fall back to name
        ministry_match_key = None
        if cid and cid in ministry_cid_set:
            ministry_match_key = ('cid', cid)
        elif ministry.upper() in ministry_name_upper_map:
            ministry_match_key = ('name', ministry_name_upper_map[ministry.upper()])
        else:
            unmatched_ministries += 1
            unmatched_ministry_names[ministry] += 1
            continue

        matched_grant_rows += 1
        grant_edge_params.append({
            'bn':        bn,
            'match_type': ministry_match_key[0],
            'match_val':  ministry_match_key[1],
            'fy':         fy,
            'era':        era,
            'amount':     amount,
            'n_payments': n_pay,
            'earliest':   earliest,
            'latest':     latest,
        })

    log(f"  Grant rows total: {total_grant_rows}")
    log(f"  Matched (org+ministry): {matched_grant_rows} ({100*matched_grant_rows/max(total_grant_rows,1):.1f}%)")
    log(f"  Unmatched recipients: {unmatched_recipients}")
    log(f"  Unmatched ministries: {unmatched_ministries}")
    if unmatched_ministry_names:
        top_unmatched = sorted(unmatched_ministry_names.items(), key=lambda x: -x[1])[:10]
        log(f"  Top unmatched ministry names: {top_unmatched}")
    log(f"  RECEIVED_GRANT edges to create: {len(grant_edge_params)}")

    # Split into canonical_id and name matches
    cid_grants = [g for g in grant_edge_params if g['match_type'] == 'cid']
    name_grants = [g for g in grant_edge_params if g['match_type'] == 'name']
    log(f"    by canonical_id: {len(cid_grants)}")
    log(f"    by name:         {len(name_grants)}")

    n_grants = 0
    with driver.session() as s:
        # Canonical ID matches -> target OrgEntity
        for batch in batched(cid_grants, BATCH_SIZE):
            s.run("""
                UNWIND $items AS p
                MATCH (o:Organization {bn: p.bn})
                MATCH (m:OrgEntity {canonical_id: p.match_val})
                MERGE (o)-[g:RECEIVED_GRANT {fiscal_year: p.fy, political_era: p.era}]->(m)
                SET g.amount     = p.amount,
                    g.n_payments = p.n_payments,
                    g.earliest   = p.earliest,
                    g.latest     = p.latest
            """, items=batch)
            n_grants += len(batch)
            if n_grants % 2000 == 0:
                log(f"    ... {n_grants} RECEIVED_GRANT edges processed (cid)")

        # Name matches -> target OrgEntity
        for batch in batched(name_grants, BATCH_SIZE):
            s.run("""
                UNWIND $items AS p
                MATCH (o:Organization {bn: p.bn})
                MATCH (m:OrgEntity {name: p.match_val})
                MERGE (o)-[g:RECEIVED_GRANT {fiscal_year: p.fy, political_era: p.era}]->(m)
                SET g.amount     = p.amount,
                    g.n_payments = p.n_payments,
                    g.earliest   = p.earliest,
                    g.latest     = p.latest
            """, items=batch)
            n_grants += len(batch)
            if n_grants % 2000 == 0:
                log(f"    ... {n_grants} RECEIVED_GRANT edges processed (name)")

    log(f"  Merged {n_grants} RECEIVED_GRANT edges in {time.time()-t0:.1f}s")

    with driver.session() as s:
        cnt = s.run("MATCH (:Organization)-[r:RECEIVED_GRANT]->(:OrgEntity) RETURN count(r) AS c").single()['c']
        log(f"  VALIDATE: {cnt} RECEIVED_GRANT relationships in graph")
    flush_log()

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
    with driver.session() as s:
        for batch in batched(flag_params, BATCH_SIZE):
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

    with driver.session() as s:
        for batch in batched(cluster_params, BATCH_SIZE):
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

    # Filter to edges where BOTH orgs are in our Alberta org set
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
    with driver.session() as s:
        for batch in batched(shared_dir_params, BATCH_SIZE):
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
    with driver.session() as s:
        for batch in batched(located_params, BATCH_SIZE):
            s.run("""
                UNWIND $items AS p
                MATCH (o:Organization {bn: p.bn})
                MERGE (r:Region {name: p.city})
                ON CREATE SET r.kgl = '\u16AA', r.kgl_handle = 'geography'
                MERGE (o)-[:LOCATED_IN]->(r)
            """, items=batch)
            n_located += len(batch)
    log(f"  Merged {n_located} LOCATED_IN edges in {time.time()-t0:.1f}s")

    with driver.session() as s:
        cnt = s.run("MATCH (:Organization)-[r:LOCATED_IN]->(:Region) RETURN count(r) AS c").single()['c']
        log(f"  VALIDATE: {cnt} LOCATED_IN relationships in graph")
    flush_log()

    # ── FINAL VALIDATION ─────────────────────────────────────────────
    log("")
    log("-- FINAL VALIDATION (targeted counts only) --")
    with driver.session() as s:
        # Count our specific node types
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

        # Orgs with most flags
        top_flags = s.run("""
            MATCH (o:Organization)-[:FLAGGED_AS]->(f:RiskFlag)
            WHERE o.bn IS NOT NULL
            RETURN o.name AS name, o.bn AS bn, count(f) AS n_flags
            ORDER BY n_flags DESC LIMIT 5
        """).data()
        log("  Top 5 orgs by number of risk flags:")
        for r in top_flags:
            log(f"    {r['name']} ({r['bn']}): {r['n_flags']} flags")

        # Directors on most boards (in our graph)
        top_dirs = s.run("""
            MATCH (d:Director)-[:SITS_ON]->(o:Organization)
            RETURN d.normalized_name AS name, count(o) AS boards
            ORDER BY boards DESC LIMIT 5
        """).data()
        log("  Top 5 directors by board seats (in graph):")
        for r in top_dirs:
            log(f"    {r['name']}: {r['boards']} boards")

        # Grant flow sample
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

        # Ministry connectivity
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

        # Cluster summary
        cluster_info = s.run("""
            MATCH (o:Organization)
            WHERE o.cluster_id IS NOT NULL
            RETURN count(o) AS n_clustered,
                   count(DISTINCT o.cluster_id) AS n_clusters
        """).data()
        if cluster_info:
            log(f"  Clustered orgs: {cluster_info[0]['n_clustered']} in {cluster_info[0]['n_clusters']} clusters")

        # Cross-check: orgs in clusters that also received grants
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
    log(f"Total elapsed time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log("=" * 72)
    log("AGENT 1A/1B -- COMPLETE")
    log("=" * 72)

    # Write final log
    flush_log()
    print(f"\nLog written to: {LOG_PATH}")


if __name__ == "__main__":
    main()
