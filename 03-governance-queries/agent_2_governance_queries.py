#!/usr/bin/env python
"""
Agent 2 — Governance Analysis Queries
Operation Lineage Audit — Phase 2

Executes 3 core Cypher queries + UCP symmetry test against the Neo4j graph.
Adapted to actual graph schema:
  - Ministry nodes: :OrgEntity (canonical_id, name)
  - Transform events: :TransformEvent (event_id, event_date, event_type)
  - Relationships: SOURCE_OF, TARGET_OF connect TransformEvent <-> OrgEntity
  - Clusters: cluster_id property on Organization nodes
  - SHARED_DIRECTORS edges between Organization nodes
  - RECEIVED_GRANT edges: Organization -> OrgEntity
"""

import sys, os, csv, json, time
from datetime import datetime
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from neo4j import GraphDatabase

# ── Configuration ────────────────────────────────────────────────────
NEO4J_URI      = "<YOUR_NEO4J_AURA_URI>"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"

OUTPUT_DIR = r"C:\Users\alina\OneDrive\Desktop\lineage-audit\03-governance-queries"
LOG_LINES = []
LOG_PATH = os.path.join(OUTPUT_DIR, "query_log.md")

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)

def flush_log():
    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        f.write("# Governance Analysis Query Log — Agent 2\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("```\n")
        for line in LOG_LINES:
            f.write(line + "\n")
        f.write("```\n")

def write_csv(filename, rows, fieldnames):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    log(f"  Wrote {filename}: {len(rows)} rows")
    return path


def main():
    t_start = time.time()
    log("=" * 72)
    log("AGENT 2 — GOVERNANCE ANALYSIS QUERIES — START")
    log("=" * 72)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    log("Connected to Neo4j Aura")

    # ── STEP 0: Schema verification ──────────────────────────────────
    log("")
    log("-- STEP 0: Verify Graph Schema --")
    with driver.session() as s:
        # Verify TransformEvent -> OrgEntity relationship direction
        # Check both directions
        fwd = s.run("""
            MATCH (evt:TransformEvent)-[r:TARGET_OF]->(m:OrgEntity)
            RETURN count(r) AS c
        """).single()['c']
        rev = s.run("""
            MATCH (m:OrgEntity)-[r:TARGET_OF]->(evt:TransformEvent)
            RETURN count(r) AS c
        """).single()['c']
        log(f"  TARGET_OF direction: evt->OrgEntity={fwd}, OrgEntity->evt={rev}")

        fwd_s = s.run("""
            MATCH (evt:TransformEvent)-[r:SOURCE_OF]->(m:OrgEntity)
            RETURN count(r) AS c
        """).single()['c']
        rev_s = s.run("""
            MATCH (m:OrgEntity)-[r:SOURCE_OF]->(evt:TransformEvent)
            RETURN count(r) AS c
        """).single()['c']
        log(f"  SOURCE_OF direction: evt->OrgEntity={fwd_s}, OrgEntity->evt={rev_s}")

        # Sample TransformEvent
        sample_evt = s.run("""
            MATCH (evt:TransformEvent)
            RETURN evt.event_id AS eid, evt.event_date AS edate,
                   evt.event_type AS etype, evt.political_context AS ctx
            LIMIT 5
        """).data()
        log(f"  TransformEvent sample: {sample_evt}")

        # Check event_date type
        date_types = s.run("""
            MATCH (evt:TransformEvent)
            WHERE evt.event_date IS NOT NULL
            RETURN DISTINCT apoc.meta.cypher.type(evt.event_date) AS dtype
        """).data()
        log(f"  event_date types: {date_types}")

        # Check relationship patterns for NDP events
        ndp_events = s.run("""
            MATCH (evt:TransformEvent)
            WHERE evt.political_context CONTAINS 'NDP' OR evt.political_context CONTAINS 'ndp'
               OR (evt.event_date IS NOT NULL AND toString(evt.event_date) >= '2015-05-24'
                   AND toString(evt.event_date) <= '2019-04-29')
            RETURN count(evt) AS c
        """).single()['c']
        log(f"  NDP-era TransformEvents: {ndp_events}")

        # Sample the relationship patterns
        rel_sample = s.run("""
            MATCH (evt:TransformEvent)-[r]->(m:OrgEntity)
            RETURN type(r) AS rel_type, evt.event_id AS eid, m.name AS ministry,
                   evt.event_date AS edate
            LIMIT 10
        """).data()
        log(f"  TransformEvent->OrgEntity relationships sample: {json.dumps(rel_sample, default=str)}")

        rel_sample2 = s.run("""
            MATCH (m:OrgEntity)-[r]->(evt:TransformEvent)
            RETURN type(r) AS rel_type, evt.event_id AS eid, m.name AS ministry,
                   evt.event_date AS edate
            LIMIT 10
        """).data()
        log(f"  OrgEntity->TransformEvent relationships sample: {json.dumps(rel_sample2, default=str)}")

        # Check RECEIVED_GRANT political_era values
        eras = s.run("""
            MATCH ()-[g:RECEIVED_GRANT]->()
            RETURN DISTINCT g.political_era AS era, count(g) AS cnt
            ORDER BY cnt DESC
        """).data()
        log(f"  RECEIVED_GRANT political_era distribution: {json.dumps(eras, default=str)}")

        # Check cluster_id distribution on Organizations
        cluster_stats = s.run("""
            MATCH (o:Organization)
            WHERE o.cluster_id IS NOT NULL AND o.bn IS NOT NULL
            RETURN count(DISTINCT o) AS n_clustered,
                   count(DISTINCT o.cluster_id) AS n_clusters
        """).single()
        log(f"  Clustered orgs: {cluster_stats['n_clustered']} in {cluster_stats['n_clusters']} clusters")

    flush_log()

    # ── STEP 1: Determine correct relationship direction ─────────────
    log("")
    log("-- STEP 1: Determine TransformEvent relationship patterns --")
    with driver.session() as s:
        # Get all NDP-era transform events by checking political_context or date range
        all_events = s.run("""
            MATCH (evt:TransformEvent)
            RETURN evt.event_id AS eid, evt.event_date AS edate,
                   evt.event_type AS etype, evt.political_context AS ctx,
                   evt.notes AS notes
            ORDER BY toString(evt.event_date)
        """).data()
        log(f"  Total TransformEvents: {len(all_events)}")

        # Categorize events by political context
        ndp_events_list = []
        ucp_events_list = []
        for evt in all_events:
            ctx = str(evt.get('ctx') or '').upper()
            edate = str(evt.get('edate') or '')
            if 'NDP' in ctx or ('2015-05-24' <= edate <= '2019-04-29'):
                ndp_events_list.append(evt)
            elif 'UCP' in ctx or edate >= '2019-04-30':
                ucp_events_list.append(evt)

        log(f"  NDP events: {len(ndp_events_list)}")
        log(f"  UCP events: {len(ucp_events_list)}")
        for evt in ndp_events_list[:5]:
            log(f"    {evt['eid']}: {evt['edate']} {evt['etype']} - {evt.get('ctx','')}")

        # Find which OrgEntity nodes are targets of NDP restructuring
        # Try both relationship patterns to find what works
        ndp_target_ministries_v1 = s.run("""
            MATCH (evt:TransformEvent)-[:TARGET_OF]->(m:OrgEntity)
            WHERE evt.political_context CONTAINS 'NDP'
               OR (toString(evt.event_date) >= '2015-05-24'
                   AND toString(evt.event_date) <= '2019-04-29')
            RETURN DISTINCT m.canonical_id AS cid, m.name AS name
        """).data()
        log(f"  NDP target ministries (evt-[:TARGET_OF]->OrgEntity): {len(ndp_target_ministries_v1)}")
        for m in ndp_target_ministries_v1[:10]:
            log(f"    {m['cid']}: {m['name']}")

        ndp_target_ministries_v2 = s.run("""
            MATCH (m:OrgEntity)-[:TARGET_OF]->(evt:TransformEvent)
            WHERE evt.political_context CONTAINS 'NDP'
               OR (toString(evt.event_date) >= '2015-05-24'
                   AND toString(evt.event_date) <= '2019-04-29')
            RETURN DISTINCT m.canonical_id AS cid, m.name AS name
        """).data()
        log(f"  NDP target ministries (OrgEntity-[:TARGET_OF]->evt): {len(ndp_target_ministries_v2)}")
        for m in ndp_target_ministries_v2[:10]:
            log(f"    {m['cid']}: {m['name']}")

        # Pick the version that found results
        if len(ndp_target_ministries_v1) >= len(ndp_target_ministries_v2):
            ndp_ministries = ndp_target_ministries_v1
            target_pattern = "(evt:TransformEvent)-[:TARGET_OF]->(m:OrgEntity)"
        else:
            ndp_ministries = ndp_target_ministries_v2
            target_pattern = "(m:OrgEntity)-[:TARGET_OF]->(evt:TransformEvent)"

        log(f"  Using pattern: {target_pattern}")
        log(f"  NDP-restructured ministries: {len(ndp_ministries)}")
        ndp_ministry_ids = [m['cid'] for m in ndp_ministries]
        log(f"  IDs: {ndp_ministry_ids}")

    flush_log()

    # If we found NO NDP-restructured ministries via graph traversal,
    # fall back to political_context on transform events
    if not ndp_ministry_ids:
        log("  WARNING: No NDP-restructured ministries found via TARGET_OF")
        log("  Trying: all OrgEntity with any NDP-era TransformEvent connection")
        with driver.session() as s:
            # Try any relationship between TransformEvent and OrgEntity
            ndp_ministries_fallback = s.run("""
                MATCH (evt:TransformEvent)-[r]-(m:OrgEntity)
                WHERE evt.political_context CONTAINS 'NDP'
                   OR (toString(evt.event_date) >= '2015-05-24'
                       AND toString(evt.event_date) <= '2019-04-29')
                RETURN DISTINCT m.canonical_id AS cid, m.name AS name, type(r) AS rel
            """).data()
            log(f"  Fallback NDP-linked ministries: {len(ndp_ministries_fallback)}")
            for m in ndp_ministries_fallback[:15]:
                log(f"    {m['cid']}: {m['name']} via {m['rel']}")

            ndp_ministry_ids = list(set(m['cid'] for m in ndp_ministries_fallback if m.get('cid')))
            log(f"  Unique NDP ministry IDs: {len(ndp_ministry_ids)}")

    flush_log()

    # ── QUERY 1: NDP Ministry Funding Trace ──────────────────────────
    log("")
    log("=" * 72)
    log("QUERY 1: NDP Ministry Funding Trace")
    log("=" * 72)
    t0 = time.time()

    with driver.session() as s:
        q1_results = s.run("""
            // Find organizations receiving grants through NDP-restructured ministries
            MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
            WHERE m.canonical_id IN $ndp_ministry_ids
              AND org.bn IS NOT NULL
            WITH org, m,
                 sum(CASE WHEN g.political_era = 'NDP' THEN g.amount ELSE 0 END) AS ndp_funding,
                 sum(CASE WHEN g.political_era IN ['UCP_Kenney', 'UCP_Smith'] THEN g.amount ELSE 0 END) AS ucp_funding,
                 sum(CASE WHEN g.political_era = 'PC' THEN g.amount ELSE 0 END) AS pc_funding,
                 count(g) AS n_grants
            WITH org,
                 collect(DISTINCT m.name) AS ndp_ministries,
                 sum(ndp_funding) AS total_ndp,
                 sum(ucp_funding) AS total_ucp,
                 sum(pc_funding) AS total_pc,
                 sum(n_grants) AS total_grants

            // Enrich with risk flags
            OPTIONAL MATCH (org)-[:FLAGGED_AS]->(flag:RiskFlag)
            WITH org, ndp_ministries, total_ndp, total_ucp, total_pc, total_grants,
                 collect(DISTINCT flag.flag_type) AS risk_flags

            WHERE total_ndp > 0 OR total_ucp > 0  // Any funding through NDP-restructured ministries
            RETURN org.name AS org_name, org.bn AS bn, org.city AS city,
                   org.cluster_id AS cluster_id, org.cluster_size AS cluster_size,
                   total_ndp, total_ucp, total_pc,
                   CASE WHEN total_ndp > 0 THEN round((total_ucp - total_ndp) * 100.0 / total_ndp) ELSE null END AS delta_pct,
                   total_grants, ndp_ministries, risk_flags,
                   size(risk_flags) AS n_flags
            ORDER BY total_ndp DESC
        """, ndp_ministry_ids=ndp_ministry_ids).data()

    log(f"  Query 1 returned {len(q1_results)} organizations")
    log(f"  Completed in {time.time()-t0:.1f}s")

    if q1_results:
        # Summary stats
        total_ndp_all = sum(r.get('total_ndp') or 0 for r in q1_results)
        total_ucp_all = sum(r.get('total_ucp') or 0 for r in q1_results)
        total_pc_all = sum(r.get('total_pc') or 0 for r in q1_results)
        clustered_count = sum(1 for r in q1_results if r.get('cluster_id') is not None)
        flagged_count = sum(1 for r in q1_results if r.get('n_flags', 0) > 0)

        log(f"  Total NDP-era funding through NDP-restructured ministries: ${total_ndp_all:,.0f}")
        log(f"  Total UCP-era funding through same ministries: ${total_ucp_all:,.0f}")
        log(f"  Total PC-era funding through same ministries: ${total_pc_all:,.0f}")
        if total_ndp_all > 0:
            log(f"  NDP->UCP change: {(total_ucp_all - total_ndp_all) / total_ndp_all * 100:.1f}%")
        log(f"  Organizations in governance clusters: {clustered_count} / {len(q1_results)}")
        log(f"  Organizations with risk flags: {flagged_count} / {len(q1_results)}")

        log(f"  Top 10 by NDP funding:")
        for r in q1_results[:10]:
            ndp = r.get('total_ndp') or 0
            ucp = r.get('total_ucp') or 0
            delta = r.get('delta_pct')
            delta_str = f"{delta:+.0f}%" if delta is not None else "N/A"
            flags = r.get('risk_flags', [])
            cluster = f"cluster:{r['cluster_id']}" if r.get('cluster_id') else "no cluster"
            log(f"    {r['org_name']}: NDP=${ndp:,.0f} UCP=${ucp:,.0f} ({delta_str}) [{cluster}] flags={flags}")

        # Write CSV
        fieldnames = ['org_name', 'bn', 'city', 'cluster_id', 'cluster_size',
                      'total_ndp', 'total_ucp', 'total_pc', 'delta_pct',
                      'total_grants', 'ndp_ministries', 'risk_flags', 'n_flags']
        rows = []
        for r in q1_results:
            row = {k: r.get(k) for k in fieldnames}
            row['ndp_ministries'] = '|'.join(r.get('ndp_ministries', []))
            row['risk_flags'] = '|'.join(r.get('risk_flags', []))
            rows.append(row)
        write_csv("q1_ndp_ministry_funding_trace.csv", rows, fieldnames)
    else:
        log("  WARNING: No results from Query 1!")

    flush_log()

    # ── QUERY 2: Director-Cluster-Funding-Concentration ──────────────
    log("")
    log("=" * 72)
    log("QUERY 2: Director-Cluster-Funding-Concentration")
    log("=" * 72)
    t0 = time.time()

    with driver.session() as s:
        # Compare clustered vs non-clustered orgs funding through NDP-restructured ministries
        q2_results = s.run("""
            // All orgs that received NDP-era grants through NDP-restructured ministries
            MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
            WHERE m.canonical_id IN $ndp_ministry_ids
              AND g.political_era = 'NDP'
              AND org.bn IS NOT NULL
            WITH org, sum(g.amount) AS ndp_funding

            // Split by cluster membership
            WITH org, ndp_funding,
                 CASE WHEN org.cluster_id IS NOT NULL THEN true ELSE false END AS is_clustered

            // Aggregate
            WITH is_clustered,
                 count(org) AS n_orgs,
                 sum(ndp_funding) AS total_funding,
                 avg(ndp_funding) AS avg_per_org,
                 percentileCont(ndp_funding, 0.5) AS median_per_org,
                 percentileCont(ndp_funding, 0.75) AS p75_per_org,
                 percentileCont(ndp_funding, 0.95) AS p95_per_org,
                 min(ndp_funding) AS min_per_org,
                 max(ndp_funding) AS max_per_org

            RETURN is_clustered, n_orgs, total_funding,
                   round(avg_per_org) AS avg_per_org,
                   round(median_per_org) AS median_per_org,
                   round(p75_per_org) AS p75_per_org,
                   round(p95_per_org) AS p95_per_org,
                   round(min_per_org) AS min_per_org,
                   round(max_per_org) AS max_per_org
            ORDER BY is_clustered DESC
        """, ndp_ministry_ids=ndp_ministry_ids).data()

    log(f"  Query 2 returned {len(q2_results)} rows")
    log(f"  Completed in {time.time()-t0:.1f}s")

    if q2_results:
        for r in q2_results:
            label = "CLUSTERED" if r['is_clustered'] else "NON-CLUSTERED"
            log(f"  {label}: {r['n_orgs']} orgs, total=${r['total_funding']:,.0f}, avg=${r['avg_per_org']:,.0f}, median=${r['median_per_org']:,.0f}")

        # Calculate disparity ratio
        clustered = next((r for r in q2_results if r['is_clustered']), None)
        non_clustered = next((r for r in q2_results if not r['is_clustered']), None)
        if clustered and non_clustered and non_clustered['avg_per_org'] > 0:
            ratio = clustered['avg_per_org'] / non_clustered['avg_per_org']
            log(f"  DISPARITY RATIO (avg clustered / avg non-clustered): {ratio:.2f}x")
            median_ratio = clustered['median_per_org'] / non_clustered['median_per_org'] if non_clustered['median_per_org'] > 0 else None
            if median_ratio:
                log(f"  MEDIAN DISPARITY RATIO: {median_ratio:.2f}x")

        fieldnames = ['is_clustered', 'n_orgs', 'total_funding', 'avg_per_org',
                      'median_per_org', 'p75_per_org', 'p95_per_org', 'min_per_org', 'max_per_org']
        write_csv("q2_cluster_funding_concentration.csv", q2_results, fieldnames)
    else:
        log("  WARNING: No results from Query 2!")

    flush_log()

    # ── QUERY 2b: Same comparison but for ALL eras (not just NDP) ────
    log("")
    log("-- Query 2b: Clustered vs Non-Clustered (ALL eras) --")
    with driver.session() as s:
        q2b_results = s.run("""
            MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
            WHERE m.canonical_id IN $ndp_ministry_ids
              AND org.bn IS NOT NULL
            WITH org,
                 sum(CASE WHEN g.political_era = 'NDP' THEN g.amount ELSE 0 END) AS ndp_funding,
                 sum(CASE WHEN g.political_era IN ['UCP_Kenney', 'UCP_Smith'] THEN g.amount ELSE 0 END) AS ucp_funding,
                 sum(CASE WHEN g.political_era = 'PC' THEN g.amount ELSE 0 END) AS pc_funding,
                 sum(g.amount) AS total_all

            WITH org, ndp_funding, ucp_funding, pc_funding, total_all,
                 CASE WHEN org.cluster_id IS NOT NULL THEN true ELSE false END AS is_clustered

            RETURN is_clustered,
                   count(org) AS n_orgs,
                   sum(ndp_funding) AS total_ndp,
                   sum(ucp_funding) AS total_ucp,
                   sum(pc_funding) AS total_pc,
                   sum(total_all) AS total_all,
                   avg(ndp_funding) AS avg_ndp,
                   avg(ucp_funding) AS avg_ucp,
                   avg(pc_funding) AS avg_pc
            ORDER BY is_clustered DESC
        """, ndp_ministry_ids=ndp_ministry_ids).data()

    if q2b_results:
        for r in q2b_results:
            label = "CLUSTERED" if r['is_clustered'] else "NON-CLUSTERED"
            log(f"  {label}: {r['n_orgs']} orgs — PC=${r['total_pc']:,.0f} NDP=${r['total_ndp']:,.0f} UCP=${r['total_ucp']:,.0f}")

    flush_log()

    # ── QUERY 3: Governance Cluster NDP Audit ────────────────────────
    log("")
    log("=" * 72)
    log("QUERY 3: Governance Cluster NDP Audit")
    log("=" * 72)
    t0 = time.time()

    with driver.session() as s:
        q3_results = s.run("""
            // Find all clustered organizations with grants through NDP-restructured ministries
            MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
            WHERE m.canonical_id IN $ndp_ministry_ids
              AND org.cluster_id IS NOT NULL
              AND org.bn IS NOT NULL
            WITH org.cluster_id AS cluster_id, org,
                 sum(CASE WHEN g.political_era = 'NDP' THEN g.amount ELSE 0 END) AS org_ndp,
                 sum(CASE WHEN g.political_era IN ['UCP_Kenney', 'UCP_Smith'] THEN g.amount ELSE 0 END) AS org_ucp,
                 collect(DISTINCT m.name) AS org_ministries

            // Aggregate per cluster
            WITH cluster_id,
                 count(DISTINCT org) AS cluster_grant_recipients,
                 sum(org_ndp) AS cluster_ndp_total,
                 sum(org_ucp) AS cluster_ucp_total,
                 collect(DISTINCT org.name)[..8] AS sample_orgs

            // Get cluster size and flags
            OPTIONAL MATCH (member:Organization {cluster_id: cluster_id})
            WITH cluster_id, cluster_grant_recipients, cluster_ndp_total, cluster_ucp_total,
                 sample_orgs, count(DISTINCT member) AS total_cluster_size

            // Get risk flags for cluster members
            OPTIONAL MATCH (flagged:Organization {cluster_id: cluster_id})-[:FLAGGED_AS]->(f:RiskFlag)
            WITH cluster_id, cluster_grant_recipients, cluster_ndp_total, cluster_ucp_total,
                 sample_orgs, total_cluster_size,
                 count(DISTINCT f.flag_type) AS distinct_flag_types,
                 count(f) AS total_flags_in_cluster

            WHERE cluster_ndp_total > 0
            RETURN cluster_id, total_cluster_size, cluster_grant_recipients,
                   cluster_ndp_total, cluster_ucp_total,
                   CASE WHEN cluster_ndp_total > 0
                        THEN round((cluster_ucp_total - cluster_ndp_total) * 100.0 / cluster_ndp_total)
                        ELSE null END AS delta_pct,
                   distinct_flag_types, total_flags_in_cluster,
                   sample_orgs
            ORDER BY cluster_ndp_total DESC
        """, ndp_ministry_ids=ndp_ministry_ids).data()

    log(f"  Query 3 returned {len(q3_results)} clusters")
    log(f"  Completed in {time.time()-t0:.1f}s")

    if q3_results:
        total_cluster_ndp = sum(r.get('cluster_ndp_total') or 0 for r in q3_results)
        total_cluster_ucp = sum(r.get('cluster_ucp_total') or 0 for r in q3_results)
        log(f"  Total clustered NDP funding: ${total_cluster_ndp:,.0f}")
        log(f"  Total clustered UCP funding: ${total_cluster_ucp:,.0f}")

        log(f"  Top 15 clusters by NDP funding:")
        for r in q3_results[:15]:
            ndp = r.get('cluster_ndp_total') or 0
            ucp = r.get('cluster_ucp_total') or 0
            delta = r.get('delta_pct')
            delta_str = f"{delta:+.0f}%" if delta is not None else "N/A"
            orgs = r.get('sample_orgs', [])
            log(f"    Cluster {r['cluster_id']}: size={r['total_cluster_size']}, "
                f"recipients={r['cluster_grant_recipients']}, "
                f"NDP=${ndp:,.0f}, UCP=${ucp:,.0f} ({delta_str}), "
                f"flags={r['total_flags_in_cluster']}")
            log(f"      orgs: {', '.join(str(o) for o in orgs[:4])}")

        fieldnames = ['cluster_id', 'total_cluster_size', 'cluster_grant_recipients',
                      'cluster_ndp_total', 'cluster_ucp_total', 'delta_pct',
                      'distinct_flag_types', 'total_flags_in_cluster', 'sample_orgs']
        rows = []
        for r in q3_results:
            row = {k: r.get(k) for k in fieldnames}
            row['sample_orgs'] = '|'.join(str(o) for o in r.get('sample_orgs', []))
            rows.append(row)
        write_csv("q3_cluster_ndp_audit.csv", rows, fieldnames)
    else:
        log("  WARNING: No results from Query 3!")

    flush_log()

    # ── SYMMETRY TEST: UCP Era ───────────────────────────────────────
    log("")
    log("=" * 72)
    log("SYMMETRY TEST: UCP-Restructured Ministries")
    log("=" * 72)
    t0 = time.time()

    # Find UCP-restructured ministries
    with driver.session() as s:
        # UCP-era transform events
        if "(evt:TransformEvent)-[:TARGET_OF]->(m:OrgEntity)" in target_pattern:
            ucp_ministries = s.run("""
                MATCH (evt:TransformEvent)-[:TARGET_OF]->(m:OrgEntity)
                WHERE evt.political_context CONTAINS 'UCP'
                   OR toString(evt.event_date) >= '2019-04-30'
                RETURN DISTINCT m.canonical_id AS cid, m.name AS name
            """).data()
        else:
            ucp_ministries = s.run("""
                MATCH (m:OrgEntity)-[:TARGET_OF]->(evt:TransformEvent)
                WHERE evt.political_context CONTAINS 'UCP'
                   OR toString(evt.event_date) >= '2019-04-30'
                RETURN DISTINCT m.canonical_id AS cid, m.name AS name
            """).data()

        if not ucp_ministries:
            # Fallback: any relationship
            ucp_ministries = s.run("""
                MATCH (evt:TransformEvent)-[r]-(m:OrgEntity)
                WHERE evt.political_context CONTAINS 'UCP'
                   OR toString(evt.event_date) >= '2019-04-30'
                RETURN DISTINCT m.canonical_id AS cid, m.name AS name
            """).data()

        ucp_ministry_ids = [m['cid'] for m in ucp_ministries if m.get('cid')]
        log(f"  UCP-restructured ministries: {len(ucp_ministry_ids)}")
        for m in ucp_ministries[:10]:
            log(f"    {m['cid']}: {m['name']}")

    flush_log()

    if ucp_ministry_ids:
        # Symmetry Query 1: UCP funding trace
        with driver.session() as s:
            sym1 = s.run("""
                MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
                WHERE m.canonical_id IN $ucp_ministry_ids
                  AND org.bn IS NOT NULL
                WITH org,
                     sum(CASE WHEN g.political_era IN ['UCP_Kenney', 'UCP_Smith'] THEN g.amount ELSE 0 END) AS ucp_funding,
                     sum(CASE WHEN g.political_era = 'NDP' THEN g.amount ELSE 0 END) AS ndp_funding,
                     count(g) AS n_grants
                WHERE ucp_funding > 0 OR ndp_funding > 0
                WITH CASE WHEN org.cluster_id IS NOT NULL THEN true ELSE false END AS is_clustered,
                     count(org) AS n_orgs,
                     sum(ucp_funding) AS total_ucp,
                     sum(ndp_funding) AS total_ndp,
                     avg(ucp_funding) AS avg_ucp,
                     avg(ndp_funding) AS avg_ndp
                RETURN is_clustered, n_orgs, total_ucp, total_ndp,
                       round(avg_ucp) AS avg_ucp, round(avg_ndp) AS avg_ndp
                ORDER BY is_clustered DESC
            """, ucp_ministry_ids=ucp_ministry_ids).data()

        log(f"  UCP Symmetry: Clustered vs Non-Clustered through UCP-restructured ministries:")
        for r in sym1:
            label = "CLUSTERED" if r['is_clustered'] else "NON-CLUSTERED"
            log(f"    {label}: {r['n_orgs']} orgs, UCP=${r['total_ucp']:,.0f} (avg=${r['avg_ucp']:,.0f})")

        # Symmetry disparity ratio
        s_clustered = next((r for r in sym1 if r['is_clustered']), None)
        s_non_clustered = next((r for r in sym1 if not r['is_clustered']), None)
        if s_clustered and s_non_clustered and s_non_clustered['avg_ucp'] > 0:
            s_ratio = s_clustered['avg_ucp'] / s_non_clustered['avg_ucp']
            log(f"  UCP DISPARITY RATIO: {s_ratio:.2f}x")
    else:
        log("  No UCP-restructured ministries found — skipping symmetry test")

    flush_log()

    # ── BONUS: Per-era comparison using SAME ministries ──────────────
    log("")
    log("=" * 72)
    log("BONUS: Same NDP-restructured ministries across ALL eras")
    log("=" * 72)
    with driver.session() as s:
        bonus = s.run("""
            MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
            WHERE m.canonical_id IN $ndp_ministry_ids
              AND org.bn IS NOT NULL
            WITH g.political_era AS era,
                 count(DISTINCT org) AS n_orgs,
                 sum(g.amount) AS total_amount,
                 count(g) AS n_grants,
                 avg(g.amount) AS avg_grant
            RETURN era, n_orgs, total_amount, n_grants, round(avg_grant) AS avg_grant
            ORDER BY total_amount DESC
        """, ndp_ministry_ids=ndp_ministry_ids).data()

    if bonus:
        log(f"  Funding through NDP-RESTRUCTURED ministries by political era:")
        for r in bonus:
            log(f"    {r['era']}: {r['n_orgs']} orgs, ${r['total_amount']:,.0f}, {r['n_grants']} grants, avg=${r['avg_grant']:,.0f}")

    flush_log()

    # ── BONUS 2: Top shared-director connections among grant recipients ──
    log("")
    log("=" * 72)
    log("BONUS 2: Shared director links among top NDP grant recipients")
    log("=" * 72)
    with driver.session() as s:
        shared_dir_grants = s.run("""
            // Find pairs of orgs that share directors AND both got NDP grants
            MATCH (o1:Organization)-[sd:SHARED_DIRECTORS]->(o2:Organization)
            WHERE o1.bn IS NOT NULL AND o2.bn IS NOT NULL
            MATCH (o1)-[g1:RECEIVED_GRANT {political_era: 'NDP'}]->(m1:OrgEntity)
            WHERE m1.canonical_id IN $ndp_ministry_ids
            MATCH (o2)-[g2:RECEIVED_GRANT {political_era: 'NDP'}]->(m2:OrgEntity)
            WHERE m2.canonical_id IN $ndp_ministry_ids
            WITH o1, o2, sd.n_shared_directors AS n_shared,
                 sum(DISTINCT g1.amount) AS o1_ndp, sum(DISTINCT g2.amount) AS o2_ndp
            RETURN o1.name AS org1, o2.name AS org2, n_shared,
                   o1_ndp, o2_ndp,
                   o1.cluster_id AS cluster1, o2.cluster_id AS cluster2
            ORDER BY n_shared DESC, o1_ndp + o2_ndp DESC
            LIMIT 25
        """, ndp_ministry_ids=ndp_ministry_ids).data()

    log(f"  Shared director pairs (both NDP grantees): {len(shared_dir_grants)}")
    for r in shared_dir_grants[:15]:
        log(f"    {r['org1']} <-> {r['org2']}: {r['n_shared']} shared dirs, "
            f"NDP: ${r['o1_ndp']:,.0f} + ${r['o2_ndp']:,.0f}")

    if shared_dir_grants:
        fieldnames = ['org1', 'org2', 'n_shared', 'o1_ndp', 'o2_ndp', 'cluster1', 'cluster2']
        write_csv("bonus_shared_director_grantees.csv", shared_dir_grants, fieldnames)

    flush_log()

    # ── FINAL SUMMARY ────────────────────────────────────────────────
    log("")
    log("=" * 72)
    log("PHASE 2 COMPLETE — SUMMARY")
    log("=" * 72)

    elapsed = time.time() - t_start
    log(f"Total elapsed: {elapsed:.1f}s ({elapsed/60:.1f} min)")

    log("")
    log("Output files:")
    for f in os.listdir(OUTPUT_DIR):
        if f.endswith('.csv'):
            fpath = os.path.join(OUTPUT_DIR, f)
            with open(fpath, 'r', encoding='utf-8') as fh:
                lines = sum(1 for _ in fh) - 1  # subtract header
            log(f"  {f}: {lines} rows")

    driver.close()
    flush_log()
    print(f"\nLog written to: {LOG_PATH}")


if __name__ == "__main__":
    main()
