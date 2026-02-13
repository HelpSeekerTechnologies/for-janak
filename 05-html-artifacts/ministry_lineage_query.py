"""
ministry_lineage_query.py
Connect to Neo4j Aura and extract FULL ministry lineage data
for building an accurate Sankey diagram of Alberta ministry
restructuring flows across PC -> NDP -> UCP eras.
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from neo4j import GraphDatabase

URI      = "<YOUR_NEO4J_AURA_URI>"
USER     = "neo4j"
PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"

# -- helpers --
def run_query(driver, title, cypher):
    """Run a Cypher query, print every record, and return the list."""
    print("=" * 90)
    print(f"  {title}")
    print("=" * 90)
    records = []
    with driver.session() as session:
        result = session.run(cypher)
        keys = result.keys()
        print("  | ".join(str(k) for k in keys))
        print("-" * 90)
        for rec in result:
            row = {k: rec[k] for k in keys}
            records.append(row)
            print("  | ".join(str(row[k]) for k in keys))
    print(f"\n>>> {len(records)} record(s) returned\n\n")
    return records


# -- main --
def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    driver.verify_connectivity()
    print("Connected to Neo4j Aura successfully.\n")

    # 1. All OrgEntity nodes
    q1 = """\
MATCH (m:OrgEntity)
RETURN m.canonical_id AS id, m.name AS name, m.status AS status,
       m.start_date AS start, m.end_date AS end_date, m.level AS level
ORDER BY m.name"""
    run_query(driver, "QUERY 1 -- All OrgEntity nodes", q1)

    # 2. All TransformEvent → OrgEntity relationships (full restructuring chain)
    q2 = """\
MATCH (source:OrgEntity)-[:SOURCE_OF]->(evt:TransformEvent)-[:TARGET_OF]->(target:OrgEntity)
RETURN source.canonical_id AS source_id, source.name AS source_name,
       evt.event_id AS event_id, evt.event_type AS event_type,
       evt.event_date AS event_date, evt.political_context AS context,
       target.canonical_id AS target_id, target.name AS target_name
ORDER BY toString(evt.event_date), evt.event_id"""
    run_query(driver, "QUERY 2 -- Full restructuring chain (SOURCE_OF -> TransformEvent -> TARGET_OF)", q2)

    # 3. Current active ministries
    q3 = """\
MATCH (m:OrgEntity)
WHERE m.end_date IS NULL OR toString(m.end_date) > '2025-01-01'
RETURN m.canonical_id AS id, m.name AS name, m.status AS status
ORDER BY m.name"""
    run_query(driver, "QUERY 3 -- Current active ministries", q3)

    # 4. UCP-Smith era TransformEvents (2022-10-11+)
    q4 = """\
MATCH (source:OrgEntity)-[:SOURCE_OF]->(evt:TransformEvent)-[:TARGET_OF]->(target:OrgEntity)
WHERE toString(evt.event_date) >= '2022-10-11'
RETURN source.name AS source_name, evt.event_type AS type,
       evt.event_date AS date, evt.political_context AS context,
       target.name AS target_name
ORDER BY toString(evt.event_date)"""
    run_query(driver, "QUERY 4 -- UCP-Smith era TransformEvents (2022-10-11+)", q4)

    # 5. Most recent TransformEvents
    q5 = """\
MATCH (evt:TransformEvent)
RETURN evt.event_id AS id, evt.event_date AS date, evt.event_type AS type,
       evt.political_context AS context
ORDER BY toString(evt.event_date) DESC
LIMIT 20"""
    run_query(driver, "QUERY 5 -- Most recent 20 TransformEvents", q5)

    # 6. RECEIVED_GRANT totals by ministry
    q6 = """\
MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity)
WHERE org.bn IS NOT NULL
WITH m.canonical_id AS mid, m.name AS ministry, g.political_era AS era,
     sum(g.amount) AS total, count(g) AS n_grants
RETURN mid, ministry, era, total, n_grants
ORDER BY total DESC
LIMIT 100"""
    run_query(driver, "QUERY 6 -- RECEIVED_GRANT totals by ministry (top 100)", q6)

    driver.close()
    print("Done -- all queries complete.")


if __name__ == "__main__":
    main()
