#!/usr/bin/env python
"""Quick verification of federal grants ingestion."""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from neo4j import GraphDatabase

NEO4J_URI      = "<YOUR_NEO4J_AURA_URI>"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as s:
    # 1. Count FUNDED_BY_FED relationships
    cnt = s.run("MATCH ()-[r:FUNDED_BY_FED]->() RETURN count(r) AS c").single()['c']
    print(f"1. Total FUNDED_BY_FED relationships: {cnt}")

    # 2. Count FederalDepartment nodes
    cnt = s.run("MATCH (fd:FederalDepartment) RETURN count(fd) AS c").single()['c']
    print(f"2. Total FederalDepartment nodes: {cnt}")

    # 3. Edge counts per department
    data = s.run("""
        MATCH (o:Organization)-[r:FUNDED_BY_FED]->(fd:FederalDepartment)
        RETURN fd.name AS dept, count(r) AS cnt, count(DISTINCT o) AS orgs, sum(r.amount) AS total
        ORDER BY cnt DESC
    """).data()
    print("\n3. FUNDED_BY_FED by department:")
    for r in data:
        t = r['total'] if r['total'] else 0
        print(f"   {r['dept']}: {r['cnt']} edges, {r['orgs']} orgs, ${t:,.0f}")

    # 4. Dual-funded orgs (both GOA and Federal)
    dual = s.run("""
        MATCH (o:Organization)-[:RECEIVED_GRANT]->(m:OrgEntity)
        WHERE EXISTS { (o)-[:FUNDED_BY_FED]->(:FederalDepartment) }
        RETURN count(DISTINCT o) AS cnt
    """).single()['cnt']
    print(f"\n4. Dual-funded orgs (GOA + Federal): {dual}")

    # 5. Sample dual-funded orgs (unique)
    samples = s.run("""
        MATCH (o:Organization)
        WHERE EXISTS { (o)-[:RECEIVED_GRANT]->(:OrgEntity) }
          AND EXISTS { (o)-[:FUNDED_BY_FED]->(:FederalDepartment) }
        WITH o
        OPTIONAL MATCH (o)-[g:RECEIVED_GRANT]->(m:OrgEntity)
        WITH o, count(DISTINCT m) AS goa_ministries
        OPTIONAL MATCH (o)-[f:FUNDED_BY_FED]->(fd:FederalDepartment)
        WITH o, goa_ministries, count(DISTINCT fd) AS fed_depts, sum(f.amount) AS fed_total
        RETURN o.name AS name, o.bn AS bn, goa_ministries, fed_depts, fed_total
        ORDER BY fed_total DESC
        LIMIT 5
    """).data()
    print("\n5. Top 5 dual-funded orgs:")
    for r in samples:
        t = r['fed_total'] if r['fed_total'] else 0
        print(f"   {r['name']} (BN {r['bn']}): {r['goa_ministries']} GOA ministries, {r['fed_depts']} fed depts, ${t:,.0f} fed total")

    # 6. Federal-only and GOA-only counts
    fed_only = s.run("""
        MATCH (o:Organization)-[:FUNDED_BY_FED]->(:FederalDepartment)
        WHERE NOT EXISTS { (o)-[:RECEIVED_GRANT]->(:OrgEntity) }
          AND o.bn IS NOT NULL
        RETURN count(DISTINCT o) AS cnt
    """).single()['cnt']
    goa_only = s.run("""
        MATCH (o:Organization)-[:RECEIVED_GRANT]->(:OrgEntity)
        WHERE NOT EXISTS { (o)-[:FUNDED_BY_FED]->(:FederalDepartment) }
          AND o.bn IS NOT NULL
        RETURN count(DISTINCT o) AS cnt
    """).single()['cnt']
    print(f"\n6. Federal-only orgs: {fed_only}")
    print(f"   GOA-only orgs: {goa_only}")

    # 7. Check for the 'None' department issue
    none_dept = s.run("""
        MATCH ()-[r:FUNDED_BY_FED]->(fd:FederalDepartment)
        WHERE fd.name = 'None'
        RETURN count(r) AS cnt
    """).single()['cnt']
    print(f"\n7. Edges pointing to 'None' dept: {none_dept}")

    # 8. Fiscal year distribution
    fy_data = s.run("""
        MATCH ()-[r:FUNDED_BY_FED]->()
        RETURN r.fiscal_year AS fy, count(r) AS cnt
        ORDER BY r.fiscal_year
    """).data()
    print("\n8. FUNDED_BY_FED by fiscal year:")
    for r in fy_data:
        print(f"   {r['fy']}: {r['cnt']} edges")

    # 9. Risk-flagged orgs receiving federal funding
    flagged = s.run("""
        MATCH (o:Organization)-[:FUNDED_BY_FED]->(:FederalDepartment)
        WHERE EXISTS { (o)-[:FLAGGED_AS]->(:RiskFlag) }
        RETURN count(DISTINCT o) AS cnt
    """).single()['cnt']
    print(f"\n9. Risk-flagged orgs with federal funding: {flagged}")

driver.close()
print("\nVerification complete.")
