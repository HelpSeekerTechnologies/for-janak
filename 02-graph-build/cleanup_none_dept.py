#!/usr/bin/env python
"""Remove the 'None' FederalDepartment node and its FUNDED_BY_FED edges."""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from neo4j import GraphDatabase

NEO4J_URI      = "<YOUR_NEO4J_AURA_URI>"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as s:
    # Count before
    cnt_before = s.run("MATCH ()-[r:FUNDED_BY_FED]->() RETURN count(r) AS c").single()['c']
    print(f"FUNDED_BY_FED edges before cleanup: {cnt_before}")

    cnt_none = s.run("""
        MATCH ()-[r:FUNDED_BY_FED]->(fd:FederalDepartment {name: 'None'})
        RETURN count(r) AS c
    """).single()['c']
    print(f"Edges to 'None' department: {cnt_none}")

    # Delete edges to 'None' department
    s.run("""
        MATCH ()-[r:FUNDED_BY_FED]->(fd:FederalDepartment {name: 'None'})
        DELETE r
    """)
    print("Deleted FUNDED_BY_FED edges to 'None' department")

    # Delete the 'None' node itself
    s.run("MATCH (fd:FederalDepartment {name: 'None'}) DELETE fd")
    print("Deleted 'None' FederalDepartment node")

    # Count after
    cnt_after = s.run("MATCH ()-[r:FUNDED_BY_FED]->() RETURN count(r) AS c").single()['c']
    cnt_depts = s.run("MATCH (fd:FederalDepartment) RETURN count(fd) AS c").single()['c']
    print(f"FUNDED_BY_FED edges after cleanup: {cnt_after}")
    print(f"FederalDepartment nodes after cleanup: {cnt_depts}")

driver.close()
print("Cleanup complete.")
