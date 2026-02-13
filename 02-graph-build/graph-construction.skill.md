# Graph Construction Skill

Design, deploy, and populate a unified KGL v1.3-aligned Neo4j knowledge graph combining ministry lineage, charity organizations, director networks, grant flows, and risk flags. Extends existing Neo4j Aura graph (264 nodes already loaded — D011).

---

## When to Use

- Generating the unified Neo4j schema (DDL)
- Writing or running the ingestion pipeline
- Validating graph integrity post-ingestion
- Extending the schema with new node/relationship types

---

## Neo4j Instance (Aura — D011)

```python
# Connection details
NEO4J_URI = "<YOUR_NEO4J_AURA_URI>"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"

from neo4j import GraphDatabase
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
```

- **URI:** `<YOUR_NEO4J_AURA_URI>`
- **Auth:** neo4j / <YOUR_NEO4J_AURA_PASSWORD>
- **Existing data:** 264 nodes, 317 relationships (ministry lineage from Archana's notebook)
- **Strategy:** MERGE to extend — existing nodes matched idempotently, new nodes added

---

## Schema: 12 KGL Node Types

```cypher
// ━━━ MINISTRY LINEAGE (264 nodes already in Aura from Archana's notebook) ━━━

// ▣ program — Government ministry/department (can transform)
CREATE CONSTRAINT ministry_id IF NOT EXISTS
FOR (m:Ministry) REQUIRE m.canonical_id IS UNIQUE;

// ⧫ event — Transformation event (SPLIT, MERGE, RENAME, etc.)
CREATE CONSTRAINT event_id IF NOT EXISTS
FOR (e:TransformEvent) REQUIRE e.event_id IS UNIQUE;

// ⟦ record — Source document (OIC, grants CSV, mandate letter)
CREATE CONSTRAINT source_doc_id IF NOT EXISTS
FOR (d:SourceDocument) REQUIRE d.doc_id IS UNIQUE;

// ━━━ CHARITY ORGANIZATIONS (from CRA T3010 via Databricks) ━━━

// ᚴ organization — Registered charity
CREATE CONSTRAINT org_bn IF NOT EXISTS
FOR (o:Organization) REQUIRE o.bn IS UNIQUE;

// ◎ person — Board director
CREATE CONSTRAINT director_id IF NOT EXISTS
FOR (d:Director) REQUIRE d.normalized_name IS UNIQUE;

// ━━━ FUNDING (from GOA grants, federal G&C via Databricks) ━━━

// ◉ resource — Grant aggregate (org × ministry × fiscal year)
// No unique constraint — composite key (org_bn, ministry_id, fiscal_year)

// ⟲ timeframe — Fiscal year
CREATE CONSTRAINT fy_id IF NOT EXISTS
FOR (fy:FiscalYear) REQUIRE fy.year IS UNIQUE;

// ━━━ CLASSIFICATION ━━━

// ᚪ geography — Region
CREATE CONSTRAINT region_id IF NOT EXISTS
FOR (r:Region) REQUIRE r.name IS UNIQUE;

// ⟡ measurement — Risk flag type
CREATE CONSTRAINT flag_id IF NOT EXISTS
FOR (f:RiskFlag) REQUIRE f.flag_type IS UNIQUE;

// Indexes for query performance
CREATE INDEX ministry_name IF NOT EXISTS FOR (m:Ministry) ON (m.name);
CREATE INDEX org_name IF NOT EXISTS FOR (o:Organization) ON (o.name);
CREATE INDEX director_name IF NOT EXISTS FOR (d:Director) ON (d.normalized_name);
CREATE INDEX grant_era IF NOT EXISTS FOR ()-[g:RECEIVED_GRANT]-() ON (g.political_era);
```

## 10 Relationship Types

```cypher
// Ministry lineage (already in Aura — 317 relationships)
// (:Ministry)-[:SOURCE_OF]->(:TransformEvent)     — predecessor
// (:TransformEvent)-[:TARGET_OF]->(:Ministry)     — successor
// (:TransformEvent)-[:EVIDENCED_BY]->(:SourceDocument)
// (:Ministry)-[:PARENT_OF]->(:Ministry)           — temporal hierarchy

// Grant flows (new — to be added)
// (:Organization)-[:RECEIVED_GRANT {amount, fiscal_year, political_era, n_payments}]->(:Ministry)

// Director governance (new — to be added)
// (:Director)-[:SITS_ON {position, start_date, end_date}]->(:Organization)

// Risk classification (new — to be added)
// (:Organization)-[:FLAGGED_AS {severity}]->(:RiskFlag)

// Geography (new — to be added)
// (:Organization)-[:LOCATED_IN]->(:Region)

// Cluster edges (new — to be added)
// (:Organization)-[:CLUSTER_MEMBER {cluster_id, shared_directors}]->(:Organization)

// Federal funding (new — to be added)
// (:Organization)-[:FUNDED_BY_FED {amount, department, program}]->(:FiscalYear)
```

---

## Ingestion Order (Critical)

**Always MERGE, never CREATE.** Entities appear in multiple datasets. Note: 264 nodes and 317 relationships already exist in Aura (D011) — MERGE will be idempotent.

1. **FiscalYear nodes** (11) — independent
2. **Region nodes** (~7) — independent
3. **RiskFlag nodes** (11 types) — independent
4. **Ministry lineage** — MERGE existing (idempotent against 264 Aura nodes from Archana's notebook)
5. **Organization nodes** (9,145) — from Databricks `ab_org_risk_flags` / `ab_master_profile`
6. **Director nodes** (~19K multi-board) — from Databricks `multi_board_directors`
7. **SITS_ON edges** — director → organization
8. **RECEIVED_GRANT edges** — organization → ministry (from aggregated grants via Databricks `goa_grants_disclosure`)
9. **FLAGGED_AS edges** — organization → risk flag
10. **CLUSTER_MEMBER edges** — organization → organization (from Databricks `org_clusters_strong`)
11. **FUNDED_BY_FED edges** — organization → fiscal year

---

## KGL Property Requirements

Every node MUST carry:
```
kgl: '{glyph}'          // e.g., '▣', 'ᚴ', '◎'
kgl_handle: '{handle}'  // e.g., 'program', 'organization', 'person'
```

---

## Validation Queries (Post-Ingestion)

```cypher
// Count all node types
MATCH (n) RETURN labels(n)[0] AS type, count(n) AS count ORDER BY count DESC;

// Count all relationship types
MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS count ORDER BY count DESC;

// Check for orphan organizations (no grants, no directors)
MATCH (o:Organization)
WHERE NOT (o)<-[:SITS_ON]-() AND NOT (o)-[:RECEIVED_GRANT]->()
RETURN count(o) AS orphan_orgs;

// Check for orphan directors (no org)
MATCH (d:Director) WHERE NOT (d)-[:SITS_ON]->() RETURN count(d) AS orphan_dirs;

// Verify political era coverage
MATCH (o:Organization)-[g:RECEIVED_GRANT]->(m:Ministry)
RETURN g.political_era, count(g) AS grants, sum(g.amount) AS total
ORDER BY total DESC;
```

---

## Anti-Patterns

1. **CREATE instead of MERGE** — duplicates nodes, breaks queries (especially critical with 264 existing Aura nodes)
2. **Loading raw 1.8M grant rows as edges** — aggregate first (D003)
3. **Missing KGL properties** — every node needs `kgl` and `kgl_handle`
4. **Reading local CSV files instead of Databricks** — all data from Databricks (D009)
