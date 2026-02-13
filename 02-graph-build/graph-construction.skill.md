# Graph Construction Skill

Design, deploy, and populate a unified KGL v1.3-aligned Neo4j knowledge graph combining ministry lineage, charity organizations, director networks, grant flows, risk flags, and political donations.

---

## When to Use

- Generating the unified Neo4j schema (DDL)
- Writing or running the ingestion pipeline
- Validating graph integrity post-ingestion
- Extending the schema with new node/relationship types

---

## Neo4j Instance

```bash
docker run -d --name lineage-audit \
  -p 7474:7474 -p 7687:7687 \
  -v lineage-audit-data:/data \
  -e NEO4J_AUTH=neo4j/<YOUR_NEO4J_LOCAL_PASSWORD> \
  neo4j:5-community
```

- **Browser:** http://localhost:7474
- **Bolt:** bolt://localhost:7687
- **Auth:** neo4j / <YOUR_NEO4J_LOCAL_PASSWORD>

---

## Schema: 14 KGL Node Types

```cypher
// ━━━ MINISTRY LINEAGE (from ministry-genealogy-graph) ━━━

// ▣ program — Government ministry/department (can transform)
CREATE CONSTRAINT ministry_id IF NOT EXISTS
FOR (m:Ministry) REQUIRE m.canonical_id IS UNIQUE;

// ⧫ event — Transformation event (SPLIT, MERGE, RENAME, etc.)
CREATE CONSTRAINT event_id IF NOT EXISTS
FOR (e:TransformEvent) REQUIRE e.event_id IS UNIQUE;

// ⟦ record — Source document (OIC, grants CSV, mandate letter)
CREATE CONSTRAINT source_doc_id IF NOT EXISTS
FOR (d:SourceDocument) REQUIRE d.doc_id IS UNIQUE;

// ━━━ CHARITY ORGANIZATIONS (from CRA T3010) ━━━

// ᚴ organization — Registered charity
CREATE CONSTRAINT org_bn IF NOT EXISTS
FOR (o:Organization) REQUIRE o.bn IS UNIQUE;

// ◎ person — Board director
CREATE CONSTRAINT director_id IF NOT EXISTS
FOR (d:Director) REQUIRE d.normalized_name IS UNIQUE;

// ━━━ FUNDING (from GOA grants, federal G&C) ━━━

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

// ✠ authority — Political party
CREATE CONSTRAINT party_id IF NOT EXISTS
FOR (p:PoliticalParty) REQUIRE p.name IS UNIQUE;

// Indexes for query performance
CREATE INDEX ministry_name IF NOT EXISTS FOR (m:Ministry) ON (m.name);
CREATE INDEX org_name IF NOT EXISTS FOR (o:Organization) ON (o.name);
CREATE INDEX director_name IF NOT EXISTS FOR (d:Director) ON (d.normalized_name);
CREATE INDEX grant_era IF NOT EXISTS FOR ()-[g:RECEIVED_GRANT]-() ON (g.political_era);
```

## 12 Relationship Types

```cypher
// Ministry lineage (existing)
// (:Ministry)-[:SOURCE_OF]->(:TransformEvent)     — predecessor
// (:TransformEvent)-[:TARGET_OF]->(:Ministry)     — successor
// (:TransformEvent)-[:EVIDENCED_BY]->(:SourceDocument)
// (:Ministry)-[:PARENT_OF]->(:Ministry)           — temporal hierarchy

// Grant flows (new)
// (:Organization)-[:RECEIVED_GRANT {amount, fiscal_year, political_era, n_payments}]->(:Ministry)

// Director governance (new)
// (:Director)-[:SITS_ON {position, start_date, end_date}]->(:Organization)

// Risk classification (new)
// (:Organization)-[:FLAGGED_AS {severity}]->(:RiskFlag)

// Geography (new)
// (:Organization)-[:LOCATED_IN]->(:Region)

// Political donations (new)
// (:Director)-[:DONATED_TO {amount, year}]->(:PoliticalParty)

// Cluster edges (new)
// (:Organization)-[:CLUSTER_MEMBER {cluster_id, shared_directors}]->(:Organization)

// Federal funding (new)
// (:Organization)-[:FUNDED_BY_FED {amount, department, program}]->(:FiscalYear)

// Era tagging (new)
// (:Ministry)-[:OPERATES_IN_ERA]->(:FiscalYear)
```

---

## Ingestion Order (Critical)

**Always MERGE, never CREATE.** Entities appear in multiple datasets.

1. **FiscalYear nodes** (11) — independent
2. **Region nodes** (~7) — independent
3. **RiskFlag nodes** (11 types) — independent
4. **PoliticalParty nodes** (4-5) — independent
5. **Ministry lineage** — run existing `ingest-all.cypher` from ministry-genealogy-graph (114+54+36 nodes, 263 edges)
6. **Organization nodes** (9,145) — from CRA T3010
7. **Director nodes** (~19K multi-board) — from CRA directors
8. **SITS_ON edges** — director → organization
9. **RECEIVED_GRANT edges** — organization → ministry (from aggregated grants)
10. **FLAGGED_AS edges** — organization → risk flag
11. **CLUSTER_MEMBER edges** — organization → organization
12. **DONATED_TO edges** — director → political party (if available)
13. **FUNDED_BY_FED edges** — organization → fiscal year

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

1. **CREATE instead of MERGE** — duplicates nodes, breaks queries
2. **Loading raw 1.8M grant rows as edges** — aggregate first (D003)
3. **Missing KGL properties** — every node needs `kgl` and `kgl_handle`
4. **Mixing this graph with ministry-genealogy-graph Neo4j** — separate containers (D007)
