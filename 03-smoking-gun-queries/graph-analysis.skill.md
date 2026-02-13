# Graph Analysis Skill

Write and execute multi-hop Cypher queries against the unified Neo4j graph to answer the smoking gun question. Each query isolates one dimension; the synthesis phase combines them.

---

## When to Use

- Writing Cypher queries that traverse ministry lineage + grants + directors + donations
- Answering questions that require graph traversal (not possible with flat tables)
- Extracting evidence chains for the traceability matrix

---

## The Three Core Queries

### Query 1: NDP Ministry Funding Trace

**Question:** Which organizations received the most funding through ministries that the NDP created or restructured, and how did that funding change under UCP?

**Why graph-only:** Requires traversing TransformEvent nodes to determine which ministries were NDP-restructured, then following RECEIVED_GRANT edges to those ministries, then computing temporal deltas across eras. A flat table doesn't know "Ministry X is a successor of Ministry Y which was created by NDP OIC."

```cypher
// Step 1: Find NDP-era transformation events
MATCH (evt:TransformEvent)
WHERE evt.event_date >= date('2015-05-24') AND evt.event_date <= date('2019-04-29')
WITH collect(evt.event_id) AS ndp_event_ids

// Step 2: Find ministries that were TARGETS of NDP restructuring
UNWIND ndp_event_ids AS eid
MATCH (evt:TransformEvent {event_id: eid})-[:TARGET_OF]->(m:Ministry)
WITH collect(DISTINCT m.canonical_id) AS ndp_ministry_ids

// Step 3: Find organizations receiving grants through those ministries
UNWIND ndp_ministry_ids AS mid
MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:Ministry {canonical_id: mid})
WITH org, m,
     sum(CASE WHEN g.political_era = 'NDP' THEN g.amount ELSE 0 END) AS ndp_funding,
     sum(CASE WHEN g.political_era IN ['UCP_Kenney', 'UCP_Smith'] THEN g.amount ELSE 0 END) AS ucp_funding

// Step 4: Enrich with risk flags
OPTIONAL MATCH (org)-[:FLAGGED_AS]->(flag:RiskFlag)
WITH org, collect(DISTINCT m.name) AS ndp_ministries,
     sum(ndp_funding) AS total_ndp, sum(ucp_funding) AS total_ucp,
     collect(DISTINCT flag.flag_type) AS risk_flags

WHERE total_ndp > 100000  // $100K threshold
RETURN org.name, org.bn, org.city,
       total_ndp, total_ucp,
       CASE WHEN total_ndp > 0 THEN round((total_ucp - total_ndp) * 100.0 / total_ndp) ELSE null END AS delta_pct,
       ndp_ministries, risk_flags
ORDER BY total_ndp DESC
LIMIT 100
```

**Output:** `ndp_ministry_funding_trace.csv`

---

### Query 2: Director-to-Donation Graph Walk

**Question:** Which directors sit on boards of NDP-era funded organizations AND donated to the NDP?

**Why graph-only:** Requires walking Director→Organization→Grant→Ministry→TransformEvent AND Director→Donation→Party simultaneously. No single table has all these joins.

```cypher
// Directors who donated to NDP
MATCH (d:Director)-[don:DONATED_TO]->(party:PoliticalParty {name: 'NDP'})

// Same directors on org boards
MATCH (d)-[:SITS_ON]->(org:Organization)

// Those orgs received grants from NDP-era ministries
MATCH (org)-[g:RECEIVED_GRANT {political_era: 'NDP'}]->(m:Ministry)

// The ministry was created/restructured during NDP era
MATCH (m)<-[:TARGET_OF]-(evt:TransformEvent)
WHERE evt.event_date >= date('2015-05-24') AND evt.event_date <= date('2019-04-29')

RETURN d.normalized_name AS director,
       sum(don.amount) AS total_donated_ndp,
       collect(DISTINCT org.name) AS organizations,
       sum(g.amount) AS total_org_grants_ndp_era,
       collect(DISTINCT m.name) AS ndp_ministries,
       count(DISTINCT org) AS n_orgs
ORDER BY total_org_grants_ndp_era DESC
```

**Output:** `director_donation_grant_chain.csv`

---

### Query 3: Governance Cluster NDP Audit

**Question:** Which governance clusters (groups of orgs sharing directors) collectively received the most through NDP-era ministries, and how many carry risk flags?

**Why graph-only:** Requires cluster traversal (CLUSTER_MEMBER edges) aggregated with grant traversal (RECEIVED_GRANT) and risk traversal (FLAGGED_AS). Three independent relationship types converging on Organization nodes.

```cypher
// Get all cluster relationships
MATCH (org1:Organization)-[c:CLUSTER_MEMBER]-(org2:Organization)
WITH c.cluster_id AS cluster, collect(DISTINCT org1) + collect(DISTINCT org2) AS all_orgs

// Deduplicate orgs per cluster
UNWIND all_orgs AS org
WITH cluster, collect(DISTINCT org) AS orgs

// Per-org: NDP grants + flags
UNWIND orgs AS org
OPTIONAL MATCH (org)-[g:RECEIVED_GRANT {political_era: 'NDP'}]->(m:Ministry)
OPTIONAL MATCH (org)-[:FLAGGED_AS]->(flag:RiskFlag)

WITH cluster,
     count(DISTINCT org) AS cluster_size,
     sum(g.amount) AS cluster_ndp_funding,
     count(DISTINCT flag) AS total_flags,
     collect(DISTINCT org.name)[..5] AS sample_orgs

WHERE cluster_ndp_funding > 1000000
RETURN cluster, cluster_size, cluster_ndp_funding, total_flags, sample_orgs
ORDER BY cluster_ndp_funding DESC
LIMIT 25
```

**Output:** `cluster_ndp_audit.csv`

---

## Symmetry Test (UCP Era — for Counter-Arguments)

Run identical queries with UCP date range:
```cypher
// Replace NDP dates with UCP dates
WHERE evt.event_date >= date('2019-04-30')
// Replace political_era filter
{political_era: 'UCP_Kenney'} or {political_era: 'UCP_Smith'}
```

Compare results side by side. If UCP shows same patterns, the finding is systemic (still valuable, just different narrative).

---

## Evidence Chain Format

Every query result row should be traceable:
```
claim: "Organization X received $Y through Ministry Z (NDP-restructured)"
source_1: "Ministry Z created by O.C. {number}/{year}" → SourceDocument node
source_2: "Grant of $Y in FY{year}" → GOA grants CSV
source_3: "Director A sits on Organization X board" → CRA T3010 directors
source_4: "Director A donated ${amount} to NDP in {year}" → Elections Alberta
```

---

## Anti-Patterns

1. **Don't interpret correlation as causation** — "funded through NDP-restructured ministry" ≠ "funded because of NDP"
2. **Don't cherry-pick top results without context** — always report total population size
3. **Don't present donation links without confidence levels** — exact name match ≠ confirmed same person
4. **Don't skip the symmetry test** — one-sided analysis is politically vulnerable (D005)
