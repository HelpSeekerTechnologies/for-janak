# Operation Lineage Audit — Workplan

## Mission

Build a KGL v1.3-aligned Neo4j knowledge graph that unifies Alberta ministry lineage, grant flows, CRA charity data, and director governance networks to answer a **graph-only governance question** for Government of Alberta political analysis.

## The Question

> "Which governance clusters (organizations sharing board directors) received disproportionate funding increases through NDP-restructured ministries compared to non-clustered organizations — and did that concentration pattern reverse under UCP?"

### Why Graph-Only

This requires multi-hop heterogeneous traversal:
```
(:Director)-[:SITS_ON]->(:Organization)-[:CLUSTER_MEMBER]->(:Organization)
  -[:RECEIVED_GRANT]->(:Ministry)-[:TARGET_OF]->(:TransformEvent {era: 'NDP'})

AND temporal delta: NDP-era funding vs UCP-era funding through successor ministries
AND clustered vs non-clustered funding distribution comparison
```

No flat join can resolve ministry successor chains — that's the lineage graph. No tabular query can simultaneously walk cluster membership + grant flows + ministry lineage + temporal era comparison.

---

## Phase 0: Data Assembly (3 Parallel Agents)

### Agent 0A: Grant-Ministry Political Era Linker
- **Status:** PENDING
- **Input:** Databricks `goa_grants_disclosure` (1.8M rows) + Volume `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/` (entity_mapping, transform_events)
- **Logic:**
  1. Tag every grant row with `political_era`: NDP (2015-05 to 2019-04), UCP-Kenney (2019-04 to 2022-10), UCP-Smith (2022-10+)
  2. Tag with `ministry_restructured_by`: which political era's OIC transformed that ministry
  3. Compute per-organization: `ndp_era_total`, `ucp_era_total`, `delta_pct`
  4. Flag orgs with >50% funding increase during NDP era through NDP-restructured ministries
- **Output:** `01-data-assembly/grants_political_era.csv`
- **Skill:** `01-data-assembly/data-assembly.skill.md` (Grant Political Era Tagging section)

### Agent 0B: CRA Director Network Builder
- **Status:** PENDING
- **Input:** Databricks tables `cra_directors_clean` (570K rows) + `multi_board_directors` (19K) + `org_clusters_strong` (4,636) + `ab_org_risk_flags` (9,145)
- **Logic:**
  1. Build director→organization bipartite graph
  2. Project to org→org shared-director edges (2+ shared = strong edge)
  3. Connected components → governance clusters
  4. Per cluster: aggregate GOA funding, risk flags, NDP-era recipients
- **Output:** `01-data-assembly/director_org_network.csv`, `governance_clusters.csv`
- **Skill:** `01-data-assembly/data-assembly.skill.md` (Director Network section)

### Agent 0C: Federal Grants Enrichment
- **Status:** PENDING
- **Input:** Databricks Volume `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/`
- **Logic:**
  1. Pull Federal G&C filtered to Province=AB
  2. Match on BN to CRA T3010
  3. Compute per-org federal funding totals
  4. Cross-reference with risk flags
- **Output:** `01-data-assembly/federal_grants_ab.csv`, `dual_funded_orgs.csv`
- **Skill:** `01-data-assembly/data-assembly.skill.md` (Federal Grants section)

---

## Phase 1: Neo4j Graph Construction (2 Agents, Sequential)

### Agent 1A: Schema Architect
- **Status:** PENDING
- **Input:** KGL skill definitions (kgl-core, neo4j-graph)
- **Logic:** Generate unified KGL-aligned Neo4j DDL with 12 node types, 10 relationship types
- **Output:** `02-graph-build/neo4j/schema/unified-schema.cypher`
- **Skill:** `02-graph-build/graph-construction.skill.md`

### Agent 1B: Ingestion Pipeline (Extend Existing Aura Graph)
- **Status:** PENDING (depends on Phase 0 + 1A)
- **Input:** All Phase 0 outputs (264 ministry lineage nodes already in Aura — D011)
- **Logic:**
  1. MERGE ministry lineage (idempotent — 264 nodes already exist from Archana's notebook)
  2. Organizations (9,145 AB charities from CRA)
  3. Directors (19K multi-board from CRA)
  4. SITS_ON, RECEIVED_GRANT, CLUSTER_MEMBER, FLAGGED_AS edges
  5. Federal grants
- **Output:** Extended Neo4j Aura (~30K nodes, 500K+ relationships)
- **Skill:** `02-graph-build/graph-construction.skill.md`

---

## Phase 2: Governance Analysis Queries (3 Parallel Agents)

### Agent 2A: NDP Ministry Funding Tracer
- **Status:** PENDING
- **Query:** Organizations receiving most through NDP-restructured ministries + UCP-era delta
- **Output:** `03-governance-queries/ndp_ministry_funding_trace.csv`
- **Skill:** `03-governance-queries/graph-analysis.skill.md`

### Agent 2B: Director-Cluster-Funding-Concentration Analyzer
- **Status:** PENDING
- **Query:** Governance clusters (orgs sharing directors) that received disproportionate per-org funding through NDP-restructured ministries vs non-clustered orgs
- **Output:** `03-governance-queries/cluster_funding_concentration.csv`
- **Skill:** `03-governance-queries/graph-analysis.skill.md`

### Agent 2C: Governance Cluster Audit
- **Status:** PENDING
- **Query:** Clusters with highest NDP-era funding + risk flag concentration
- **Output:** `03-governance-queries/cluster_ndp_audit.csv`
- **Skill:** `03-governance-queries/graph-analysis.skill.md`

---

## Phase 3: Synthesis & Artifacts (3 Parallel Agents)

### Agent 3A: Business Analysis Synthesis
- **Status:** PENDING
- **Method:** "So What" chain (Observation → Pattern → Impact → Decision)
- **Output:** `04-synthesis/governance-synthesis.md`
- **Skill:** `04-synthesis/analysis-synthesis.skill.md`

### Agent 3B: HTML Dashboard Generator
- **Status:** PENDING
- **Artifacts:**
  1. `05-html-artifacts/00-process-overview.html` — pipeline animation
  2. `05-html-artifacts/01-ministry-lineage-political.html` — Sankey diagram
  3. `05-html-artifacts/02-director-cluster-network.html` — D3 force graph (governance clusters)
  4. `05-html-artifacts/03-cluster-funding-heatmap.html` — era heatmap
  5. `05-html-artifacts/04-governance-executive.html` — executive dashboard
- **Skill:** `05-html-artifacts/visualization.skill.md`

### Agent 3C: Evidence Traceability Matrix
- **Status:** PENDING
- **Output:** `06-validation/evidence-traceability.csv` + `.html`
- **Skill:** `06-validation/validation.skill.md`

---

## Phase 4: Validation (2 Parallel Agents)

### Agent 4A: Fact-Check Agent
- **Status:** PENDING
- **Method:** Fetch live OICs, cross-reference grants, spot-check 10 random claims
- **Output:** `06-validation/fact-check-report.md`

### Agent 4B: Counter-Argument Stress Test
- **Status:** PENDING
- **Method:** Symmetry test (UCP-era same analysis), statistical significance, pre-briefed rebuttals
- **Output:** `06-validation/counter-arguments.md`

---

## Agent Execution Map

```
PHASE 0 (Parallel)           PHASE 1 (Sequential)      PHASE 2 (Parallel)       PHASE 3 (Parallel)      PHASE 4 (Parallel)
┌──────────┐                ┌──────────┐              ┌──────────┐             ┌──────────┐            ┌──────────┐
│ Agent 0A │────────┐       │ Agent 1A │              │ Agent 2A │             │ Agent 3A │            │ Agent 4A │
│ Grant-Era│        │       │ Schema   │              │ NDP Fund │             │ Synthesis│            │ FactCheck│
└──────────┘        ├──────→│ Architect│──┐           │ Tracer   │──┐          └──────────┘            └──────────┘
┌──────────┐        │       └──────────┘  │           └──────────┘  │          ┌──────────┐            ┌──────────┐
│ Agent 0B │────────┤       ┌──────────┐  │           ┌──────────┐  │          │ Agent 3B │            │ Agent 4B │
│ Directors│        │       │ Agent 1B │  │           │ Agent 2B │  ├─────────→│ HTML Gen │            │ Stress   │
└──────────┘        │       │ Ingest   │──┘──────────→│Clust-Fund│  │          └──────────┘            └──────────┘
┌──────────┐        │       │ (Aura)   │              └──────────┘  │          ┌──────────┐
│ Agent 0C │────────┘       └──────────┘              ┌──────────┐  │          │ Agent 3C │
│ Federal  │                                          │ Agent 2C │──┘          │ Evidence │
└──────────┘                                          │ Clusters │             └──────────┘
                                                      └──────────┘

Total: 12 agent instances | Max concurrency: 3 | Estimated phases: 5
```

---

## Human-Validatable Artifacts (12 Deliverables)

| # | Artifact | Format | Validation Method |
|---|----------|--------|-------------------|
| 1 | `00-process-overview.html` | HTML | "Did the pipeline run these steps?" |
| 2 | `01-ministry-lineage-political.html` | HTML (Sankey) | "Do transformations match OICs?" |
| 3 | `02-director-cluster-network.html` | HTML (D3) | "Are cluster-director links real?" |
| 4 | `03-cluster-funding-heatmap.html` | HTML (Heatmap) | "Do clusters match known relationships?" |
| 5 | `04-governance-executive.html` | HTML (Dashboard) | "Are headline numbers correct?" |
| 6 | `evidence-traceability.csv` | CSV | "Every claim has a source — spot-check any row" |
| 7 | `evidence-traceability.html` | HTML (filterable) | Interactive version of above |
| 8 | `governance-synthesis.md` | Markdown | "Does the So What chain hold?" |
| 9 | `counter-arguments.md` | Markdown | "Are rebuttals addressed?" |
| 10 | `unified-schema.cypher` | Cypher DDL | "Is schema KGL-compliant?" |
| 11 | `grants_political_era.csv` | CSV | "Spot-check era tags vs OIC dates" |
| 12 | Neo4j Aura (`<YOUR_NEO4J_AURA_URI>`) | Live graph | "Run any query, verify any claim" |
