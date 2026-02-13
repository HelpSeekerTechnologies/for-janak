# Operation Lineage Audit — Workplan

## Mission

Build a KGL v1.3-aligned Neo4j knowledge graph that unifies Alberta ministry lineage, grant flows, CRA charity data, director governance networks, and political donations to answer a **graph-only smoking gun question** for Government of Alberta political analysis.

## The Question

> "Which organizations saw the largest funding increases through NDP-created or NDP-restructured ministries (2015-2019), shared board directors with each other in governance clusters, and did those same directors appear as NDP donors — and what happened to that funding after UCP restructured the same ministries?"

### Why Graph-Only

This requires 5-hop heterogeneous traversal:
```
(:Director)-[:SITS_ON]->(:Organization)-[:RECEIVED_GRANT]->(:Grant)
  -[:FUNDED_BY]->(:Ministry)-[:TARGET_OF]->(:TransformEvent {era: 'NDP'})

AND (:Director)-[:DONATED_TO]->(:PoliticalParty {name: 'NDP'})

AND temporal delta: NDP-era funding vs UCP-era funding through successor ministries
```

No flat join can resolve ministry successor chains — that's the lineage graph. No tabular query can walk director→org→grant→ministry→event→era AND director→donation→party simultaneously.

---

## Phase 0: Data Assembly (4 Parallel Agents)

### Agent 0A: Grant-Ministry Political Era Linker
- **Status:** PENDING
- **Input:** `goa_grants_all.csv` (1.8M rows) + `entity_mapping.csv` (318 rows) + `transform_events.csv` (54 rows)
- **Logic:**
  1. Tag every grant row with `political_era`: NDP (2015-05 to 2019-04), UCP-Kenney (2019-04 to 2022-10), UCP-Smith (2022-10+)
  2. Tag with `ministry_restructured_by`: which political era's OIC transformed that ministry
  3. Compute per-organization: `ndp_era_total`, `ucp_era_total`, `delta_pct`
  4. Flag orgs with >50% funding increase during NDP era through NDP-restructured ministries
- **Output:** `01-data-assembly/grants_political_era.csv`
- **Skill:** `01-data-assembly/data-assembly.skill.md` (Grant Political Era Tagging section)

### Agent 0B: CRA Director Network Builder
- **Status:** PENDING
- **Input:** `directors_2023.csv` (571K rows) + `clusters.csv` + `master_watchlist.csv`
- **Logic:**
  1. Build director→organization bipartite graph
  2. Project to org→org shared-director edges (2+ shared = strong edge)
  3. Connected components → governance clusters
  4. Per cluster: aggregate GOA funding, risk flags, NDP-era recipients
  5. Export normalized director name list for donation matching
- **Output:** `01-data-assembly/director_org_network.csv`, `governance_clusters.csv`, `director_names_for_matching.csv`
- **Skill:** `01-data-assembly/data-assembly.skill.md` (Director Network section)

### Agent 0C: Elections Alberta Donation Collector
- **Status:** PENDING (depends on 0B for director names)
- **Input:** Director names from Agent 0B
- **Logic:**
  1. Query Elections Alberta contributor search for top multi-board directors
  2. Parse: contributor name, amount, recipient party, year
  3. Match director names to donation records
  4. Flag directors donating to NDP AND sitting on NDP-era funded org boards
- **Output:** `01-data-assembly/director_donations.csv`
- **Fallback:** Manual download step if scraping blocked
- **Skill:** `01-data-assembly/data-assembly.skill.md` (Political Donation Matching section)

### Agent 0D: Federal Grants Enrichment
- **Status:** PENDING
- **Input:** Federal G&C from open.canada.ca OR Databricks
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
- **Logic:** Generate unified KGL-aligned Neo4j DDL with 14 node types, 12 relationship types
- **Output:** `02-graph-build/neo4j/schema/unified-schema.cypher`
- **Skill:** `02-graph-build/graph-construction.skill.md`

### Agent 1B: Ingestion Pipeline
- **Status:** PENDING (depends on Phase 0 + 1A)
- **Input:** All Phase 0 outputs + existing ministry lineage data
- **Logic:**
  1. Ministry lineage (114 entities, 54 events, 36 sources)
  2. Organizations (9,145 AB charities from CRA)
  3. Directors (19K multi-board from CRA)
  4. SITS_ON, RECEIVED_GRANT, CLUSTER_MEMBER, FLAGGED_AS edges
  5. Political donations, federal grants
- **Output:** Populated Neo4j (~30K nodes, 500K+ relationships)
- **Skill:** `02-graph-build/graph-construction.skill.md`

---

## Phase 2: Smoking Gun Queries (3 Parallel Agents)

### Agent 2A: NDP Ministry Funding Tracer
- **Status:** PENDING
- **Query:** Organizations receiving most through NDP-restructured ministries + UCP-era delta
- **Output:** `03-smoking-gun-queries/ndp_ministry_funding_trace.csv`
- **Skill:** `03-smoking-gun-queries/graph-analysis.skill.md`

### Agent 2B: Director-to-Donation Graph Walker
- **Status:** PENDING
- **Query:** Directors who donated to NDP AND sit on boards of NDP-era funded orgs
- **Output:** `03-smoking-gun-queries/director_donation_grant_chain.csv`
- **Skill:** `03-smoking-gun-queries/graph-analysis.skill.md`

### Agent 2C: Governance Cluster Audit
- **Status:** PENDING
- **Query:** Clusters with highest NDP-era funding + risk flag concentration
- **Output:** `03-smoking-gun-queries/cluster_ndp_audit.csv`
- **Skill:** `03-smoking-gun-queries/graph-analysis.skill.md`

---

## Phase 3: Synthesis & Artifacts (3 Parallel Agents)

### Agent 3A: Business Analysis Synthesis
- **Status:** PENDING
- **Method:** "So What" chain (Observation → Pattern → Impact → Decision)
- **Output:** `04-synthesis/smoking-gun-synthesis.md`
- **Skill:** `04-synthesis/analysis-synthesis.skill.md`

### Agent 3B: HTML Dashboard Generator
- **Status:** PENDING
- **Artifacts:**
  1. `05-html-artifacts/00-process-overview.html` — pipeline animation
  2. `05-html-artifacts/01-ministry-lineage-political.html` — Sankey diagram
  3. `05-html-artifacts/02-director-donation-network.html` — D3 force graph
  4. `05-html-artifacts/03-cluster-funding-heatmap.html` — era heatmap
  5. `05-html-artifacts/04-smoking-gun-executive.html` — executive dashboard
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
│ Agent 0B │────┐   │       ┌──────────┐  │           ┌──────────┐  │          │ Agent 3B │            │ Agent 4B │
│ Directors│    ├───┤       │ Agent 1B │  │           │ Agent 2B │  ├─────────→│ HTML Gen │            │ Stress   │
└──────────┘    │   │       │ Ingest   │──┘──────────→│ Dir-Don  │  │          └──────────┘            └──────────┘
┌──────────┐    │   │       └──────────┘              └──────────┘  │          ┌──────────┐
│ Agent 0C │←───┘   │                                 ┌──────────┐  │          │ Agent 3C │
│ Elections│────────┤                                 │ Agent 2C │──┘          │ Evidence │
└──────────┘        │                                 │ Clusters │             └──────────┘
┌──────────┐        │                                 └──────────┘
│ Agent 0D │────────┘
│ Federal  │
└──────────┘

Total: 13 agent instances | Max concurrency: 4 | Estimated phases: 5
```

---

## Human-Validatable Artifacts (12 Deliverables)

| # | Artifact | Format | Validation Method |
|---|----------|--------|-------------------|
| 1 | `00-process-overview.html` | HTML | "Did the pipeline run these steps?" |
| 2 | `01-ministry-lineage-political.html` | HTML (Sankey) | "Do transformations match OICs?" |
| 3 | `02-director-donation-network.html` | HTML (D3) | "Are director-donation links real?" |
| 4 | `03-cluster-funding-heatmap.html` | HTML (Heatmap) | "Do clusters match known relationships?" |
| 5 | `04-smoking-gun-executive.html` | HTML (Dashboard) | "Are headline numbers correct?" |
| 6 | `evidence-traceability.csv` | CSV | "Every claim has a source — spot-check any row" |
| 7 | `evidence-traceability.html` | HTML (filterable) | Interactive version of above |
| 8 | `smoking-gun-synthesis.md` | Markdown | "Does the So What chain hold?" |
| 9 | `counter-arguments.md` | Markdown | "Are rebuttals addressed?" |
| 10 | `unified-schema.cypher` | Cypher DDL | "Is schema KGL-compliant?" |
| 11 | `grants_political_era.csv` | CSV | "Spot-check era tags vs OIC dates" |
| 12 | Neo4j browser (localhost:7474) | Live graph | "Run any query, verify any claim" |
