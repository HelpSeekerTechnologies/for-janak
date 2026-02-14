# Operation Lineage Audit — Decisions Log

## Decision Format

Each decision follows: **Context → Decision → Rationale → Alternatives Considered**

---

### D001: Use KGL `▣` program for ministries, `ᚴ` organization for charities

**Date:** 2026-02-12
**Context:** The ministry-genealogy-graph uses `▣` program for all ministry entities (per KGL v1.3 design decision — programs can split/merge/rename). This project adds CRA-registered charities as a second entity type.
**Decision:** Ministries remain `▣` program. Charities are `ᚴ` organization. This creates the dual-ontology pattern: programs (government mandate units) fund organizations (charitable entities).
**Rationale:** KGL v1.3 distinguishes `▣` (mandate-based, can transform) from `ᚴ` (persistent institutional identity). Government ministries transform via OICs; charities have persistent BN numbers and legal identity. The grant relationship `(:Organization)-[:RECEIVED_GRANT]->(:Ministry)` correctly models the funding flow direction.
**Alternatives:** (a) Use `▣` for both — rejected because charities don't split/merge via OICs. (b) Use `ᚴ` for both — rejected because ministries are not persistent identities (they dissolve, rename, split).

---

### D002: Political era boundaries based on premier swearing-in dates

**Date:** 2026-02-12
**Context:** Need to classify grants and events into political eras for comparative analysis.
**Decision:** Use premier swearing-in dates, not election dates:
- NDP: 2015-05-24 (Notley sworn in) to 2019-04-30 (Kenney sworn in)
- UCP-Kenney: 2019-04-30 to 2022-10-11 (Smith sworn in)
- UCP-Smith: 2022-10-11 to present
**Rationale:** Grants are authorized by sitting government. The day a new premier is sworn in is when machinery-of-government changes can legally occur. OICs establishing departments are signed on or after swearing-in.
**Alternatives:** (a) Election day boundaries — rejected because lame-duck period still has active government. (b) First OIC date per era — too fine-grained and varies.

---

### D003: Aggregate grants at Organization × Ministry × FiscalYear level for graph edges

**Date:** 2026-02-12
**Context:** Raw grants data has 1.8M rows (individual payments). Loading all as graph edges would create an unwieldy graph.
**Decision:** Aggregate to `(Organization, Ministry, FiscalYear)` tuples. Each tuple becomes one `RECEIVED_GRANT` relationship with `amount` (sum), `n_payments` (count), `political_era` (derived).
**Rationale:** The core governance question operates at org×ministry×era granularity, not individual payment level. Aggregation reduces edges from ~1.8M to ~50-100K while preserving all analytical power.
**Alternatives:** (a) Individual payment edges — rejected for performance. (b) Aggregate at org×era only (lose ministry dimension) — rejected because ministry lineage traversal requires the ministry node.

---

### D004: Director name matching uses exact normalized match, not fuzzy

**Date:** 2026-02-12
**Context:** Matching CRA director names to Elections Alberta donor names.
**Decision:** Start with exact match on `UPPER(TRIM(LastName)), UPPER(TRIM(FirstName))`. Report match rate. Escalate to fuzzy only if match rate < 10%.
**Rationale:** Conservative matching avoids false positives that would undermine credibility. A false "Director X donated to NDP" claim is worse than missing a real match. Can always expand later.
**Alternatives:** (a) Fuzzy matching from start — rejected for credibility risk. (b) BN-based matching — not possible (Elections Alberta tracks individuals, not organizations).

---

### D005: Include symmetry test (same analysis for UCP era) as fairness control

**Date:** 2026-02-12
**Context:** Running NDP-era-only analysis could be accused of cherry-picking.
**Decision:** Run the identical analysis for UCP-era restructured ministries as Agent 4B counter-argument stress test. Report both results side by side.
**Rationale:** If the pattern is unique to NDP era, the contrast strengthens the finding. If it also appears in UCP era, that's an equally valid (and more defensible) systemic finding: "ministry restructuring correlates with funding pattern changes regardless of party."
**Alternatives:** (a) NDP-only — rejected as politically vulnerable. (b) All-eras combined — loses the comparative power.

---

### D006: Elections Alberta data is the weakest link — plan for human validation fallback

**Date:** 2026-02-12
**Context:** Elections Alberta has no bulk CSV download. Web search interface may block automated queries.
**Decision:** Attempt automated collection first. If blocked, produce a `director_names_for_matching.csv` with instructions for manual lookup. Flag all donation-dependent claims as `confidence: MEDIUM (pending human verification)`.
**Rationale:** The ministry lineage + grant flow + director network analysis is already powerful without donations. Donations are the "bonus" layer. Don't let the perfect be the enemy of the good.
**Alternatives:** (a) Skip donations entirely — rejected because it's the political angle. (b) Use federal Elections Canada data instead — viable fallback (they have bulk CSV), but provincial donations are more relevant.

---

### D007: Neo4j Docker container with persistent volume

**Date:** 2026-02-12
**Context:** Need a Neo4j instance for the unified graph.
**Decision:** `docker run -d --name lineage-audit -p 7474:7474 -p 7687:7687 -v lineage-audit-data:/data -e NEO4J_AUTH=neo4j/<YOUR_NEO4J_LOCAL_PASSWORD> neo4j:5-community`
**Rationale:** Named volume ensures data persists across container restarts. Separate container name from ministry-genealogy-graph to avoid conflicts.
**Alternatives:** (a) Reuse ministry-genealogy-graph Neo4j — rejected to avoid contaminating the validated graph. (b) Neo4j Aura (cloud) — unnecessary cost for local analysis.

---

### D008: Each folder gets a skill.md for session persistence

**Date:** 2026-02-12
**Context:** Long-running analysis sessions lose context over time. Need to preserve methodology in files that can be re-read.
**Decision:** Every folder gets a `{domain}.skill.md` that encodes the methodology, inputs, outputs, and anti-patterns for that phase. These serve as both documentation AND reusable methodology files.
**Rationale:** On session recovery, the relevant skill.md provides full context for that phase. This is the KGL-skill pattern applied to the project itself.
**Alternatives:** (a) Single monolithic CLAUDE.md — too large after expansion. (b) No skills — lose context on restart.

---

### D009: Databricks as source of truth (not local files)

**Date:** 2026-02-12
**Context:** The project initially referenced local CSV files on the developer's desktop (e.g., `goa_grants_all.csv`, `directors_2023.csv`). However, all data has been uploaded, cleaned, and deduplicated in the Databricks workspace (`<YOUR_DATABRICKS_HOST>`, Unity Catalog `dbw_unitycatalog_test`). Local files may be stale or inconsistent across team members.
**Decision:** All data sourcing must come from Databricks — either from Unity Catalog tables (e.g., `goa_grants_disclosure`, `cra_directors_clean`, `multi_board_directors`) or from Volume files (e.g., `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/...`). No local CSV files should be referenced.
**Rationale:** Databricks has validated/deduplicated tables that are the single source of truth. Local files may be stale, and different team members may have different versions. Using Databricks ensures reproducibility and consistency.
**Alternatives:** (a) Continue using local files — rejected because of staleness and team inconsistency risk. (b) Hybrid local+Databricks — rejected for simplicity; one source of truth is better than two.

---

### D010: Drop Elections Alberta, reframe without political donations

**Date:** 2026-02-12
**Context:** The original workplan included Agent 0C (Elections Alberta Donation Collector) to scrape political donation data and link directors to NDP donations. However, no Elections Alberta data is available (no bulk CSV download, scraping unreliable), and the core governance question depended on director-to-donation links.
**Decision:** Remove Agent 0C entirely. Remove all `PoliticalParty` nodes, `Donation` nodes, and `DONATED_TO` relationships from the graph schema. Reframe the core governance question from "directors who donated to NDP" to "governance clusters receiving disproportionate funding through NDP-restructured ministries vs non-clustered organizations." Reframe Agent 2B from "Director-to-Donation Graph Walker" to "Director-Cluster-Funding-Concentration Analyzer."
**Rationale:** The funding pattern through ministry lineage + governance clusters is already powerful without donations. The reframed question is STILL graph-only (requires ministry lineage traversal + director network cluster detection + temporal funding comparison) and avoids the weakest data link. Per D006, donations were always the weakest link.
**Alternatives:** (a) Keep Agent 0C and manually collect donations — rejected because data is unavailable. (b) Use federal Elections Canada data instead — rejected because provincial donations are more relevant and the reframed question is stronger without donations.

---

### D011: Extend existing Neo4j Aura instead of Docker

**Date:** 2026-02-12
**Context:** The original plan (D007) used a local Docker Neo4j container. However, Archana's ingest notebook has already loaded 264 nodes and 317 relationships (ministry lineage) into a Neo4j Aura instance (`<YOUR_NEO4J_AURA_URI>`).
**Decision:** Use the existing Neo4j Aura instance instead of spinning up a new Docker container. MERGE operations will be idempotent — the 264 existing nodes will be matched, not duplicated. All new nodes (organizations, directors, grants) will extend the existing graph.
**Rationale:** Avoids duplicate work (ministry lineage already loaded). Aura is accessible to the whole team (not just the local developer). Cloud-hosted instance is more reliable for demo/presentation purposes.
**Alternatives:** (a) Fresh Docker container — rejected because it duplicates Archana's work and is local-only. (b) Wipe Aura and reload — rejected because existing data is valid and MERGE handles idempotency.
