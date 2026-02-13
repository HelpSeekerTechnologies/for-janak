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
**Rationale:** The smoking gun question operates at org×ministry×era granularity, not individual payment level. Aggregation reduces edges from ~1.8M to ~50-100K while preserving all analytical power.
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
**Context:** Claude Code sessions compress context over time. Need to preserve methodology in files that can be re-read.
**Decision:** Every folder gets a `{domain}.skill.md` that encodes the methodology, inputs, outputs, and anti-patterns for that phase. These serve as both documentation AND Claude Code skill files that can be loaded in future sessions.
**Rationale:** After context compression, Claude can `Read` the relevant skill.md to recover full context for that phase. This is the KGL-skill pattern applied to the project itself.
**Alternatives:** (a) Single monolithic CLAUDE.md — too large after expansion. (b) No skills — lose context on compression.
