# Operation Lineage Audit — Claude Code Context

## Project Overview

Graph-based political-era analysis of Alberta ministry restructuring, grant flows, director governance networks, and political donations. Builds a unified Neo4j knowledge graph aligned to KGL v1.3 ontology to answer questions that **only a graph can answer** — specifically tracing funding through ministry lineage chains across political eras (NDP 2015-2019, UCP 2019-present).

## The Smoking Gun Question

"Which organizations saw the largest funding increases through NDP-created or NDP-restructured ministries (2015-2019), shared board directors with each other in governance clusters, and did those same directors appear as NDP donors — and what happened to that funding after UCP restructured the same ministries?"

This requires 5-hop graph traversal: Director → Organization → Grant → Ministry → TransformEvent → PoliticalEra — impossible with flat tables.

## Key References

- **Ministry Genealogy Graph:** `C:\Users\alina\OneDrive\Desktop\ministry-genealogy-graph\` (114 entities, 54 events, 1.8M harmonized grants)
- **Janak Demo:** `C:\Users\alina\OneDrive\Desktop\Janak Demo\` (CRA analysis, director networks, risk flags, 14 HTML dashboards)
- **KGL Skills Repo:** https://github.com/HelpSeekerTechnologies/kgl-skill (private)
- **KGL Ontology:** https://github.com/HelpSeekerTechnologies/kgl-ontology
- **This Repo:** https://github.com/HelpSeekerTechnologies/lineage-audit

## Architecture

- **Graph database:** Neo4j 5.x (Docker: `neo4j:5-community`, bolt://localhost:7687)
- **Data warehouse:** Databricks (`<YOUR_DATABRICKS_HOST>`)
- **Databricks SQL Warehouse:** `<YOUR_DATABRICKS_SQL_WAREHOUSE>`
- **Unity Catalog:** `dbw_unitycatalog_test`

## Data Sources

| Source | Location | Records | Key Fields |
|--------|----------|---------|------------|
| GOA Grants (all years) | `C:\Users\alina\OneDrive\Desktop\goa_grants_all.csv` | 1,806,202 | Ministry, Recipient, Program, Amount, FiscalYear |
| GOA Grants (per-year) | `C:\Users\alina\OneDrive\Desktop\goa_grants\goa_grants_YYYY-YY.csv` | ~130-180K each | Same |
| Ministry Lineage | `ministry-genealogy-graph/04-graph-build/databricks/` | 114 entities, 54 events | canonical_id, event_type, event_date |
| Entity Mapping | `ministry-genealogy-graph/04-graph-build/databricks/entity_mapping.csv` | 318 rows | grants_ministry_name → canonical_id → current_name |
| CRA Directors 2023 | `Janak Demo/Excploratory Analysis/data/cra/directors_2023.csv` | 571,461 | BN, Last Name, First Name, Position |
| Super Directors | `Janak Demo/Excploratory Analysis/data/super_directors.csv` | 50 | name, n_boards (up to 357) |
| Master Watchlist | `Janak Demo/.../master_watchlist.csv` | 321 | BN, Organization, 11 risk flag columns |
| Director Clusters | `Janak Demo/Excploratory Analysis/data/clusters.csv` | 380+ | cluster_id, size, org_names |
| CRA T3010 (Databricks) | `postgresql_catalog.cra_data.cra_2023` | ~83K nationally | BN, revenue, expenses, directors |
| CRA 2024 (Databricks) | `dbw_unitycatalog_test/uploads/uploaded_files/CRA - Oct 2025 update/` | ~71K | Updated financials |
| Federal G&C | `open.canada.ca` | ~70K AB | BN, amount, department |
| Elections Alberta | `efpublic.elections.ab.ca` | TBD | contributor, party, amount, year |

## Neo4j Graph Schema (KGL-Aligned)

### Node Types (14 KGL canonical nodes)
| Glyph | Handle | Neo4j Label | Source |
|-------|--------|-------------|--------|
| `▣` | program | `:Ministry` | ministry-genealogy-graph |
| `⧫` | event | `:TransformEvent` | ministry-genealogy-graph |
| `⟦` | record | `:SourceDocument` | OICs, grants CSVs |
| `◉` | resource | `:Grant` | GOA grants (aggregated) |
| `ᚴ` | organization | `:Organization` | CRA T3010 |
| `◎` | person | `:Director` | CRA directors |
| `⟲` | timeframe | `:FiscalYear` | 11 fiscal years |
| `ᚪ` | geography | `:Region` | AB regions |
| `⟡` | measurement | `:RiskFlag` | 11 flag types |
| `Ϫ` | risk | property | risk level |
| `✠` | authority | `:PoliticalParty` | NDP, UCP, etc. |
| `⌖` | data | `:Donation` | Elections Alberta |
| `Ϡ` | type | property | classification |
| `Ͼ` | status | property | lifecycle |

### Relationship Types (12)
`SOURCE_OF`, `TARGET_OF`, `EVIDENCED_BY`, `PARENT_OF`, `RECEIVED_GRANT`, `SITS_ON`, `FLAGGED_AS`, `LOCATED_IN`, `DONATED_TO`, `CLUSTER_MEMBER`, `FUNDED_BY_FED`, `OPERATES_IN_ERA`

## Political Eras

| Era | Start | End | Premier | Key OICs |
|-----|-------|-----|---------|----------|
| NDP | 2015-05-24 | 2019-04-16 | Rachel Notley | O.C. 249/2015, O.C. 27/2016 |
| UCP-Kenney | 2019-04-30 | 2022-10-11 | Jason Kenney | O.C. 88/2019, O.C. 361/2022 |
| UCP-Smith | 2022-10-11 | present | Danielle Smith | O.C. 156/2023, O.C. 147/2025 |

## Folder Map

| Path | Purpose | Skill |
|------|---------|-------|
| `00-project-management/` | Workplan, decisions log, session notes | project-orchestration.skill.md |
| `01-data-assembly/` | Political era tagging, director networks, data pulls | data-assembly.skill.md |
| `02-graph-build/` | Neo4j schema, ingestion pipeline, Cypher DDL | graph-construction.skill.md |
| `03-smoking-gun-queries/` | Multi-hop Cypher queries for the core question | graph-analysis.skill.md |
| `04-synthesis/` | Business analysis, "So What" chain, narratives | analysis-synthesis.skill.md |
| `05-html-artifacts/` | Interactive HTML dashboards for demo | visualization.skill.md |
| `06-validation/` | Fact-checking, counter-arguments, evidence matrix | validation.skill.md |
| `.claude/skills/` | Aggregated skill loader | |

## Current State

- **Phase:** 0 (Project Setup — this file)
- **Next:** Phase 0 data assembly agents (4 parallel)
- **Neo4j:** Not yet running (Docker available v28.4.0)
- **Databricks:** SDK installed, tokens available

## Known Gotchas

- GOA grants have NO BN number — must fuzzy-match on recipient name to CRA
- CRA 2023 data only ~45% complete — prefer 2022 for financial analysis, 2024 for registry
- Entity mapping has `CULTURE,MULTICULTURALISMANDSTATUSOFWOMEN` corrupted name — use GRANTS_NAME_ALIASES
- Windows cp1252 encoding breaks KGL glyphs — always use `encoding='utf-8'`
- Elections Alberta has no bulk CSV — must use web extract tool or scrape
- MERGE not CREATE in Neo4j — entities appear in multiple datasets
