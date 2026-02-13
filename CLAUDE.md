# Operation Lineage Audit — Claude Code Context

## Project Overview

Graph-based political-era analysis of Alberta ministry restructuring, grant flows, and director governance networks. Builds a unified Neo4j knowledge graph aligned to KGL v1.3 ontology to answer questions that **only a graph can answer** — specifically tracing funding through ministry lineage chains across political eras (NDP 2015-2019, UCP 2019-present) and detecting disproportionate funding concentration in governance clusters.

## The Core Governance Question

"Which governance clusters (organizations sharing board directors) received disproportionate funding increases through NDP-restructured ministries compared to non-clustered organizations — and did that concentration pattern reverse under UCP?"

This requires multi-hop graph traversal: ministry lineage traversal + director network cluster detection + temporal funding comparison across political eras — impossible with flat tables.

## Key References

- **Ministry Genealogy Graph:** `C:\Users\alina\OneDrive\Desktop\ministry-genealogy-graph\` (114 entities, 54 events, 1.8M harmonized grants)
- **Janak Demo:** `C:\Users\alina\OneDrive\Desktop\Janak Demo\` (CRA analysis, director networks, risk flags, 14 HTML dashboards)
- **KGL Skills Repo:** https://github.com/HelpSeekerTechnologies/kgl-skill (private)
- **KGL Ontology:** https://github.com/HelpSeekerTechnologies/kgl-ontology
- **This Repo:** https://github.com/HelpSeekerTechnologies/lineage-audit

## Architecture

- **Graph database:** Neo4j Aura (`<YOUR_NEO4J_AURA_URI>`, user: `neo4j`, password: `<YOUR_NEO4J_AURA_PASSWORD>`) — 264 nodes / 317 relationships already loaded (ministry lineage from Archana's notebook)
- **Data warehouse:** Databricks (`<YOUR_DATABRICKS_HOST>`) — **source of truth for all data** (D009)
- **Databricks Token:** `<YOUR_DATABRICKS_TOKEN>`
- **Databricks SQL Warehouse:** `<YOUR_DATABRICKS_SQL_WAREHOUSE>`
- **Unity Catalog:** `dbw_unitycatalog_test`

## Data Sources (all from Databricks — D009)

### Databricks Tables (`dbw_unitycatalog_test.default`)

| Source | Table | Records | Key Fields |
|--------|-------|---------|------------|
| AB Org Risk Flags | `ab_org_risk_flags` | 9,145 | All AB charities with risk flags |
| AB Master Profile | `ab_master_profile` | 9,446 | Master joining CRA+GOA+Federal |
| AB Name History | `ab_name_history` | ~82K | Name tracking 2015-2023 |
| GOA Grants Disclosure | `goa_grants_disclosure` | 1,806,214 | All GOA grants 2014-2025 |
| GOA Multi-Ministry | `goa_multi_ministry` | 4,597 | Recipients from 2+ ministries |
| GOA-CRA Matched | `goa_cra_matched` | 1,304 | GOA recipients matched to CRA |
| CRA Directors 2023 | `cra_directors_2023` | — | BN, Last Name, First Name, Position |
| CRA Directors 2024 | `cra_directors_2024` | — | Updated directors |
| CRA Directors Clean | `cra_directors_clean` | 570,798 | Normalized director records |
| Multi-Board Directors | `multi_board_directors` | 19,156 | Directors on 3+ boards |
| Multi-Board Enriched | `multi_board_enriched` | — | Enriched multi-board data |
| Org Clusters (Strong) | `org_clusters_strong` | 4,636 | Cluster assignments |
| Cluster Financials | `org_cluster_strong_financials` | 1,540 | Cluster financial summaries |
| Org Network Edges | `org_network_edges_filtered` | 154,015 | Org-to-org edges |

### Databricks Volume Files

| Source | Volume Path | Description |
|--------|-------------|-------------|
| CRA Full (2010-2024) | `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/CRA - Oct 2025 update/CRA - Cleaned/` | CRA-2010-Full.csv through CRA-2024-Full.csv |
| Ministry Lineage | `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/` | org_entities.csv, transform_events.csv, edges_*.csv |
| GoC Grants | `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/` | Federal G&C data |
| GoA Grants for Graph | `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoA Grants for Graph/` | GOA grants formatted for graph |
| Governor Party vs Funding | `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Governor Party vs Funding/` | Political era funding analysis |

## Neo4j Graph Schema (KGL-Aligned)

### Node Types (12 KGL canonical nodes)
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
| `Ϡ` | type | property | classification |
| `Ͼ` | status | property | lifecycle |

### Relationship Types (10)
`SOURCE_OF`, `TARGET_OF`, `EVIDENCED_BY`, `PARENT_OF`, `RECEIVED_GRANT`, `SITS_ON`, `FLAGGED_AS`, `LOCATED_IN`, `CLUSTER_MEMBER`, `FUNDED_BY_FED`

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
| `03-governance-queries/` | Multi-hop Cypher queries for the core question | graph-analysis.skill.md |
| `04-synthesis/` | Business analysis, "So What" chain, narratives | analysis-synthesis.skill.md |
| `05-html-artifacts/` | Interactive HTML dashboards for demo | visualization.skill.md |
| `06-validation/` | Fact-checking, counter-arguments, evidence matrix | validation.skill.md |
| `.claude/skills/` | Aggregated skill loader | |

## Current State

- **Phase:** 0 (Project Setup — this file)
- **Next:** Phase 0 data assembly agents (3 parallel)
- **Neo4j Aura:** 264 nodes, 317 relationships (ministry lineage already loaded by Archana — D011)
- **Databricks:** SDK installed, tokens available — source of truth for all data (D009)

## Known Gotchas

- GOA grants have NO BN number — must fuzzy-match on recipient name to CRA (use `goa_cra_matched` table for pre-matched records)
- CRA 2023 data only ~45% complete — prefer 2022 for financial analysis, 2024 for registry
- Entity mapping has `CULTURE,MULTICULTURALISMANDSTATUSOFWOMEN` corrupted name — use GRANTS_NAME_ALIASES
- Windows cp1252 encoding breaks KGL glyphs — always use `encoding='utf-8'`
- MERGE not CREATE in Neo4j — entities appear in multiple datasets
- Neo4j Aura already has 264 nodes — use MERGE to extend idempotently (D011)
- All data sourced from Databricks tables/volumes — do NOT read local CSV files (D009)
