# Operation Lineage Audit

**Graph-based political-era analysis of Alberta ministry restructuring, grant flows, and governance networks**

*HelpSeeker Technologies — TRACE Initiative*
*Ontology: KGL v1.3*

---

## What This Is

A unified Neo4j knowledge graph that combines:
- **Ministry lineage** (114 entities, 54 transformation events across 11 fiscal years)
- **GOA grant flows** (1.8M grant records, $28B+ total)
- **CRA charity data** (9,145 Alberta charities, financial profiles, risk flags)
- **Director governance networks** (571K director records, 380+ governance clusters)
- **Political donations** (Elections Alberta contributor data)

To answer a question that **only a graph can answer:**

> "Which organizations saw the largest funding increases through NDP-created or NDP-restructured ministries (2015-2019), shared board directors in governance clusters, and did those same directors appear as NDP donors — and what happened to that funding after UCP restructured the same ministries?"

## Project Structure

```
lineage-audit/
├── CLAUDE.md                           # Claude Code context (read first after compression)
├── README.md                           # This file
├── .gitignore
│
├── 00-project-management/
│   ├── WORKPLAN.md                     # Full phased workplan with agent statuses
│   ├── decisions-log.md                # Design decisions with rationale
│   └── project-orchestration.skill.md  # Session recovery & agent coordination
│
├── 01-data-assembly/
│   ├── data-assembly.skill.md          # Grant era tagging, director networks, donations
│   └── (output CSVs — gitignored)
│
├── 02-graph-build/
│   ├── graph-construction.skill.md     # Neo4j schema, ingestion, validation
│   ├── neo4j/
│   │   ├── schema/                     # Cypher DDL
│   │   └── queries/                    # Analytical queries
│   └── python/                         # Ingestion scripts
│
├── 03-smoking-gun-queries/
│   ├── graph-analysis.skill.md         # Multi-hop Cypher queries
│   └── (query result CSVs — gitignored)
│
├── 04-synthesis/
│   ├── analysis-synthesis.skill.md     # "So What" chain methodology
│   └── smoking-gun-synthesis.md        # Executive narrative (generated)
│
├── 05-html-artifacts/
│   ├── visualization.skill.md          # HTML dashboard generation
│   └── *.html                          # 5 interactive dashboards (generated)
│
├── 06-validation/
│   ├── validation.skill.md             # Fact-checking & counter-arguments
│   ├── evidence-traceability.csv       # Every claim traced to source
│   └── counter-arguments.md            # Pre-briefed rebuttals
│
└── .claude/skills/                     # Skill loader
```

## Pipeline

```
PHASE 0              PHASE 1             PHASE 2              PHASE 3             PHASE 4
Data Assembly    →   Graph Build     →   Smoking Gun     →    Synthesis     →    Validation
(4 agents)           (2 agents)          Queries              & HTML              & Stress Test
                                         (3 agents)           (3 agents)          (2 agents)
```

**13 agent instances | 5 phases | Max 4 concurrent**

## Data Sources

| Source | Records | Linkage Key |
|--------|---------|-------------|
| GOA Grants Disclosure | 1,806,202 | Recipient name (fuzzy → CRA) |
| CRA T3010 | 9,145 AB charities | BN number |
| CRA Directors | 571,461 | BN + director name |
| Federal G&C | ~70K AB | BN number |
| Ministry Lineage | 114 entities, 54 events | canonical_id |
| Elections Alberta | TBD | contributor name |

## Quick Start

```bash
# 1. Clone
git clone https://github.com/HelpSeekerTechnologies/lineage-audit.git

# 2. Start Neo4j
docker run -d --name lineage-audit \
  -p 7474:7474 -p 7687:7687 \
  -v lineage-audit-data:/data \
  -e NEO4J_AUTH=neo4j/<YOUR_NEO4J_LOCAL_PASSWORD> \
  neo4j:5-community

# 3. Read CLAUDE.md for full context
# 4. Follow WORKPLAN.md phases sequentially
```

## Deliverables

12 human-validatable artifacts — see [WORKPLAN.md](00-project-management/WORKPLAN.md) for full list.

---

*Built with Claude Code + KGL v1.3 ontology*
