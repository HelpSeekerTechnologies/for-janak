# Analysis Synthesis Skill

Transform raw graph query results into decision-ready insights using the "So What" chain methodology from HelpSeeker's business-analysis-synthesis skill (kgl-skill repo). Every finding must be quantified, scoped, causal, cited, and actionable.

> **Upstream dependency:** `kgl-skill/analysis/business-analysis-synthesis/SKILL.md`

---

## When to Use

- Turning Cypher query CSVs into executive narratives
- Building the "So What" chain for each finding
- Framing results for political audience (Danielle Smith / UCP perspective)
- Generating the governance-synthesis.md deliverable

---

## The "So What" Chain (Mandatory for Every Finding)

```
OBSERVATION → PATTERN → IMPACT → DECISION

"What did we find?" → "What does it mean?" → "Who/what is affected?" → "What should we do?"
```

If you cannot complete the chain, the finding is not ready for output.

### Example

```
OBSERVATION: 47 organizations received $340M through NDP-restructured ministries
PATTERN:     12 of those orgs share directors in 3 governance clusters
IMPACT:      Those clusters received 4.2x more per-org than non-clustered orgs
             during NDP era; funding dropped 31% under UCP for the same successor ministries
DECISION:    Consolidated audit of cluster-level funding through ministry lineage,
             with specific review of the 3 clusters for governance capture indicators
```

---

## Quality Gates (ALL must pass)

| Gate | Test | Fail Example | Pass Example |
|------|------|-------------|--------------|
| **Quantified** | No "many", "some", "significant" | "Many orgs got more funding" | "47 of 312 orgs (15%) saw >50% increases" |
| **Scoped** | States affected population/dollars | "Funding increased" | "$340M across 47 orgs in 3 clusters" |
| **Causal** | Explains WHY, not just WHAT | "Funding was higher during NDP" | "NDP O.C. 249/2015 created Ministry X, which became the primary funder" |
| **Cited** | Every number traceable to source | "About $340M" | "$340,247,891 (GOA grants FY2015-16 through FY2018-19, summed)" |
| **Actionable** | Points to specific decision | "This should be reviewed" | "Recommend consolidated audit of Cluster 7 (12 orgs, $89M, 4 shared directors)" |

---

## Output Template: governance-synthesis.md

```markdown
# Governance Analysis Synthesis — Operation Lineage Audit

## Executive Summary (1 paragraph, 3 numbers max)
{The single most important finding in 2-3 sentences}

## The Graph-Only Question
{State the question and why only a graph can answer it}

## Finding 1: NDP Ministry Funding Patterns
### Observation
### Pattern
### Impact
### Recommendation
### Evidence Chain (cite specific source documents)

## Finding 2: Director-Donation Convergence
### Observation
### Pattern
### Impact
### Recommendation
### Evidence Chain

## Finding 3: Governance Cluster Concentration
### Observation
### Pattern
### Impact
### Recommendation
### Evidence Chain

## Symmetry Test: UCP-Era Comparison
{Same analysis, UCP dates — does the pattern hold?}

## Methodology
{How the graph was built, what data sources, what confidence levels}

## Limitations & Counter-Arguments
{Pre-briefed responses — see 06-validation/counter-arguments.md}

## Appendix: Evidence Traceability
{Link to evidence-traceability.csv}
```

---

## Framing for Political Audience

### What Danielle Smith Wants to Hear
1. **Governance capture narrative:** "NDP built ministry structures that disproportionately funded their donor networks"
2. **Stewardship narrative:** "UCP restructuring redirected funding to more accountable channels"
3. **Waste narrative:** "X% of NDP-era funded orgs were flagged as salary mills or low-passthrough"

### What Must Be Defensible
1. All claims traced to public source documents (OICs, grants data, CRA filings)
2. Symmetry test shows whether pattern is NDP-specific or systemic
3. Director-donation links explicitly flagged with confidence level
4. No causal claims without evidence — "correlated with" not "caused by"

---

## Anti-Patterns

1. **Catalog without synthesis** — listing 47 orgs without explaining the pattern = useless
2. **Generic recommendations** — "review and clarify" instead of "audit Cluster 7's $89M through Ministry X"
3. **Unquantified impact** — "many organizations" instead of "47 of 312 (15%)"
4. **Missing causality** — stating funding increased without explaining the ministry lineage chain
5. **Partisan tone in methodology** — the analysis must be technically neutral even if the framing is political
