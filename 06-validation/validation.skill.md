# Validation Skill

Independently verify all claims, stress-test for counter-arguments, and produce human-validatable evidence traceability matrix. Every claim in the synthesis must survive this phase.

> **Upstream dependency:** `kgl-skill/analysis/agent-spawning/SKILL.md` (dual-agent verification pattern)

---

## When to Use

- After Phase 3 synthesis is complete
- Fact-checking specific claims against source documents
- Building the evidence traceability matrix
- Preparing counter-argument responses for political context

---

## Fact-Check Protocol (Agent 4A)

### Step 1: Ministry Lineage Verification
- Fetch live OIC URLs from King's Printer
- Cross-reference transform_events against actual OIC text
- Verify: event_type, event_date, source/target ministries all match

### Step 2: Grant Amount Spot-Checks
- Select 10 random (Organization, Ministry, FiscalYear) tuples from query results
- Verify totals against Databricks `goa_grants_disclosure` by summing individual payments
- Acceptable variance: < $1 (rounding only)

### Step 3: Director-Organization Verification
- Select 5 random director→org links
- Verify against Databricks `cra_directors_clean` raw data
- Check: BN matches, name matches, position matches

### Step 4: Current State Validation
- Fetch `https://www.alberta.ca/ministries`
- Verify all active ministries in graph match current GoA website
- Flag any mismatches

### Step 5: KGL Compliance Check
- Query all nodes: verify `kgl` and `kgl_handle` properties exist
- Verify glyph-handle pairs match KGL v1.3 canonical list
- Run edge cardinality checks (SPLIT = 1 source + 2+ targets, etc.)

---

## Evidence Traceability Matrix (Agent 4A/3C)

### Schema
```csv
claim_id,claim_text,claim_type,source_type,source_file,source_row_or_query,
value,unit,confidence,validation_status,validator_notes
```

### Claim Types
- `LINEAGE` — "Ministry X was created by OIC Y on date Z"
- `FUNDING` — "Organization A received $B from Ministry C in FY D"
- `DIRECTOR` — "Director E sits on board of Organization A"
- `RISK_FLAG` — "Organization A is flagged as J"
- `CLUSTER` — "Organizations A and K share N directors"
- `CONCENTRATION` — "Clustered orgs received $X avg vs $Y avg for non-clustered"

### Confidence Levels
| Level | Criteria | Source |
|-------|----------|--------|
| HIGH | Verified against primary source document | OIC text, Databricks table row |
| MEDIUM | Derived from validated data via query | Graph traversal result |
| LOW | Based on name matching or inference | Fuzzy name match across datasets |
| PENDING | Not yet verified | Awaiting human validation |

---

## Counter-Argument Stress Test (Agent 4B)

### Argument 1: "Pre-existing trends"
**Challenge:** "Funding increases through NDP-restructured ministries were just continuation of pre-2015 trends"
**Test:** Compare funding growth rates for the same organizations before NDP (PC era) vs during NDP. If growth rate was already high, the NDP-era increase is not attributable to restructuring.
**Query:** Include PC-era (pre-2015) data in the temporal comparison.

### Argument 2: "Cherry-picking"
**Challenge:** "You only showed the worst examples"
**Test:** Report full distribution — what % of all orgs funded through NDP-restructured ministries show the pattern? If 15%, say "15% of 312 organizations" not just "47 organizations."

### Argument 3: "Same pattern under UCP"
**Challenge:** "UCP restructuring shows the same funding concentration patterns"
**Test:** Run identical queries with UCP date filters. Compare results side by side. Three outcomes:
- NDP-only pattern → strong finding
- Both eras show it → systemic finding (still valuable, different narrative)
- UCP-only pattern → finding inverts (abort this angle)

### Argument 4: "Cluster membership is coincidental"
**Challenge:** "Organizations sharing directors is normal in the nonprofit sector; it doesn't indicate coordination or preferential funding"
**Test:** Compare funding distributions for clustered vs non-clustered organizations receiving grants through NDP-restructured ministries. Use Kolmogorov-Smirnov or Mann-Whitney U test. If clustered orgs receive statistically significantly more per-org funding than non-clustered orgs (p < 0.05), the concentration pattern is meaningful. Also test: does cluster size correlate with funding amount?

### Argument 5: "Name matching errors"
**Challenge:** "Organization name matching between GOA grants and CRA is unreliable"
**Test:** Use Databricks `goa_cra_matched` table (1,304 pre-matched records) as the gold standard. For any additional fuzzy matches, check city/province overlap. Report the number of ambiguous matches vs confident matches. All ambiguous matches should be flagged `confidence: LOW`.

---

## Output Files

- `06-validation/fact-check-report.md` — pass/fail for each verification step
- `06-validation/evidence-traceability.csv` — every claim with source citation
- `06-validation/evidence-traceability.html` — interactive filterable version
- `06-validation/counter-arguments.md` — pre-briefed responses to 5 counter-arguments

---

## Anti-Patterns

1. **Skipping spot-checks** — "the query ran successfully" ≠ "the results are correct"
2. **Ignoring the symmetry test** — this is the single most important counter-argument
3. **Treating MEDIUM confidence as HIGH** — cluster-based concentration claims must be explicitly flagged with statistical significance
4. **Validating your own work** — ideally, fact-check agent reads synthesis output cold, without knowing the expected answers
