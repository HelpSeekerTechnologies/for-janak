# Methodology — Operation Lineage Audit

**Version:** 1.0
**Date:** 2026-02-13
**Author:** HelpSeeker Technologies (automated pipeline)

---

## 1. The Graph

### Access

| Property | Value |
|----------|-------|
| **Platform** | Neo4j Aura (managed cloud) |
| **URI** | `<YOUR_NEO4J_AURA_URI>` |
| **User** | `neo4j` |
| **Password** | `<YOUR_NEO4J_AURA_PASSWORD>` |
| **Browser** | [Neo4j Aura Console](https://console.neo4j.io) → select instance `3a0f5f16` |

To connect from a local machine:
```bash
pip install neo4j
python -c "
from neo4j import GraphDatabase
d = GraphDatabase.driver('<YOUR_NEO4J_AURA_URI>', auth=('neo4j','<YOUR_NEO4J_AURA_PASSWORD>'))
with d.session() as s:
    print(s.run('MATCH (n) RETURN labels(n)[0] AS label, count(n) AS c ORDER BY c DESC LIMIT 20').data())
d.close()
"
```

### Pre-existing data (loaded by Archana's notebook, preserved)

The Aura instance contained data from prior HelpSeeker work before this audit:
- 193,193 Organization nodes (no `bn` property — CRA bulk load)
- 579,152 Person nodes (CRA director filings)
- 19,156 Director nodes (multi-board directors, pre-computed)
- 142 OrgEntity nodes (Alberta ministry lineage from `org_entities.csv`)
- 66 TransformEvent nodes (ministry restructuring events from `transform_events.csv`)
- 42 SourceDocument nodes (OIC references)
- Relationships: SOURCE_OF (62), TARGET_OF (70), PARENT_OF (89), EVIDENCED_BY (82), SITS_ON (509)

**Decision:** All pre-existing data was preserved. New nodes were MERGEd alongside existing nodes using unique keys (`bn`, `normalized_name`, etc.) so they do not collide with the pre-existing keyless nodes.

### Nodes added by this audit

| Label | Count | Key Property | Source |
|-------|-------|-------------|--------|
| Organization | 9,145 new (11,672 total with BN) | `bn` (CRA Business Number) | `ab_org_risk_flags` table |
| Director | 19,156 (merged with existing) | `normalized_name` | `multi_board_directors` table |
| FiscalYear | 2,679 | `year` | `grants_aggregated.csv` fiscal_year column |
| Region | 494 | `name` | `org_risk_flags.csv` City column |
| RiskFlag | 7 | `flag_type` | Hardcoded from 7 flag column names |

### Relationships added by this audit

| Type | Count | Pattern | Source |
|------|-------|---------|--------|
| RECEIVED_GRANT | 17,993 new (35,075 total) | Organization → OrgEntity | `grants_aggregated.csv` matched via `goa_cra_matched.csv` |
| SITS_ON | 9,313 new (11,023 total) | Director → Organization | `multi_board_directors.csv` linked_bns |
| FLAGGED_AS | 9,216 new (11,033 total) | Organization → RiskFlag | `org_risk_flags.csv` flag columns |
| LOCATED_IN | 9,145 new (10,723 total) | Organization → Region | `org_risk_flags.csv` City column |
| SHARED_DIRECTORS | 6,826 new (9,081 total) | Organization → Organization | `org_network_edges_filtered` table |

---

## 2. Data Sources

### 2.1 GOA Grants Disclosure

| Property | Value |
|----------|-------|
| **Databricks table** | `dbw_unitycatalog_test.default.goa_grants_disclosure` |
| **Rows** | 1,806,214 |
| **Timespan** | 2014–2025 (individual payment records) |
| **Publisher** | Government of Alberta, Open Data |
| **All columns (all STRING type)** | Ministry, BUName, Recipient, Program, Amount, Lottery, PaymentDate, FiscalYear, DisplayFiscalYear, Fiscal_Year, _rescued_data |

**Column types problem:** Every column in this table is stored as STRING, including Amount and PaymentDate. This required CAST operations during aggregation:
- `CAST(Amount AS DOUBLE)` for dollar values
- `CAST(PaymentDate AS DATE)` for era assignment

**FiscalYear column corruption:** The `FiscalYear` column contains a mix of:
- Valid fiscal years: `2014`, `2015`, ..., `2024`
- Dollar amounts: `-1000000.000`, `10000.000`, `500000.000`
- Full dates: `2014-04-01`, `2019-06-18`
- Program names: `ALBERTA`, `ANTI-RACISM`, `OVERNIGHT CAMPS`
- Booleans: `True`, `False`

**Cleaning decision:** For the aggregation query, we grouped by the raw `FiscalYear` value as-is. Political era assignment used `PaymentDate` (not `FiscalYear`), so the FiscalYear corruption does not affect the era-based findings. However, it created 2,679 FiscalYear nodes in Neo4j, most of which are garbage. Only ~11 represent real fiscal years.

### 2.2 GOA-CRA Matched Organizations (Gold Standard)

| Property | Value |
|----------|-------|
| **Databricks table** | `dbw_unitycatalog_test.default.goa_cra_matched` |
| **Rows** | 1,304 |
| **Purpose** | Pre-verified mapping of GOA grant recipient names → CRA Business Numbers |
| **Columns** | goa_name (string), bn (string), cra_name (string), n_ministries (bigint), goa_total (double), n_grants (bigint), ministries (array\<string\>), Total_Revenue (double), total_gov_rev (double), gov_dependency_pct (double), program_pct (double), compensation_pct_of_exp (double), + 7 flag columns (int) |

**How the gold standard was built:** These 1,304 matches were pre-computed in a separate HelpSeeker analysis that used exact name matching + manual verification between GOA grant recipients and CRA T3010 legal names. No fuzzy matching was used in the gold standard.

**Cleaning decisions:**
- 4 rows had blank `bn` → excluded from name lookup (1,300 usable entries)
- `ministries` column stored as `array<string>` in Databricks → converted to pipe-delimited string in CSV export

### 2.3 Ministry Entities

| Property | Value |
|----------|-------|
| **Source** | Databricks Volume: `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/org_entities.csv` |
| **Rows** | 142 |
| **Columns** | canonical_id, name, level, status, start_date, end_date, normalized_name, aliases, jurisdiction, kgl_sequence, _rescued_data |

**Provenance:** Manually curated by HelpSeeker from Government of Alberta ministry records. Each row represents one ministry entity across its full lifecycle. Entities that were renamed get separate rows with different canonical_ids.

**Cleaning decisions:**
- Ministry name lookup built from both `name` and `aliases` columns → 150 name variants mapping to 142 canonical_ids
- Normalized to UPPER for case-insensitive matching against grants data
- Match rate: 701,864 of 702,930 aggregation groups (99.8%)
- **1 unmatched variant:** `CULTURE,MULTICULTURALISMANDSTATUSOFWOMEN` (1,066 groups) — a corrupted name with no spaces or punctuation. No matching OrgEntity exists. These 1,066 groups were excluded from graph ingestion.

### 2.4 Transform Events (Ministry Restructuring)

| Property | Value |
|----------|-------|
| **Source** | Databricks Volume: `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/transform_events.csv` |
| **Rows** | 66 |
| **Columns** | event_id, event_type (RENAME/TRANSFER/MERGE/SPLIT/CREATE/ABOLISH), event_date, effective_fy, confidence, evidence_basis, political_context, notes, kgl_sequence, _rescued_data |

**Provenance:** Manually curated by HelpSeeker from Orders in Council (OICs) published by the Alberta King's Printer. Each row is a structural change to the ministry architecture.

**Event date storage:** Stored as `DATE` type in Neo4j (confirmed: `apoc.meta.cypher.type(evt.event_date)` returns `DATE`). This means date comparisons work natively in Cypher.

**Graph relationships:**
- `(OrgEntity)-[:SOURCE_OF]->(TransformEvent)` — the ministry that was restructured FROM (62 relationships)
- `(TransformEvent)-[:TARGET_OF]->(OrgEntity)` — the ministry that was restructured TO (70 relationships)
- Note the asymmetry: SOURCE_OF points OrgEntity→TransformEvent, but TARGET_OF points TransformEvent→OrgEntity

**NDP event identification:** 14 events have `political_context` containing "NDP" or `event_date` between 2015-05-24 and 2019-04-29. These 14 events TARGET 13 distinct OrgEntity nodes (the "NDP-restructured ministries").

### 2.5 CRA Charity Risk Flags

| Property | Value |
|----------|-------|
| **Databricks table** | `dbw_unitycatalog_test.default.ab_org_risk_flags` |
| **Rows** | 9,145 |
| **Columns** | 35 columns (see director_assembly_log.md for full list) |
| **Key columns** | bn (string), Legal_name (string), Account_name (string), City (string), + financial metrics (double) + 7 flag columns (int: 0 or 1) |

**Source of underlying data:** CRA T3010 annual charity returns. This is a single-year snapshot (most recent filing year), not a time series.

**The 7 risk flags:**

| Flag | Definition | Count |
|------|-----------|-------|
| `flag_low_passthrough` | Organization passes through <25% of revenue to programs | 3,864 |
| `flag_salary_mill` | Compensation exceeds 50% of expenditures | 328 |
| `flag_high_gov_dependency` | Government revenue exceeds 80% of total revenue | 644 |
| `flag_deficit` | Organization is in deficit (expenditures > revenue) | 3,624 |
| `flag_insolvency_5pct_cut` | Organization would be insolvent with 5% revenue cut | 314 |
| `flag_shadow_network` | Organization is part of a shadow governance network | 23 |
| `flag_in_director_cluster` | Organization is in a shared-director cluster | 419 |

**Cleaning decisions:**
- Financial fields with NULL/NaN left as-is (469 orgs have NULL program_pct, admin_pct, etc.)
- `deficit_amount` is a continuous field separate from the binary flag
- All 9,145 orgs have non-null `bn` — no exclusions needed

### 2.6 Multi-Board Directors

| Property | Value |
|----------|-------|
| **Databricks table** | `dbw_unitycatalog_test.default.multi_board_directors` |
| **Rows** | 19,156 |
| **Columns** | clean_name_no_initial (string), n_boards (bigint), linked_bns (array\<string\>), n_non_arms_length (bigint), earliest_start (date), latest_start (date) |

**Definition:** Directors who sit on the boards of 3 or more CRA-registered charities, identified from CRA T3010 Schedule B (directors/officers) filings.

**Name normalization:** `clean_name_no_initial` is pre-normalized: UPPER case, middle initials removed, standardized. This was done upstream in the Databricks pipeline, not by this audit.

**linked_bns parsing:** Stored as `array<string>` in Databricks, exported as JSON array string in CSV. Parsed back to Python list via `json.loads()` with `ast.literal_eval()` fallback. Total BN references: 83,887 across 19,156 directors.

**SITS_ON match rate:** Only 9,313 of 83,887 director→BN references (11.1%) matched the 9,145 organizations in our `org_risk_flags` set. The remaining 74,574 BNs belong to charities outside Alberta or outside the risk-flagged set.

### 2.7 Governance Clusters

| Property | Value |
|----------|-------|
| **Databricks table** | `dbw_unitycatalog_test.default.org_clusters_strong` |
| **Rows** | 4,636 |
| **Columns** | bn (string), cluster_id (int), cluster_size (int) |

**How clusters were computed:** Pre-computed upstream using connected components on the shared-director graph. Two organizations are in the same cluster if they share one or more directors (directly or transitively through intermediate orgs). The `_strong` suffix indicates a minimum threshold was applied (likely ≥2 shared directors or ≥3 connected orgs).

**Distribution:** 1,540 unique clusters. Size: min=2, max=61, median=2, mean=3.01.

**Cleaning decisions:**
- `cluster_id` set as integer property on Organization nodes in Neo4j
- Only 509 of 4,636 clustered orgs also exist in the 9,145 `org_risk_flags` set (the rest have BNs not in our flagged Alberta charities)
- Of those 509, only 143 also have RECEIVED_GRANT edges

### 2.8 Organization Network Edges

| Property | Value |
|----------|-------|
| **Databricks table** | `dbw_unitycatalog_test.default.org_network_edges_filtered` |
| **Rows** | 154,015 |
| **Columns** | org1_bn (string), org2_bn (string), n_shared_directors (bigint), shared_director_names (array\<string\>) |

**Filtering for graph:** Only edges where BOTH org1_bn and org2_bn are in the 9,145 org_risk_flags set → 6,826 edges (4.4% of total). The remainder involve organizations outside our Alberta flagged-charity scope.

### 2.9 Federal Grants (GoC Proactive Disclosure)

| Property | Value |
|----------|-------|
| **Source** | Databricks Volume: `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/grants.csv` |
| **File size** | 2.2 GB |
| **Total rows (all provinces)** | 1,811,088 |
| **Alberta rows extracted** | 117,525 (filtered by `recipient_province IN ('AB', 'ALBERTA')`) |
| **After deduplication** | 109,583 |
| **Original columns** | 39 columns from GoC proactive disclosure (ref_number, recipient_business_number, recipient_legal_name, owner_org_title, prog_name_en, agreement_value, agreement_start_date, recipient_province, etc.) |

**Column mapping applied:**

| Output Column | Source Column | Transformation |
|---------------|-------------|----------------|
| `BN` | `recipient_business_number` | Stripped whitespace |
| `org_name` | `recipient_legal_name` | As-is |
| `federal_department` | `owner_org_title` | As-is |
| `program` | `prog_name_en` | As-is |
| `amount` | `agreement_value` | Cast to float |
| `fiscal_year` | `agreement_start_date` | Derived Apr-Mar FY (e.g., 2018-07-03 → 2018-2019) |
| `province` | `recipient_province` | Used for filtering only |

**Cleaning decisions:**
- 7,942 duplicate rows removed (117,525 → 109,583)
- 17 rows with unparseable dates in fiscal_year left as-is
- BN populated: 52,349 (47.8%), BN missing: 57,234 (52.2%)
- Fiscal year column has some garbage values (e.g., `0`, `100000`, `187500.0`) from rows where `agreement_start_date` contained non-date data
- **Not ingested into Neo4j** — federal grants were assembled for enrichment but not loaded into the graph in this phase

---

## 3. Cleaning Decisions Log

| # | Decision | Rationale | Impact |
|---|----------|-----------|--------|
| C1 | Exclude 6 rows with NULL/blank Ministry from grant aggregation | Cannot assign to a ministry | 6 of 1,806,214 rows (0.0003%) |
| C2 | Exclude 6,160 rows with NULL/blank PaymentDate | Cannot assign political era | 6,160 of 1,806,214 (0.34%) |
| C3 | CAST Amount from STRING to DOUBLE | Source table stores everything as string | Affects all 1.8M rows |
| C4 | CAST PaymentDate from STRING to DATE | Needed for era boundary comparison | Affects all non-null rows |
| C5 | Use `goa_cra_matched` as primary name→BN lookup (1,300 entries) | Gold standard, pre-verified matches | Conservative — misses orgs not in gold standard |
| C6 | Extend lookup with Legal_name + Account_name from `org_risk_flags` (→10,154 entries) | Exact match only, no fuzzy | Adds coverage but only for orgs already in CRA risk set |
| C7 | Exclude grant rows where recipient doesn't match any known BN | No false positives; accept low recall | 684,425 of 702,930 groups unmatched (97.4%) |
| C8 | Exclude grant rows where ministry doesn't match any OrgEntity | 1 corrupted name variant | 512 groups (0.07%) |
| C9 | Use MERGE (not CREATE) for all Neo4j operations | Idempotent; safe for re-runs | No duplicate nodes/edges |
| C10 | Skip `org_bn` uniqueness constraint (failed) | Pre-existing duplicate BNs in Aura | MERGE still works correctly without constraint |
| C11 | Create FiscalYear nodes for ALL fiscal_year values including garbage | No easy way to filter valid years server-side | 2,679 nodes, ~2,668 are garbage |
| C12 | Filter SHARED_DIRECTORS edges to both-in-set only | Only edges between known Alberta charities are useful | 6,826 of 154,015 (4.4%) |
| C13 | Filter SITS_ON edges to known org BNs only | Can only link directors to orgs in our graph | 9,313 of 83,887 (11.1%) |
| C14 | ~~Federal grants NOT ingested into Neo4j~~ **RESOLVED** | Now ingested: 6,470 FUNDED_BY_FED edges, 8 FederalDepartment nodes, 1,348 matched BNs | 1,666 dual-funded orgs (GOA + federal) identified |

---

## 4. Assumptions

| # | Assumption | Justification | Risk if Wrong |
|---|-----------|---------------|---------------|
| A1 | **Political era boundaries are correct** (NDP: 2015-05-24 to 2019-04-29, etc.) | Based on official premier swearing-in dates from Elections Alberta | If boundaries are off by days, a few grants near transitions could be misclassified. Unlikely to affect aggregate findings. |
| A2 | **PaymentDate (not FiscalYear) determines political era** | FiscalYear column is corrupted; PaymentDate is the only reliable date field | Grants approved under one government but paid under the next would be assigned to the paying government's era. This is intentional — we measure when money flowed, not when it was committed. |
| A3 | **"NDP-restructured ministry" = target of any TransformEvent dated during NDP era** | TransformEvents have DATE-typed event_date and political_context fields | Some restructurings may have been planned under the previous government or merely renamed. We classify by execution date, not intent. |
| A4 | **Name matching is exact (no fuzzy/probabilistic matching)** | Gold standard pre-verified; additional matches are exact Legal_name/Account_name | We sacrifice recall for precision. The 2.6% match rate is a floor, not a ceiling. Many genuine matches are missed. |
| A5 | **Cluster membership is static** | Pre-computed from most recent CRA filings; not tracked over time | Directors may have joined/left boards during the study period. Cluster composition could differ between PC, NDP, and UCP eras. |
| A6 | **Risk flags are based on a single-year snapshot** | CRA T3010 data from most recent filing year | An org flagged as "deficit" today may not have been in deficit during NDP era. Risk flags are a current indicator, not a historical one. |
| A7 | **The 9,145 orgs in `org_risk_flags` are representative of funded charities** | They are CRA-registered charities with at least one risk flag OR in a director cluster | This is a biased sample — it over-represents problematic organizations. Non-flagged, non-clustered charities are not in this set. The disparity ratios compare WITHIN this set. |
| A8 | **Grant amounts in `goa_grants_disclosure` are correct as published** | Government of Alberta open data | We did not independently verify individual grant amounts. The $0 amounts on some RECEIVED_GRANT edges suggest some rows had amount/fiscal_year column swaps in the source data. |
| A9 | **The GOA-CRA name match (1,304 records) is the ground truth** | Pre-verified by separate HelpSeeker analysis | If any of these 1,304 matches are wrong, the downstream grant-to-org links are wrong. No independent verification was performed in this audit. |
| A10 | **MERGE operations are idempotent across multiple script runs** | Neo4j MERGE semantics guarantee this | Three script executions occurred due to session expiry. Relationship counts were validated after each run. |

---

## 5. Pipeline Architecture

### Agents

| Agent | Phase | Input | Output | Runtime |
|-------|-------|-------|--------|---------|
| 0A (Grant-Ministry Linker) | Data Assembly | Databricks: goa_grants_disclosure, goa_cra_matched, org_entities.csv, transform_events.csv | grants_aggregated.csv (702,930 rows), goa_cra_matched.csv, entity_mapping.csv, transform_events.csv | ~55s |
| 0B (Director Network Builder) | Data Assembly | Databricks: multi_board_directors, org_clusters_strong, ab_org_risk_flags, org_network_edges_filtered | multi_board_directors.csv, org_clusters.csv, org_risk_flags.csv, org_network_edges.csv | ~9s |
| 0D (Federal Grants) | Data Assembly | Databricks Volume: GoC Grants/grants.csv | federal_grants.csv (109,583 rows) | ~108s |
| 1A/1B (Graph Builder) | Graph Construction | All Phase 0 CSVs → Neo4j Aura | 76,935 relationships across 5 types | ~2.5 hours (3 script runs) |
| 2 (Governance Analysis) | Graph Analysis | Neo4j Cypher queries | 4 result CSVs + query_log.md | ~8s |
| 1C (Federal Grants) | Graph Construction | federal_grants.csv → Neo4j Aura | 6,470 FUNDED_BY_FED edges, 8 FederalDepartment nodes | ~195s |
| 4C (Statistical Tests) | Validation | Neo4j → Mann-Whitney U + KS test | statistical_test_results.md | ~15s |

### Databricks Connection

| Property | Value |
|----------|-------|
| Host | `<YOUR_DATABRICKS_HOST>` |
| HTTP Path | `<YOUR_DATABRICKS_SQL_WAREHOUSE>` |
| Catalog | `dbw_unitycatalog_test` |
| Schema | `default` |

### Aggregation SQL (Agent 0A, Step 4)

The core aggregation that produced `grants_aggregated.csv` ran server-side on Databricks:

```sql
SELECT
    Recipient                           AS recipient,
    Ministry                            AS ministry,
    FiscalYear                          AS fiscal_year,
    CASE
        WHEN CAST(PaymentDate AS DATE) < DATE '2015-05-24' THEN 'PC'
        WHEN CAST(PaymentDate AS DATE) BETWEEN DATE '2015-05-24' AND DATE '2019-04-29' THEN 'NDP'
        WHEN CAST(PaymentDate AS DATE) BETWEEN DATE '2019-04-30' AND DATE '2022-10-10' THEN 'UCP_Kenney'
        WHEN CAST(PaymentDate AS DATE) > DATE '2022-10-10' THEN 'UCP_Smith'
        ELSE 'UNKNOWN'
    END                                 AS political_era,
    SUM(CAST(Amount AS DOUBLE))         AS total_amount,
    COUNT(*)                            AS n_payments,
    MIN(CAST(PaymentDate AS DATE))      AS earliest_payment,
    MAX(CAST(PaymentDate AS DATE))      AS latest_payment
FROM dbw_unitycatalog_test.default.goa_grants_disclosure
WHERE Ministry IS NOT NULL AND TRIM(Ministry) != ''
  AND PaymentDate IS NOT NULL AND TRIM(PaymentDate) != ''
GROUP BY Recipient, Ministry, FiscalYear,
    CASE
        WHEN CAST(PaymentDate AS DATE) < DATE '2015-05-24' THEN 'PC'
        WHEN CAST(PaymentDate AS DATE) BETWEEN DATE '2015-05-24' AND DATE '2019-04-29' THEN 'NDP'
        WHEN CAST(PaymentDate AS DATE) BETWEEN DATE '2019-04-30' AND DATE '2022-10-10' THEN 'UCP_Kenney'
        WHEN CAST(PaymentDate AS DATE) > DATE '2022-10-10' THEN 'UCP_Smith'
        ELSE 'UNKNOWN'
    END
```

**Note:** The GROUP BY includes FiscalYear, which means rows with the same Recipient + Ministry + Era but different FiscalYear values are separate aggregation groups. This is why there are 702,930 groups from 1.8M rows.

---

## 6. Known Data Quality Issues

| # | Issue | Severity | Impact on Findings |
|---|-------|----------|-------------------|
| DQ1 | FiscalYear column corruption (amounts, dates, strings mixed with years) | MEDIUM | Does NOT affect era-based analysis (uses PaymentDate). Creates garbage FiscalYear nodes in Neo4j. |
| DQ2 | 2.6% grant match rate (17,993 of 702,930 groups) | HIGH | Findings represent CRA-registered charities ONLY, not all grant recipients. The 97.4% unmatched includes municipalities, school boards, health authorities, individuals. |
| DQ3 | org_bn uniqueness constraint failure (duplicate BNs) | LOW | Pre-existing from prior data load. MERGE operations handle correctly. |
| DQ4 | $0 amounts on some RECEIVED_GRANT edges | MEDIUM | Some grant rows had amount/fiscal_year column swaps. The "top grants by amount" spot check showed $0 for CENTRAL ALBERTA CHILD ADVOCACY CENTRE, likely due to this. |
| DQ5 | 71,248 grant groups (10.1%) assigned to UNKNOWN era | MEDIUM | PaymentDate values that failed DATE cast. These are excluded from PC/NDP/UCP comparisons. |
| DQ6 | 1,066 grant groups reference corrupted ministry name `CULTURE,MULTICULTURALISMANDSTATUSOFWOMEN` | LOW | These could not be matched to an OrgEntity. Excluded from graph ingestion. |
| DQ7 | Cluster membership is single-point-in-time, not temporal | MEDIUM | Clusters may have formed or dissolved during the study period. The analysis assumes static cluster composition. |
| DQ8 | Risk flags are single-year snapshot | MEDIUM | A deficit flag today doesn't mean deficit during NDP era. Flags indicate current risk, not historical. |
| DQ9 | ~~Federal grants assembled but not ingested into graph~~ **RESOLVED** | — | Now ingested: 6,470 FUNDED_BY_FED edges across 8 federal departments. 1,666 dual-funded orgs identified. |
| DQ10 | Some duplicate org rows in Q1 output (NAIT appears 4x, Mount Royal 6x) | LOW | Caused by multiple RECEIVED_GRANT edges to different OrgEntity nodes for same org. Aggregate totals are correct; row count inflated. |

---

## 7. Reproducibility

All scripts are committed to `github.com/HelpSeekerTechnologies/lineage-audit`:

| Script | Phase | Purpose |
|--------|-------|---------|
| `01-data-assembly/agent_0a_grant_linker.py` | 0 | GOA grants aggregation from Databricks |
| `01-data-assembly/agent_0b_director_network.py` | 0 | Director/cluster/risk flag pulls |
| `01-data-assembly/agent_0d_federal_grants_v2.py` | 0 | Federal grants extraction |
| `02-graph-build/agent_1_graph_builder.py` | 1 | Full graph construction (Steps 1-10) |
| `02-graph-build/agent_1_resume.py` | 1 | Resume script for Steps 8-10 |
| `03-governance-queries/agent_2_governance_queries.py` | 2 | All Cypher queries + symmetry test |
| `02-graph-build/agent_1_federal_grants.py` | 1C | Federal grants ingestion into Neo4j |
| `06-validation/statistical_tests.py` | 4C | Mann-Whitney U + KS significance tests |

To reproduce from scratch:
1. Ensure Databricks and Neo4j Aura connections are active
2. Run agents 0A, 0B, 0D (produces CSVs in `01-data-assembly/`)
3. Run agent 1 (ingests CSVs into Neo4j Aura)
4. Run agent 2 (executes Cypher queries, produces result CSVs in `03-governance-queries/`)

**Dependencies:** `pip install databricks-sql-connector neo4j pandas`
