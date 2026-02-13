# Grant Assembly Log (Agent 0A)

**Generated:** 2026-02-12 19:49:59
**Agent:** 0A (Grant-Ministry Linker)
**Source:** Databricks `dbw_unitycatalog_test.default`

## Data Sources

| Source | Rows | Notes |
|--------|------|-------|
| `goa_grants_disclosure` | 1,806,214 | All GOA grants 2014-2025 |
| `goa_cra_matched` | 1,304 | Gold standard GOA-CRA name matches |
| `org_entities.csv` (Volume) | 142 | Ministry entity mapping |
| `transform_events.csv` (Volume) | 66 | Ministry restructuring events |

## Political Era Boundaries (D002)

| Era | Start | End | Premier |
|-----|-------|-----|---------|
| PC | pre-2015 | 2015-05-23 | Various PCs |
| NDP | 2015-05-24 | 2019-04-29 | Rachel Notley |
| UCP_Kenney | 2019-04-30 | 2022-10-10 | Jason Kenney |
| UCP_Smith | 2022-10-11 | present | Danielle Smith |

## Political Era Distribution (Aggregated)

| Era | Agg Groups | Total Amount | Total Payments |
|-----|-----------|-------------|----------------|
| NDP | 248,715 | $110,659,925,694.42 | 613,279 |
| PC | 50,996 | $28,056,905,503.34 | 140,301 |
| UCP_Kenney | 201,951 | $81,640,865,397.10 | 512,909 |
| UCP_Smith | 130,019 | $67,990,457,261.02 | 290,263 |
| UNKNOWN | 71,248 | $30,546,679,796.14 | 243,296 |

## Data Quality Notes

- NULL/blank Ministry rows excluded: **6**
- NULL/blank PaymentDate rows excluded: **6,160**
- Entity mapping match rate: **701,864 / 702,930** (99.8%)
- Aggregation performed server-side on Databricks SQL Warehouse (1.8M rows never pulled locally)
- Amount column is stored as STRING in source; CAST to DOUBLE for aggregation
- PaymentDate stored as STRING; CAST to DATE for era assignment
- 1 unmatched ministry name variants found (see log for details)

## Output Files

| File | Rows | Description |
|------|------|-------------|
| `grants_aggregated.csv` | 702,930 | Org x Ministry x FY x Era (main output) |
| `goa_cra_matched.csv` | 1,304 | Gold standard GOA-CRA name matches |
| `entity_mapping.csv` | 142 | Ministry entity mapping (from org_entities.csv) |
| `transform_events.csv` | 66 | Ministry restructuring events |

## grants_aggregated.csv Schema

| Column | Description |
|--------|-------------|
| `recipient` | GOA grant recipient name |
| `ministry` | Granting ministry name |
| `fiscal_year` | Fiscal year (e.g. 2014) |
| `political_era` | PC / NDP / UCP_Kenney / UCP_Smith |
| `total_amount` | Sum of grant amounts (DOUBLE) |
| `n_payments` | Count of individual payment records |
| `earliest_payment` | First payment date in group |
| `latest_payment` | Last payment date in group |
| `canonical_ministry_id` | Joined from org_entities (e.g. EM-001) |

## Execution Log

```
[19:49:05] ======================================================================
[19:49:05] Agent 0A (Grant-Ministry Linker) starting
[19:49:05] Output directory: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly
[19:49:05] ======================================================================
[19:49:06] Databricks connection established
[19:49:06] 
[19:49:06] --- STEP 1: Verify table schemas ---
[19:49:06] Running: DESCRIBE goa_grants_disclosure
[19:49:06]   SQL: DESCRIBE dbw_unitycatalog_test.default.goa_grants_disclosure
[19:49:07]   Returned 11 rows, 3 cols in 0.5s
[19:49:07] goa_grants_disclosure columns:
[19:49:07]   Ministry                                 string
[19:49:07]   BUName                                   string
[19:49:07]   Recipient                                string
[19:49:07]   Program                                  string
[19:49:07]   Amount                                   string
[19:49:07]   Lottery                                  string
[19:49:07]   PaymentDate                              string
[19:49:07]   FiscalYear                               string
[19:49:07]   DisplayFiscalYear                        string
[19:49:07]   Fiscal_Year                              string
[19:49:07]   _rescued_data                            string
[19:49:07] Running: DESCRIBE goa_cra_matched
[19:49:07]   SQL: DESCRIBE dbw_unitycatalog_test.default.goa_cra_matched
[19:49:07]   Returned 19 rows, 3 cols in 0.3s
[19:49:07] goa_cra_matched columns:
[19:49:07]   goa_name                                 string
[19:49:07]   n_ministries                             bigint
[19:49:07]   goa_total                                double
[19:49:07]   n_grants                                 bigint
[19:49:07]   ministries                               array<string>
[19:49:07]   bn                                       string
[19:49:07]   cra_name                                 string
[19:49:07]   Total_Revenue                            double
[19:49:07]   total_gov_rev                            double
[19:49:07]   gov_dependency_pct                       double
[19:49:07]   program_pct                              double
[19:49:07]   compensation_pct_of_exp                  double
[19:49:07]   flag_low_passthrough                     int
[19:49:07]   flag_salary_mill                         int
[19:49:07]   flag_high_gov_dependency                 int
[19:49:07]   flag_deficit                             int
[19:49:07]   flag_insolvency_5pct_cut                 int
[19:49:07]   flag_shadow_network                      int
[19:49:07]   flag_in_director_cluster                 int
[19:49:07] Running: COUNT grants
[19:49:07]   SQL: SELECT COUNT(*) as cnt FROM dbw_unitycatalog_test.default.goa_grants_disclosure
[19:49:07]   Returned 1 rows, 1 cols in 0.2s
[19:49:07] Total grants rows: 1,806,214
[19:49:07] 
[19:49:07] --- STEP 2: Read Ministry Data from Databricks Volumes ---
[19:49:07] Running: org_entities.csv from Volume
[19:49:07]   SQL: SELECT * FROM read_files('/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/org_entities.csv', format => 'csv', header => true)
[19:49:08]   Returned 142 rows, 11 cols in 0.9s
[19:49:08] org_entities: 142 rows, columns: ['canonical_id', 'name', 'level', 'status', 'start_date', 'end_date', 'normalized_name', 'aliases', 'jurisdiction', 'kgl_sequence', '_rescued_data']
[19:49:08] Running: transform_events.csv from Volume
[19:49:08]   SQL: SELECT * FROM read_files('/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/transform_events.csv', format => 'csv', header => true)
[19:49:09]   Returned 66 rows, 10 cols in 0.8s
[19:49:09] transform_events: 66 rows, columns: ['event_id', 'event_type', 'event_date', 'effective_fy', 'confidence', 'evidence_basis', 'political_context', 'notes', 'kgl_sequence', '_rescued_data']
[19:49:09] Saved entity_mapping.csv (142 rows) [source: org_entities.csv]
[19:49:09] Saved transform_events.csv (66 rows)
[19:49:09] 
[19:49:09] --- STEP 3: Count NULL/blank ministry rows ---
[19:49:09] Running: Count NULL/blank Ministry rows
[19:49:09]   SQL: SELECT COUNT(*) as cnt         FROM dbw_unitycatalog_test.default.goa_grants_disclosure         WHERE Ministry IS NULL OR TRIM(Ministry) = ''
[19:49:10]   Returned 1 rows, 1 cols in 0.9s
[19:49:10] NULL/blank Ministry rows: 6 (will be EXCLUDED from aggregation)
[19:49:10] Running: Count NULL/blank PaymentDate rows
[19:49:10]   SQL: SELECT COUNT(*) as cnt         FROM dbw_unitycatalog_test.default.goa_grants_disclosure         WHERE PaymentDate IS NULL OR TRIM(PaymentDate) = ''
[19:49:10]   Returned 1 rows, 1 cols in 0.4s
[19:49:10] NULL/blank PaymentDate rows: 6,160 (will be EXCLUDED from aggregation)
[19:49:10] 
[19:49:10] --- STEP 4: Main aggregation (server-side on Databricks) ---
[19:49:10] Columns: Ministry, Recipient, FiscalYear, Amount(str->double), PaymentDate(str->date)
[19:49:10] Running: Main grants aggregation (server-side)
[19:49:10]   SQL: SELECT         Recipient                           AS recipient,         Ministry                            AS ministry,         FiscalYear                          AS fiscal_year,         CASE      ...
[19:49:56]   Returned 702,930 rows, 8 cols in 45.3s
[19:49:56] Aggregated result: 702,930 rows (Org x Ministry x FY x Era)
[19:49:56] 
[19:49:56] --- STEP 5: Political era distribution ---
[19:49:56] Political era distribution (aggregated groups):
[19:49:56]   Era             |     Groups |       Total Amount |     Payments
[19:49:56]   ----------------+------------+--------------------+-------------
[19:49:56]   NDP             |    248,715 | $110,659,925,694.42 |      613,279
[19:49:56]   PC              |     50,996 | $28,056,905,503.34 |      140,301
[19:49:56]   UCP_Kenney      |    201,951 | $81,640,865,397.10 |      512,909
[19:49:56]   UCP_Smith       |    130,019 | $67,990,457,261.02 |      290,263
[19:49:56]   UNKNOWN         |     71,248 | $30,546,679,796.14 |      243,296
[19:49:56] 
[19:49:56] --- STEP 6: Join with org_entities for canonical_ministry_id ---
[19:49:56] org_entities columns: ['canonical_id', 'name', 'level', 'status', 'start_date', 'end_date', 'normalized_name', 'aliases', 'jurisdiction', 'kgl_sequence', '_rescued_data']
[19:49:56] Built ministry name lookup: 150 name variants -> canonical_id
[19:49:56] Ministry name match: 701,864 / 702,930 rows (99.8%)
[19:49:56] Top unmatched ministry names (1 shown):
[19:49:56]   CULTURE,MULTICULTURALISMANDSTATUSOFWOMEN: 1,066 groups
[19:49:56] 
[19:49:56] --- STEP 7: Save grants_aggregated.csv ---
[19:49:58] Saved grants_aggregated.csv (702,930 rows)
[19:49:58]   Columns: ['recipient', 'ministry', 'fiscal_year', 'political_era', 'total_amount', 'n_payments', 'earliest_payment', 'latest_payment', 'canonical_ministry_id']
[19:49:58] 
[19:49:58] --- STEP 8: Pull goa_cra_matched (gold standard org matching) ---
[19:49:58] Running: goa_cra_matched full pull
[19:49:58]   SQL: SELECT * FROM dbw_unitycatalog_test.default.goa_cra_matched
[19:49:59]   Returned 1,304 rows, 19 cols in 0.9s
[19:49:59] goa_cra_matched: 1,304 rows
[19:49:59]   Columns: ['goa_name', 'n_ministries', 'goa_total', 'n_grants', 'ministries', 'bn', 'cra_name', 'Total_Revenue', 'total_gov_rev', 'gov_dependency_pct', 'program_pct', 'compensation_pct_of_exp', 'flag_low_passthrough', 'flag_salary_mill', 'flag_high_gov_dependency', 'flag_deficit', 'flag_insolvency_5pct_cut', 'flag_shadow_network', 'flag_in_director_cluster']
[19:49:59]   Converted 'ministries' array column to pipe-delimited string
[19:49:59] Saved goa_cra_matched.csv (1,304 rows)
[19:49:59] Databricks connection closed
[19:49:59] 
[19:49:59] --- STEP 10: Writing grant_assembly_log.md ---
```
