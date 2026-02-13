# Federal Grants Ingestion Log

**Generated:** 2026-02-12 23:30:14
**Post-cleanup:** 2026-02-12 23:33:00 (removed 'None' FederalDepartment node + stale edges)

## Summary

| Metric | Value |
|--------|-------|
| Source file | `federal_grants.csv` |
| Total CSV rows | 109,583 |
| Rows with BN | 52,349 (47.8%) |
| Rows with BN + valid department | 45,856 |
| Rows with BN + valid dept + valid FY | 45,856 |
| Unique BNs in CSV | 19,716 |
| BNs matching existing Organization nodes | 1,348 |
| Aggregated edges (BN x dept x FY) | 3,783 |
| FederalDepartment nodes created | 8 |
| **FUNDED_BY_FED relationships (final)** | **6,470** |
| Dual-funded orgs (GOA + Federal) | 1,666 |
| Federal-only orgs | 579 |
| GOA-only orgs | 2,357 |
| Risk-flagged orgs with federal funding | 1,502 |
| Matched funding amount | $626,402,730.57 |
| Cleaning decisions addressed | C14, DQ9 |

## Ingestion Log

```
[23:27:00] ========================================================================
[23:27:00] FEDERAL GRANTS INGESTION -- START
[23:27:00]   Addresses: C14 (federal grants not ingested), DQ9
[23:27:00] ========================================================================
[23:27:00]
[23:27:00] -- STEP 1: Load and Filter CSV --
[23:27:00]   Total rows loaded: 109583
[23:27:00]   Rows with BN: 52349 (47.8%)
[23:27:00]   Rows without BN (skipped): 57234
[23:27:00]   Rows with BN + valid department: 45856
[23:27:00]   Rows with BN but no/None department (skipped): 6493
[23:27:00]   Rows with BN + dept + valid fiscal_year: 45856
[23:27:00]   Rows with invalid fiscal_year (skipped): 0
[23:27:00]   Unique BNs: 19716
[23:27:00]   Unique departments: 49
[23:27:00]   Unique fiscal years: 2006-2007 through 2025-2026 (20 years)
[23:27:00]   Step 1 completed in 0.4s
[23:27:00]
[23:27:00] -- STEP 2: Aggregate by BN x Department x Fiscal Year --
[23:27:00]   Aggregated edges: 34371
[23:27:00]   (from 45856 raw rows)
[23:27:00]   Total funding amount: $16,469,711,100.51
[23:27:00]   Total grant count: 45856
[23:27:00]   Step 2 completed in 0.1s
[23:27:00]
[23:27:00] -- STEP 3: Connect to Neo4j / Check Existing Org Nodes --
[23:27:02]   Existing Organization nodes with BN: 9645
[23:27:02]   Aggregated edges matching existing Org nodes: 3783
[23:27:02]   BNs in CSV but NOT in graph: 18368
[23:27:02]   BNs in CSV that ARE in graph: 1348
[23:27:02]   Matched funding amount: $626,402,730.57
[23:27:02]   Matched grant count: 4676
[23:27:02]   Unique departments in matched set: 8
[23:27:02]   Step 3 completed in 2.0s
[23:27:02]
[23:27:02] -- STEP 4: Schema DDL --
[23:27:02]   CONSTRAINT fed_dept_name: OK
[23:27:02]   INDEX fed_dept_idx: OK
[23:27:02]   Step 4 completed in 0.2s
[23:27:02]
[23:27:02] -- STEP 5: MERGE FederalDepartment Nodes --
[23:27:02]   FederalDepartment nodes to MERGE: 8
[23:27:02]   VALIDATE: 8 FederalDepartment nodes in graph
[23:27:02]   Step 5 completed in 0.2s
[23:27:02]
[23:27:02] -- STEP 6: MERGE FUNDED_BY_FED Relationships --
[23:27:02]   FUNDED_BY_FED edges to MERGE: 3783
[23:30:13]   Merged 3783 FUNDED_BY_FED edges in 190.9s
[23:30:13]   Step 6 completed in 190.9s
[23:30:13]
[23:30:13] -- STEP 7: Verification (post-cleanup) --
[23:30:13]   Total FUNDED_BY_FED relationships: 6470
[23:30:13]   Total FederalDepartment nodes: 8
[23:30:14]   Edges to 'None' department: 0

Federal departments by funded organizations:
  Employment and Social Development Canada: 2222 orgs, 6362 edges, $1,358,105,016
  Public Safety Canada: 23 orgs, 24 edges, $5,405,718
  Royal Canadian Mounted Police: 22 orgs, 29 edges, $216,730
  National Defence: 18 orgs, 18 edges, $19,896,669
  Immigration, Refugees and Citizenship Canada: 10 orgs, 25 edges, $392,697,593
  Library and Archives Canada: 6 orgs, 6 edges, $300,000
  Veterans Affairs Canada: 5 orgs, 5 edges, $508,000
  Department of Justice Canada: 1 orgs, 1 edges, $0

Dual-funded organizations (GOA + Federal): 1666
Federal-only organizations: 579
GOA-only organizations: 2357

Top dual-funded orgs:
  CALGARY HOMELESS FOUNDATION (BN 880846829RR0001): $105,609,517 fed total
  EDMONTON IMMIGRANT SERVICES ASSOCIATION (BN 123161432RR0001): $104,452,776 fed total
  CALGARY CATHOLIC IMMIGRATION SOCIETY (BN 118823244RR0001): $40,350,536 fed total
  THE GOVERNORS OF THE UNIVERSITY OF CALGARY (BN 108102864RR0001): $39,999,532 fed total

FUNDED_BY_FED by fiscal year:
  2016-2017: 11 edges
  2017-2018: 1 edges
  2018-2019: 997 edges
  2019-2020: 1056 edges
  2020-2021: 977 edges
  2021-2022: 271 edges
  2022-2023: 959 edges
  2023-2024: 758 edges
  2024-2025: 796 edges
  2025-2026: 644 edges

Risk-flagged orgs receiving federal funding: 1502

Total elapsed time: 194.8s (3.2 min)
[23:30:14] ========================================================================
[23:30:14] FEDERAL GRANTS INGESTION -- COMPLETE
[23:30:14] ========================================================================
```

## Post-Ingestion Cleanup

- Deleted `FederalDepartment {name: 'None'}` node and 1,022 associated FUNDED_BY_FED edges
  (caused by Python `None` stringified to `"None"` in source CSV)
- Deleted additional 11 stale edges from prior run
- Final clean state: 6,470 FUNDED_BY_FED edges, 8 FederalDepartment nodes
