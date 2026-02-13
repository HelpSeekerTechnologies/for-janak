# Agent 0B - Director Network Assembly Log

**Run timestamp:** 2026-02-13 02:46:34 UTC

**Agent:** 0B (Director Network Builder)

**Operation:** Lineage Audit

---

## Raw Output

```
Agent 0B — Director Network Builder
Run started: 2026-02-13 02:46:34 UTC
Output directory: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly

Connected to Databricks SQL warehouse.

============================================================
STEP 1: Explore table schemas (DESCRIBE)
============================================================

--- DESCRIBE dbw_unitycatalog_test.default.multi_board_directors ---
  clean_name_no_initial                     string
  n_boards                                  bigint
  linked_bns                                array<string>
  n_non_arms_length                         bigint
  earliest_start                            date
  latest_start                              date

--- DESCRIBE dbw_unitycatalog_test.default.org_clusters_strong ---
  bn                                        string
  cluster_id                                int
  cluster_size                              int

--- DESCRIBE dbw_unitycatalog_test.default.ab_org_risk_flags ---
  bn                                        string
  Legal_name                                string
  Account_name                              string
  City                                      string
  Category_English_Desc                     string
  Total_Revenue                             double
  Total_Expenditures                        double
  fed_rev                                   double
  prov_rev                                  double
  muni_rev                                  double
  other_gov_rev                             double
  total_gov_rev                             double
  program_exp                               double
  admin_exp                                 double
  fundraising_exp                           double
  compensation                              double
  gifts_to_donees                           double
  Total_Assets                              double
  Total_Liabilities                         double
  net_assets                                double
  program_pct                               double
  admin_pct                                 double
  fundraising_pct                           double
  compensation_pct_of_exp                   double
  gov_dependency_pct                        double
  prov_dependency_pct                       double
  fed_dependency_pct                        double
  flag_low_passthrough                      int
  flag_salary_mill                          int
  flag_high_gov_dependency                  int
  flag_deficit                              int
  deficit_amount                            double
  flag_insolvency_5pct_cut                  int
  flag_shadow_network                       int
  flag_in_director_cluster                  int

--- DESCRIBE dbw_unitycatalog_test.default.org_network_edges_filtered ---
  org1_bn                                   string
  org2_bn                                   string
  n_shared_directors                        bigint
  shared_director_names                     array<string>

--- DESCRIBE dbw_unitycatalog_test.default.ab_master_profile ---
  bn                                        string
  Legal_name                                string
  Account_name                              string
  City                                      string
  Category_English_Desc                     string
  Total_Revenue                             double
  Total_Expenditures                        double
  fed_rev                                   double
  prov_rev                                  double
  muni_rev                                  double
  other_gov_rev                             double
  total_gov_rev                             double
  program_exp                               double
  admin_exp                                 double
  fundraising_exp                           double
  compensation                              double
  gifts_to_donees                           double
  Total_Assets                              double
  Total_Liabilities                         double
  net_assets                                double
  program_pct                               double
  admin_pct                                 double
  fundraising_pct                           double
  compensation_pct_of_exp                   double
  gov_dependency_pct                        double
  prov_dependency_pct                       double
  fed_dependency_pct                        double
  flag_low_passthrough                      int
  flag_salary_mill                          int
  flag_high_gov_dependency                  int
  flag_deficit                              int
  deficit_amount                            double
  flag_insolvency_5pct_cut                  int
  flag_shadow_network                       int
  flag_in_director_cluster                  int
  goa_n_ministries                          bigint
  goa_total_2324                            double
  goa_n_grants                              bigint
  goa_ministries                            array<string>
  goa_total_all_years                       double
  goa_n_years                               bigint
  fed_total                                 double
  fed_n_agreements                          bigint
  fed_n_depts                               bigint
  n_name_changes                            bigint
  total_cra_flags                           int
  flag_multi_ministry                       int
  flag_name_changed                         int
  total_all_flags                           int

============================================================
STEP 2: Pull multi_board_directors
  Table: dbw_unitycatalog_test.default.multi_board_directors
  Description: Directors sitting on 3+ charity boards
  Expected rows: ~19,156
============================================================
  Pulled 19,156 rows in 1.4s
  Saved to: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly\multi_board_directors.csv
  Columns: ['clean_name_no_initial', 'n_boards', 'linked_bns', 'n_non_arms_length', 'earliest_start', 'latest_start']
  Shape: (19156, 6)

============================================================
STEP 3: Pull org_clusters_strong
  Table: dbw_unitycatalog_test.default.org_clusters_strong
  Description: Pre-computed governance clusters
  Expected rows: ~4,636
============================================================
  Pulled 4,636 rows in 0.3s
  Saved to: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly\org_clusters.csv
  Columns: ['bn', 'cluster_id', 'cluster_size']
  Shape: (4636, 3)

============================================================
STEP 4: Pull ab_org_risk_flags
  Table: dbw_unitycatalog_test.default.ab_org_risk_flags
  Description: Organizations with risk flags
  Expected rows: ~9,145
============================================================
  Pulled 9,145 rows in 2.1s
  Saved to: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly\org_risk_flags.csv
  Columns: ['bn', 'Legal_name', 'Account_name', 'City', 'Category_English_Desc', 'Total_Revenue', 'Total_Expenditures', 'fed_rev', 'prov_rev', 'muni_rev', 'other_gov_rev', 'total_gov_rev', 'program_exp', 'admin_exp', 'fundraising_exp', 'compensation', 'gifts_to_donees', 'Total_Assets', 'Total_Liabilities', 'net_assets', 'program_pct', 'admin_pct', 'fundraising_pct', 'compensation_pct_of_exp', 'gov_dependency_pct', 'prov_dependency_pct', 'fed_dependency_pct', 'flag_low_passthrough', 'flag_salary_mill', 'flag_high_gov_dependency', 'flag_deficit', 'deficit_amount', 'flag_insolvency_5pct_cut', 'flag_shadow_network', 'flag_in_director_cluster']
  Shape: (9145, 35)

============================================================
STEP 5: Pull org_network_edges_filtered
  Table: dbw_unitycatalog_test.default.org_network_edges_filtered
  Description: Org-to-org shared director edges
  Expected rows: ~154,015
============================================================
  Pulled 154,015 rows in 5.3s
  Saved to: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly\org_network_edges.csv
  Columns: ['org1_bn', 'org2_bn', 'n_shared_directors', 'shared_director_names']
  Shape: (154015, 4)

Databricks connection closed.

============================================================
STEP 6: Summary Statistics
============================================================

--- Multi-Board Directors ---
  Total rows: 19,156
  Unique directors: 19,156  (column: clean_name_no_initial)
  Could not identify an org column. Columns: ['clean_name_no_initial', 'n_boards', 'linked_bns', 'n_non_arms_length', 'earliest_start', 'latest_start']
  Boards-per-director (n_boards):
    min=3, max=357, median=3.0, mean=4.38

--- Governance Clusters ---
  Total rows: 4,636
  Columns: ['bn', 'cluster_id', 'cluster_size']
  Unique clusters: 1,540  (column: cluster_id)
  Cluster size distribution:
    min=2, max=61, median=2.0, mean=3.01

  Top 10 largest clusters by member count:
    #1: cluster 0 — 61 members
    #2: cluster 1 — 42 members
    #3: cluster 2 — 33 members
    #4: cluster 3 — 33 members
    #5: cluster 4 — 31 members
    #6: cluster 5 — 30 members
    #7: cluster 6 — 28 members
    #8: cluster 7 — 26 members
    #9: cluster 8 — 25 members
    #10: cluster 9 — 22 members
  Unique organizations in clusters: 4,636

--- Risk Flags ---
  Total rows: 9,145
  Columns: ['bn', 'Legal_name', 'Account_name', 'City', 'Category_English_Desc', 'Total_Revenue', 'Total_Expenditures', 'fed_rev', 'prov_rev', 'muni_rev', 'other_gov_rev', 'total_gov_rev', 'program_exp', 'admin_exp', 'fundraising_exp', 'compensation', 'gifts_to_donees', 'Total_Assets', 'Total_Liabilities', 'net_assets', 'program_pct', 'admin_pct', 'fundraising_pct', 'compensation_pct_of_exp', 'gov_dependency_pct', 'prov_dependency_pct', 'fed_dependency_pct', 'flag_low_passthrough', 'flag_salary_mill', 'flag_high_gov_dependency', 'flag_deficit', 'deficit_amount', 'flag_insolvency_5pct_cut', 'flag_shadow_network', 'flag_in_director_cluster']
  Risk flag columns detected (boolean pattern):
    flag_low_passthrough: 3,864 flagged
    flag_salary_mill: 328 flagged
    flag_high_gov_dependency: 644 flagged
    flag_deficit: 3,624 flagged
    flag_insolvency_5pct_cut: 314 flagged
    flag_shadow_network: 23 flagged
    flag_in_director_cluster: 419 flagged
  Unique organizations with risk flags: 9,145

--- Network Edges ---
  Total edges: 154,015
  Columns: ['org1_bn', 'org2_bn', 'n_shared_directors', 'shared_director_names']
  Unique organizations in edges: 41,801
  Source column: org1_bn, Target column: org2_bn

============================================================
GRAND TOTALS
============================================================
  Unique directors (multi-board): 19,156
  Unique organizations (union across all tables): 13,362

  Table row counts:
    multi_board_directors                         19,156 rows  (expected ~19,156, delta +0)
    org_clusters_strong                            4,636 rows  (expected ~4,636, delta +0)
    ab_org_risk_flags                              9,145 rows  (expected ~9,145, delta +0)
    org_network_edges_filtered                   154,015 rows  (expected ~154,015, delta +0)

Writing assembly log to: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly\director_assembly_log.md
```

---

## Output Files

| File | Rows | Description |
|------|------|-------------|
| `multi_board_directors.csv` | 19,156 | Directors sitting on 3+ charity boards |
| `org_clusters.csv` | 4,636 | Pre-computed governance clusters |
| `org_risk_flags.csv` | 9,145 | Organizations with risk flags |
| `org_network_edges.csv` | 154,015 | Org-to-org shared director edges |
| `director_assembly_log.md` | - | This file |

## Column Inventories

### multi_board_directors.csv

| Column | Dtype | Non-Null | Unique |
|--------|-------|----------|--------|
| `clean_name_no_initial` | object | 19,156 | 19,156 |
| `n_boards` | int64 | 19,156 | 64 |
| `linked_bns` | object | 19,156 | 19,007 |
| `n_non_arms_length` | int64 | 19,156 | 26 |
| `earliest_start` | object | 19,047 | 5,970 |
| `latest_start` | object | 19,047 | 2,858 |

### org_clusters.csv

| Column | Dtype | Non-Null | Unique |
|--------|-------|----------|--------|
| `bn` | object | 4,636 | 4,636 |
| `cluster_id` | int64 | 4,636 | 1,540 |
| `cluster_size` | int64 | 4,636 | 26 |

### org_risk_flags.csv

| Column | Dtype | Non-Null | Unique |
|--------|-------|----------|--------|
| `bn` | object | 9,145 | 9,145 |
| `Legal_name` | object | 9,145 | 8,788 |
| `Account_name` | object | 9,145 | 9,012 |
| `City` | object | 9,145 | 494 |
| `Category_English_Desc` | object | 9,145 | 31 |
| `Total_Revenue` | float64 | 9,145 | 8,548 |
| `Total_Expenditures` | float64 | 9,145 | 8,528 |
| `fed_rev` | float64 | 9,145 | 1,297 |
| `prov_rev` | float64 | 9,145 | 1,953 |
| `muni_rev` | float64 | 9,145 | 1,106 |
| `other_gov_rev` | float64 | 9,145 | 450 |
| `total_gov_rev` | float64 | 9,145 | 3,045 |
| `program_exp` | float64 | 9,145 | 6,561 |
| `admin_exp` | float64 | 9,145 | 5,257 |
| `fundraising_exp` | float64 | 9,145 | 1,826 |
| `compensation` | float64 | 9,145 | 4,066 |
| `gifts_to_donees` | float64 | 9,145 | 2,608 |
| `Total_Assets` | float64 | 9,145 | 8,363 |
| `Total_Liabilities` | float64 | 9,145 | 5,384 |
| `net_assets` | float64 | 9,145 | 8,181 |
| `program_pct` | float64 | 8,676 | 1,639 |
| `admin_pct` | float64 | 8,676 | 861 |
| `fundraising_pct` | float64 | 8,676 | 337 |
| `compensation_pct_of_exp` | float64 | 8,647 | 883 |
| `gov_dependency_pct` | float64 | 8,676 | 939 |
| `prov_dependency_pct` | float64 | 8,676 | 787 |
| `fed_dependency_pct` | float64 | 8,676 | 413 |
| `flag_low_passthrough` | int64 | 9,145 | 2 |
| `flag_salary_mill` | int64 | 9,145 | 2 |
| `flag_high_gov_dependency` | int64 | 9,145 | 2 |
| `flag_deficit` | int64 | 9,145 | 2 |
| `deficit_amount` | float64 | 9,145 | 8,487 |
| `flag_insolvency_5pct_cut` | int64 | 9,145 | 2 |
| `flag_shadow_network` | int64 | 9,145 | 2 |
| `flag_in_director_cluster` | int64 | 9,145 | 2 |

### org_network_edges.csv

| Column | Dtype | Non-Null | Unique |
|--------|-------|----------|--------|
| `org1_bn` | object | 154,015 | 33,670 |
| `org2_bn` | object | 154,015 | 34,322 |
| `n_shared_directors` | int64 | 154,015 | 28 |
| `shared_director_names` | object | 154,015 | 20,865 |

