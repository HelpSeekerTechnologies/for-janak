# Agent 0D -- Federal Grants Enrichment Log

**Generated:** 2026-02-12 20:03:00

**Agent:** 0D (Federal Grants Enrichment)
**Operation:** Lineage Audit
**Data Source:** Databricks Volume `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/`

---

## Summary

| Metric | Value |
|--------|-------|
| Total GoC Grants rows (all provinces) | 1,811,088 |
| Alberta rows extracted | 109,583 |
| Unique organizations | 37,860 |
| Unique federal departments | 169 |
| Unique programs | 1,623 |
| Total amount (Alberta) | $65,741,044,059.24 |
| BN populated (exact match ready) | 52,349 (47.8%) |
| BN missing (fuzzy match needed) | 57,234 (52.2%) |
| Fiscal year range | 0 to Glacier2Ocean Watch: Community-led environmental monitoring in Aujuittuq (Grise Fiord) |

---

## File Inventory

| File | Size | Description |
|------|------|-------------|
| grants.csv | ~2.2 GB | GoC Proactive Disclosure of Grants & Contributions |

---

## Column Mapping

| Output Column | Source Column (GoC) |
|---------------|--------------------|
| BN | recipient_business_number |
| org_name | recipient_legal_name |
| federal_department | owner_org_title |
| program | prog_name_en |
| amount | agreement_value |
| fiscal_year | agreement_start_date (derived Apr-Mar FY) |
| province | recipient_province |

---

## Execution Log

```
[2026-02-12 20:01:15] [INFO] ============================================================
[2026-02-12 20:01:15] [INFO] AGENT 0D — Federal Grants Enrichment
[2026-02-12 20:01:15] [INFO] Operation Lineage Audit
[2026-02-12 20:01:15] [INFO] ============================================================
[2026-02-12 20:01:15] [INFO] Timestamp: 2026-02-12T20:01:15.243147
[2026-02-12 20:01:15] [INFO] Volume path: /Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/
[2026-02-12 20:01:15] [INFO] Output dir: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly
[2026-02-12 20:01:15] [INFO] 
[2026-02-12 20:01:15] [INFO] Connecting to Databricks SQL warehouse...
[2026-02-12 20:01:16] [INFO]   Connected successfully.
[2026-02-12 20:01:16] [INFO] Checking total row count in GoC Grants file...
[2026-02-12 20:01:16] [INFO] Executing COUNT total rows: SELECT COUNT(*) as cnt FROM read_files('/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/grants.csv', format => 'csv', header => true)...
[2026-02-12 20:01:21] [INFO]   -> returned 1 rows, 1 columns
[2026-02-12 20:01:21] [INFO]   Total rows in GoC grants file: 1,811,088
[2026-02-12 20:01:21] [INFO] ============================================================
[2026-02-12 20:01:21] [INFO] STEP 1: Discover files in GoC Grants volume
[2026-02-12 20:01:21] [INFO] ============================================================
[2026-02-12 20:01:21] [INFO] Executing LIST volume directory: LIST '/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/'...
[2026-02-12 20:01:21] [INFO]   -> returned 1 rows
[2026-02-12 20:01:21] [INFO] LIST returned 1 entries:
[2026-02-12 20:01:21] [INFO]   - /Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/grants.csv  (size=2194832758)
[2026-02-12 20:01:21] [INFO] ============================================================
[2026-02-12 20:01:21] [INFO] STEP 2: Check for managed tables with federal grants data
[2026-02-12 20:01:21] [INFO] ============================================================
[2026-02-12 20:01:21] [INFO] Executing SHOW TABLES LIKE '%fed%': SHOW TABLES IN dbw_unitycatalog_test.default LIKE '%fed%'...
[2026-02-12 20:01:21] [INFO]   -> returned 0 rows, 3 columns
[2026-02-12 20:01:21] [INFO] Executing SHOW TABLES LIKE '%goc%': SHOW TABLES IN dbw_unitycatalog_test.default LIKE '%goc%'...
[2026-02-12 20:01:22] [INFO]   -> returned 0 rows, 3 columns
[2026-02-12 20:01:22] [INFO] Executing SHOW TABLES LIKE '%federal%': SHOW TABLES IN dbw_unitycatalog_test.default LIKE '%federal%'...
[2026-02-12 20:01:22] [INFO]   -> returned 0 rows, 3 columns
[2026-02-12 20:01:22] [INFO] Executing SHOW TABLES LIKE '%gc_%': SHOW TABLES IN dbw_unitycatalog_test.default LIKE '%gc_%'...
[2026-02-12 20:01:22] [INFO]   -> returned 0 rows, 3 columns
[2026-02-12 20:01:22] [INFO] Executing SHOW TABLES LIKE '%grants_contrib%': SHOW TABLES IN dbw_unitycatalog_test.default LIKE '%grants_contrib%'...
[2026-02-12 20:01:22] [INFO]   -> returned 0 rows, 3 columns
[2026-02-12 20:01:22] [INFO] Executing SHOW TABLES LIKE '%canada_grant%': SHOW TABLES IN dbw_unitycatalog_test.default LIKE '%canada_grant%'...
[2026-02-12 20:01:23] [INFO]   -> returned 0 rows, 3 columns
[2026-02-12 20:01:23] [INFO] Executing SHOW TABLES LIKE '%gov_canada%': SHOW TABLES IN dbw_unitycatalog_test.default LIKE '%gov_canada%'...
[2026-02-12 20:01:23] [INFO]   -> returned 0 rows, 3 columns
[2026-02-12 20:01:23] [INFO] No managed tables matching federal grant patterns found.
[2026-02-12 20:01:23] [INFO] ============================================================
[2026-02-12 20:01:23] [INFO] STEP 3: Read federal grants CSV files from volume
[2026-02-12 20:01:23] [INFO] ============================================================
[2026-02-12 20:01:23] [INFO] Attempting to read: /Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/grants.csv
[2026-02-12 20:01:23] [INFO] Executing SAMPLE CSV (read_files) grants.csv: SELECT * FROM read_files('/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/grants.csv', format => 'csv', header => true) LIMIT 5...
[2026-02-12 20:01:24] [INFO]   -> returned 5 rows, 39 columns
[2026-02-12 20:01:24] [INFO]   Schema detected via read_files: ['ref_number', 'amendment_number', 'amendment_date', 'agreement_type', 'recipient_type', 'recipient_business_number', 'recipient_legal_name', 'recipient_operating_name', 'research_organization_name', 'recipient_country', 'recipient_province', 'recipient_city', 'recipient_postal_code', 'federal_riding_name_en', 'federal_riding_name_fr', 'federal_riding_number', 'prog_name_en', 'prog_name_fr', 'prog_purpose_en', 'prog_purpose_fr', 'agreement_title_en', 'agreement_title_fr', 'agreement_number', 'agreement_value', 'foreign_currency_type', 'foreign_currency_value', 'agreement_start_date', 'agreement_end_date', 'coverage', 'description_en', 'description_fr', 'naics_identifier', 'expected_results_en', 'expected_results_fr', 'additional_information_en', 'additional_information_fr', 'owner_org', 'owner_org_title', '_rescued_data']
[2026-02-12 20:01:24] [INFO]   Province column found: 'recipient_province' — filtering at SQL level for AB
[2026-02-12 20:01:24] [INFO] Executing READ CSV (AB filtered) grants.csv: SELECT * FROM read_files('/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/grants.csv', format => 'csv', header => true) WHERE UPPER(TRIM(`recipient_province`)) IN ('AB', 'ALBERTA')...
[2026-02-12 20:02:31] [INFO]   -> returned 117525 rows, 39 columns
[2026-02-12 20:02:31] [INFO]   SUCCESS: 117525 rows x 39 cols from grants.csv
[2026-02-12 20:02:31] [INFO]   Columns: ['ref_number', 'amendment_number', 'amendment_date', 'agreement_type', 'recipient_type', 'recipient_business_number', 'recipient_legal_name', 'recipient_operating_name', 'research_organization_name', 'recipient_country', 'recipient_province', 'recipient_city', 'recipient_postal_code', 'federal_riding_name_en', 'federal_riding_name_fr', 'federal_riding_number', 'prog_name_en', 'prog_name_fr', 'prog_purpose_en', 'prog_purpose_fr', 'agreement_title_en', 'agreement_title_fr', 'agreement_number', 'agreement_value', 'foreign_currency_type', 'foreign_currency_value', 'agreement_start_date', 'agreement_end_date', 'coverage', 'description_en', 'description_fr', 'naics_identifier', 'expected_results_en', 'expected_results_fr', 'additional_information_en', 'additional_information_fr', 'owner_org', 'owner_org_title', '_rescued_data']
[2026-02-12 20:02:31] [INFO] Combined dataset: 117525 rows x 40 cols
[2026-02-12 20:02:31] [INFO] 
Raw data sample (first 3 rows):
[2026-02-12 20:02:31] [INFO]                  ref_number amendment_number amendment_date agreement_type recipient_type recipient_business_number              recipient_legal_name recipient_operating_name research_organization_name recipient_country recipient_province recipient_city recipient_postal_code federal_riding_name_en federal_riding_name_fr federal_riding_number                                                               prog_name_en                                                                              prog_name_fr                                                                                                                                                                                                                                        prog_purpose_en                                                                                                                                                                                                                                                                                        prog_purpose_fr                                  agreement_title_en                                                                       agreement_title_fr agreement_number agreement_value foreign_currency_type foreign_currency_value agreement_start_date agreement_end_date coverage                                                                                                                                                                                                                                         description_en                                                                                                                                                                                                                                                                                         description_fr naics_identifier expected_results_en expected_results_fr additional_information_en additional_information_fr owner_org                                                           owner_org_title _rescued_data _source_file
[2026-02-12 20:02:31] [INFO]   0  235-2018-2019-Q1-00027                0           None              C           None                      None               489316 Alberta Ltd.                     None                       None                CA                 AB     Bonnyville                T9N2M9                   None                   None                  None  Career Focus Program/Agricultural Youth Green Jobs Initiative/Green Farms  Programme Objectif carrière / Initiative de stage en Agroenvironnement/ Volet à la ferme                                                                                           The Youth Green Jobs Initiatives under the Green Farms Stream of the Career Focus Program helps an individual acquire experience in the agricultural sector.                                                                                                                    Le Volet à la ferme de l’Initiative de stage en Agroenvironnement du programme Objectif carrière aide les individus à acquérir de l’expérience de travail dans le secteur agricole.                                     Work Experience                                                                    Expérience de travail    GF1819-440-NW          1033.5                  None                   None           2018-07-03         2018-08-24     None                                                                                           The Youth Green Jobs Initiatives under the Green Farms Stream of the Career Focus Program helps an individual acquire experience in the agricultural sector.                                                                                                                    Le Volet à la ferme de l’Initiative de stage en Agroenvironnement du programme Objectif carrière aide les individus à acquérir de l’expérience de travail dans le secteur agricole.             None                None                None                      None                      None  aafc-aac  Agriculture and Agri-Food Canada | Agriculture et Agroalimentaire Canada          None   grants.csv
[2026-02-12 20:02:31] [INFO]   1  235-2018-2019-Q1-00003                0           None              C           None                      None  Canadian Cattlemen's Association                     None                       None                CA                 AB        Calgary                T2E7H7                   None                   None                  None                                                        AgriScience Program                                                                    Programme Agri-science  AgriScience Program Cluster or Project component accelerate the pace of innovation by providing funding and support for pre-commercial science activities and cutting-edge research that benefits the agriculture and agri-food sector and Canadians.  Programme Agri-science - volet Grappes ou Projet  d'accélérer le rythme des innovations, au moyen du financement et du soutien d'activités scientifiques de pré-commercialisation et de la recherche de pointe pour le bénéfice du secteur de l'agriculture et de l'agroalimentaire et des Canadiens.  Canada's Sustainable Beef & Forage Science Cluster  Grappe scientifique canadienne en production bovine et en cultures fourragères durables           ASC-01       8510311.0                  None                   None           2018-04-01         2023-03-31     None  AgriScience Program Cluster or Project component accelerate the pace of innovation by providing funding and support for pre-commercial science activities and cutting-edge research that benefits the agriculture and agri-food sector and Canadians.  Programme Agri-science - volet Grappes ou Projet  d'accélérer le rythme des innovations, au moyen du financement et du soutien d'activités scientifiques de pré-commercialisation et de la recherche de pointe pour le bénéfice du secteur de l'agriculture et de l'agroalimentaire et des Canadiens.             None                None                None                      None                      None  aafc-aac  Agriculture and Agri-Food Canada | Agriculture et Agroalimentaire Canada          None   grants.csv
[2026-02-12 20:02:31] [INFO]   2  235-2018-2019-Q1-00062                0           None              C           None                      None           Froese Bros. Farms Inc.                     None                       None                CA                 AB         Edberg                T0B1J0                   None                   None                  None  Career Focus Program/Agricultural Youth Green Jobs Initiative/Green Farms  Programme Objectif carrière / Initiative de stage en Agroenvironnement/ Volet à la ferme                                                                                           The Youth Green Jobs Initiatives under the Green Farms Stream of the Career Focus Program helps an individual acquire experience in the agricultural sector.                                                                                                                    Le Volet à la ferme de l’Initiative de stage en Agroenvironnement du programme Objectif carrière aide les individus à acquérir de l’expérience de travail dans le secteur agricole.                                     Work Experience                                                                    Expérience de travail    GF1819-436-NW         10000.0                  None                   None           2018-04-01         2018-11-01     None                                                                                           The Youth Green Jobs Initiatives under the Green Farms Stream of the Career Focus Program helps an individual acquire experience in the agricultural sector.                                                                                                                    Le Volet à la ferme de l’Initiative de stage en Agroenvironnement du programme Objectif carrière aide les individus à acquérir de l’expérience de travail dans le secteur agricole.             None                None                None                      None                      None  aafc-aac  Agriculture and Agri-Food Canada | Agriculture et Agroalimentaire Canada          None   grants.csv
[2026-02-12 20:02:31] [INFO] 
[2026-02-12 20:02:31] [INFO] Normalizing columns...
[2026-02-12 20:02:31] [INFO]   Raw columns: ['ref_number', 'amendment_number', 'amendment_date', 'agreement_type', 'recipient_type', 'recipient_business_number', 'recipient_legal_name', 'recipient_operating_name', 'research_organization_name', 'recipient_country', 'recipient_province', 'recipient_city', 'recipient_postal_code', 'federal_riding_name_en', 'federal_riding_name_fr', 'federal_riding_number', 'prog_name_en', 'prog_name_fr', 'prog_purpose_en', 'prog_purpose_fr', 'agreement_title_en', 'agreement_title_fr', 'agreement_number', 'agreement_value', 'foreign_currency_type', 'foreign_currency_value', 'agreement_start_date', 'agreement_end_date', 'coverage', 'description_en', 'description_fr', 'naics_identifier', 'expected_results_en', 'expected_results_fr', 'additional_information_en', 'additional_information_fr', 'owner_org', 'owner_org_title', '_rescued_data', '_source_file']
[2026-02-12 20:02:31] [INFO]   BN column: 'recipient_business_number'
[2026-02-12 20:02:31] [INFO]   org_name column: 'recipient_legal_name'
[2026-02-12 20:02:31] [INFO]   federal_department column: 'owner_org_title'
[2026-02-12 20:02:31] [INFO]   program column: 'prog_name_en'
[2026-02-12 20:02:31] [INFO]   amount column: 'agreement_value'
[2026-02-12 20:02:31] [INFO]   fiscal_year column: 'agreement_start_date'
[2026-02-12 20:02:31] [INFO]   province column: 'recipient_province'
[2026-02-12 20:02:31] [INFO]   Normalized: 117525 rows
[2026-02-12 20:02:31] [INFO] Data appears pre-filtered to Alberta at SQL level.
[2026-02-12 20:02:31] [INFO] ============================================================
[2026-02-12 20:02:31] [INFO] STEP 5: Clean and prepare matching keys
[2026-02-12 20:02:31] [INFO] ============================================================
[2026-02-12 20:02:32] [INFO]   BN populated: 56841 / 117525
[2026-02-12 20:02:32] [INFO]   BN empty: 60684 / 117525
[2026-02-12 20:02:32] [INFO]   Total federal grant amount (Alberta): $68,183,931,206.79
[2026-02-12 20:02:32] [INFO]   fiscal_year column contains date values — deriving fiscal year (Apr-Mar)...
[2026-02-12 20:02:59] [INFO]   Fiscal year sample after conversion: ['2018-2019', '2018-2019', '2018-2019', '2017-2018', '2018-2019']
[2026-02-12 20:02:59] [WARN]   NOTE: 17 rows had unparseable date values in fiscal_year
[2026-02-12 20:02:59] [INFO]   Exact BN match ready: 56841
[2026-02-12 20:02:59] [INFO]   Fuzzy match needed:   60684
[2026-02-12 20:03:00] [INFO]   Deduplication: 117525 -> 109583 (removed 7942 dupes)
[2026-02-12 20:03:00] [INFO] ============================================================
[2026-02-12 20:03:00] [INFO] STEP 6: Write output files
[2026-02-12 20:03:00] [INFO] ============================================================
[2026-02-12 20:03:00] [INFO]   Written: C:\Users\alina\OneDrive\Desktop\lineage-audit\01-data-assembly\federal_grants.csv
[2026-02-12 20:03:00] [INFO]   Rows: 109583
[2026-02-12 20:03:00] [INFO]   Columns: ['BN', 'org_name', 'federal_department', 'program', 'amount', 'fiscal_year', 'province']
[2026-02-12 20:03:00] [INFO]   Amount range: $-40,111,744.00 to $3,397,857,039.00
[2026-02-12 20:03:00] [INFO]   Unique orgs: 37860
[2026-02-12 20:03:00] [INFO]   Unique departments: 169
[2026-02-12 20:03:00] [INFO]   Unique programs: 1623
[2026-02-12 20:03:00] [INFO]   Fiscal years: ['0', '100000', '100000.0', '187500.0', '2005-2006', '2006-2007', '2007-2008', '2008-2009', '2009-2010', '2010-2011', '2011-2012', '2011-Q1-712', '2012-2013', '2013-2014', '2014-2015', '2015-2016', '2016-2017', '2017-2018', '2018-2019', '2019-2020']
```
