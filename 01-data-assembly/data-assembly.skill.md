# Data Assembly Skill

Prepare and enrich source datasets for graph ingestion. Tag grants with political eras, build director networks, and pull federal grants. All data sourced from Databricks (D009). All outputs are CSV files ready for Neo4j MERGE operations.

---

## When to Use

- Tagging GOA grants with political era (NDP/UCP-Kenney/UCP-Smith)
- Building director→organization bipartite networks from CRA T3010
- Pulling and linking federal Grants & Contributions data

---

## Databricks Connection Pattern

All data is sourced from Databricks (D009). Use the `databricks-sdk` for table queries and Volume file reads.

```python
from databricks import sql as dbsql

DATABRICKS_HOST = "<YOUR_DATABRICKS_HOST>"
DATABRICKS_TOKEN = "<YOUR_DATABRICKS_TOKEN>"
DATABRICKS_WAREHOUSE = "<YOUR_DATABRICKS_SQL_WAREHOUSE>"
CATALOG = "dbw_unitycatalog_test"

def get_databricks_connection():
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_WAREHOUSE,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
        schema="default"
    )

def query_table(table_name, limit=None):
    """Query a Databricks table and return as pandas DataFrame."""
    conn = get_databricks_connection()
    sql = f"SELECT * FROM {CATALOG}.default.{table_name}"
    if limit:
        sql += f" LIMIT {limit}"
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

# Example queries:
# grants = query_table("goa_grants_disclosure")         # 1,806,214 rows
# directors = query_table("cra_directors_clean")         # 570,798 rows
# clusters = query_table("org_clusters_strong")          # 4,636 rows
# risk_flags = query_table("ab_org_risk_flags")          # 9,145 rows
# multi_board = query_table("multi_board_directors")     # 19,156 rows
# network_edges = query_table("org_network_edges_filtered")  # 154,015 rows
```

---

## Section 1: Grant Political Era Tagging

### Input Sources (Databricks — D009)
- Databricks table: `goa_grants_disclosure` (1,806,214 rows)
- Databricks Volume: `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/entity_mapping.csv` (318 rows)
- Databricks Volume: `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/transform_events.csv` (54 rows)

```sql
-- GOA grants from Databricks
SELECT * FROM dbw_unitycatalog_test.default.goa_grants_disclosure;

-- Or for pre-matched GOA-CRA records
SELECT * FROM dbw_unitycatalog_test.default.goa_cra_matched;
```

### Political Era Boundaries (per D002)
```python
POLITICAL_ERAS = {
    'PC': ('1900-01-01', '2015-05-23'),        # Pre-NDP (Progressive Conservatives)
    'NDP': ('2015-05-24', '2019-04-29'),        # Rachel Notley
    'UCP_Kenney': ('2019-04-30', '2022-10-10'), # Jason Kenney
    'UCP_Smith': ('2022-10-11', '2099-12-31'),  # Danielle Smith
}
```

### Era Assignment Logic
```python
def assign_era(payment_date_str):
    """Assign political era based on payment date."""
    dt = pd.to_datetime(payment_date_str)
    for era, (start, end) in POLITICAL_ERAS.items():
        if pd.Timestamp(start) <= dt <= pd.Timestamp(end):
            return era
    return 'UNKNOWN'
```

### Ministry Restructuring Attribution
For each ministry in entity_mapping.csv, determine which political era restructured it:
1. Join transform_events.csv on canonical_id
2. Look at event_date — which era does it fall in?
3. Tag: `ministry_restructured_by = assign_era(event_date)`

### Aggregation (per D003)
```python
# Aggregate to Organization × Ministry × FiscalYear
agg = grants.groupby(['Recipient', 'Ministry', 'FiscalYear']).agg(
    amount=('Amount', 'sum'),
    n_payments=('Amount', 'count'),
    political_era=('political_era', 'first')  # era of first payment in group
).reset_index()
```

### Output Schema
```
Recipient, Ministry, FiscalYear, amount, n_payments, political_era,
canonical_ministry_id, current_ministry_name, ministry_restructured_by,
ndp_era_total, ucp_era_total, delta_pct
```

### Known Issues
- `CULTURE,MULTICULTURALISMANDSTATUSOFWOMEN` — use alias lookup
- Blank Ministry rows — skip (summary/total rows)
- PaymentDate format varies across years — parse flexibly

---

## Section 2: Director Network Construction

### Input Sources (Databricks — D009)
- Databricks table: `cra_directors_clean` (570,798 rows — normalized director records)
- Databricks table: `multi_board_directors` (19,156 rows — directors on 3+ boards)
- Databricks table: `org_clusters_strong` (4,636 rows — cluster assignments)
- Databricks table: `org_network_edges_filtered` (154,015 rows — org-to-org edges)
- Databricks table: `ab_org_risk_flags` (9,145 rows — risk flags)

```sql
-- Multi-board directors
SELECT * FROM dbw_unitycatalog_test.default.multi_board_directors;

-- Pre-computed strong clusters
SELECT * FROM dbw_unitycatalog_test.default.org_clusters_strong;

-- Org-to-org network edges
SELECT * FROM dbw_unitycatalog_test.default.org_network_edges_filtered;
```

### Director Name Normalization
```python
def normalize_director_name(last, first):
    """Normalize for matching across CRA datasets."""
    name = f"{last.strip().upper()}, {first.strip().upper()}"
    # Remove titles
    for title in ['DR.', 'REV.', 'HON.', 'MR.', 'MRS.', 'MS.', 'PROF.']:
        name = name.replace(title, '').strip()
    return name
```

### Network Construction
```python
# 1. Filter to Alberta (BN province = AB or org in AB charities list)
# 2. Build director→org edges (BN + normalized_name)
# 3. Project to org→org edges where shared_directors >= 2
# 4. Connected components → cluster IDs
# 5. Per cluster: aggregate funding, flags
```

### Output Files
- `director_org_network.csv`: director_name, BN, org_name, position, n_boards
- `governance_clusters.csv`: cluster_id, size, org_names, total_goa_funding, total_flags, ndp_era_funding

---

## Section 3: Federal Grants Enrichment

### Input Sources (Databricks — D009)
- Databricks Volume: `/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/GoC Grants/`

### Matching
Primary: BN number (exact match to CRA T3010)
Secondary: Recipient legal name (fuzzy)

### Output Schema
```
BN, org_name, federal_department, program, amount, fiscal_year, province
```

---

## Anti-Patterns

1. **Don't read local CSV files** — all data comes from Databricks (D009)
2. **Don't load individual grant payment rows into Neo4j** — aggregate first (D003)
3. **Don't use CRA 2023 financial data** — use CRA 2022 (2023 only ~45% complete)
4. **Don't skip blank Ministry rows silently** — log the count for audit trail
