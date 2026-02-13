# Data Assembly Skill

Prepare and enrich source datasets for graph ingestion. Tag grants with political eras, build director networks, collect political donations, and pull federal grants. All outputs are CSV files ready for Neo4j MERGE operations.

---

## When to Use

- Tagging GOA grants with political era (NDP/UCP-Kenney/UCP-Smith)
- Building director→organization bipartite networks from CRA T3010
- Matching director names to Elections Alberta donation records
- Pulling and linking federal Grants & Contributions data

---

## Section 1: Grant Political Era Tagging

### Input Files
- `C:\Users\alina\OneDrive\Desktop\goa_grants_all.csv` (1,806,202 rows)
- `ministry-genealogy-graph/04-graph-build/databricks/entity_mapping.csv` (318 rows)
- `ministry-genealogy-graph/04-graph-build/databricks/transform_events.csv` (54 rows)

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

### Input Files
- `Janak Demo/Excploratory Analysis/data/cra/directors_2023.csv` (571,461 rows)
- `Janak Demo/Excploratory Analysis/data/super_directors.csv` (50 rows)
- `Janak Demo/Excploratory Analysis/data/clusters.csv` (380+ clusters)

### Director Name Normalization
```python
def normalize_director_name(last, first):
    """Normalize for matching across CRA and Elections Alberta."""
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
- `director_names_for_matching.csv`: normalized_name, n_boards, orgs, total_org_funding

---

## Section 3: Political Donation Matching

### Source
Elections Alberta: `https://efpublic.elections.ab.ca/efContributorSearch.cfm`

### Strategy (per D004 and D006)
1. Start with top 100 multi-board directors (highest n_boards)
2. Exact match on normalized `LAST, FIRST` name
3. Record: contributor_name, amount, recipient_party, year
4. If automated collection blocked → produce manual lookup instructions
5. All donation-dependent claims tagged `confidence: MEDIUM`

### Output Schema
```
director_name, party, amount, year, matched_to_orgs, n_orgs_funded_ndp_era
```

---

## Section 4: Federal Grants Enrichment

### Source
- Databricks: `dbw_unitycatalog_test` uploaded files OR
- open.canada.ca: Federal Proactive Disclosure G&C dataset

### Matching
Primary: BN number (exact match to CRA T3010)
Secondary: Recipient legal name (fuzzy)

### Output Schema
```
BN, org_name, federal_department, program, amount, fiscal_year, province
```

---

## Anti-Patterns

1. **Don't fuzzy-match director names for donation linking** — exact match only (D004)
2. **Don't load individual grant payment rows into Neo4j** — aggregate first (D003)
3. **Don't use CRA 2023 financial data** — use CRA 2022 (2023 only ~45% complete)
4. **Don't skip blank Ministry rows silently** — log the count for audit trail
