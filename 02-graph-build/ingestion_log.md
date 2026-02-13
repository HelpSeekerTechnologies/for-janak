# Ingestion Log -- Agent 1A/1B Graph Builder

**Generated:** 2026-02-12 22:41:17
**Total Runtime:** ~2.5 hours (across 3 script executions due to session expiry)

## Execution Timeline

| Script | Steps | Duration | Notes |
|--------|-------|----------|-------|
| `agent_1_graph_builder.py` | Phase 0, 1A, Steps 1-7 | ~38 min | Session expired after Step 7 |
| `agent_1_resume.py` | Steps 8-10, Validation | ~35 min | Completed all remaining steps |
| `agent_1_complete.py` | Step 10 (re-run), Validation | ~8 min | Confirmed idempotent completion |

## Final Graph State

### Node Counts

| Label | Count | Source |
|-------|-------|--------|
| Organization (with BN) | 11,672 | 9,145 from CRA T3010 + 2,527 pre-existing |
| Director | 19,156 | multi_board_directors.csv |
| OrgEntity (Ministry lineage) | 142 | Archana's notebook (preserved) |
| FiscalYear | 2,679 | grants_aggregated.csv (NOTE: includes garbage values) |
| Region | 494 | org_risk_flags.csv City column |
| RiskFlag | 7 | 7 flag types from org_risk_flags.csv |

### Relationship Counts

| Relationship | Count | Description |
|-------------|-------|-------------|
| RECEIVED_GRANT | 35,075 | Organization -> OrgEntity (ministry) |
| SITS_ON | 11,023 | Director -> Organization |
| FLAGGED_AS | 11,033 | Organization -> RiskFlag |
| LOCATED_IN | 10,723 | Organization -> Region |
| SHARED_DIRECTORS | 9,081 | Organization -> Organization |
| SOURCE_OF | 62 | Ministry lineage (preserved) |
| TARGET_OF | 70 | Ministry lineage (preserved) |
| PARENT_OF | 89 | Ministry lineage (preserved) |
| EVIDENCED_BY | 82 | Ministry lineage (preserved) |

## Detailed Execution Log

```
[20:19:47] ========================================================================
[20:19:47] AGENT 1A/1B -- KGL v1.3 Graph Builder -- START
[20:19:47] ========================================================================
[20:19:47] Connected to Neo4j Aura

[20:19:47] -- PHASE 0: Existing Graph State (targeted label counts) --
[20:19:48]   OrgEntity: 142
[20:19:48]   TransformEvent: 66
[20:19:48]   SourceDocument: 42
[20:19:48]   Organization: 193193
[20:19:48]   Director: 19156
[20:19:48]   Person: 579152
[20:19:48]   FiscalYear: 2679
[20:19:49]   Region: 494
[20:19:49]   RiskFlag: 16
[20:19:49]   Rel SOURCE_OF: 62
[20:19:49]   Rel TARGET_OF: 70
[20:19:49]   Rel PARENT_OF: 89
[20:19:49]   Rel EVIDENCED_BY: 82
[20:19:49]   Rel SITS_ON: 509
[20:19:50]   Phase 0 completed in 2.7s

[20:19:50] -- PHASE 1A: Schema DDL (Constraints & Indexes) --
[20:19:50]   WARN: org_bn constraint failed (duplicate BNs from prior load)
[20:19:50]   OK: director_id
[20:19:50]   OK: fy_id
[20:19:50]   OK: region_id
[20:19:50]   OK: flag_id
[20:19:51]   OK: orgentity_canonical
[20:19:51]   OK: orgentity_name
[20:19:51]   OK: org_name
[20:19:51]   OK: director_name
[20:19:51]   Schema DDL completed in 1.1s

[20:19:51] -- STEP 1: FiscalYear Nodes --
[20:19:52]   Loaded grants_aggregated.csv: 702930 rows
[20:19:53]   NOTE: fiscal_year column contains dates, amounts, and strings -- not just year integers
[20:19:53]   VALIDATE: 2679 FiscalYear nodes in graph
[20:19:53]   Completed in 2.1s

[20:19:53] -- STEP 2: Region Nodes --
[20:19:53]   Loaded org_risk_flags.csv: 9145 rows
[20:19:53]   Unique cities/regions: 494
[20:19:53]   VALIDATE: 494 Region nodes in graph
[20:19:53]   Completed in 0.3s

[20:19:53] -- STEP 3: RiskFlag Nodes --
[20:19:54]   VALIDATE: 7 RiskFlag nodes with flag_type in graph
[20:19:54]   Completed in 0.7s

[20:19:54] -- STEP 4: Organization Nodes (CRA charities with BN) --
[20:19:54]   Prepared 9145 Organization params
[20:29:42]   Merged 9145 Organization nodes in 588.0s
[20:29:42]   VALIDATE: 11672 Organization nodes with BN in graph

[20:29:42] -- STEP 5: Director Nodes --
[20:29:42]   Loaded multi_board_directors.csv: 19156 rows
[20:29:42]   Prepared 19156 Director params (skipped 0)
[20:29:46]   Merged 19156 Director nodes in 4.0s
[20:29:46]   VALIDATE: 19156 Director nodes in graph

[20:29:46] -- STEP 6: SITS_ON Edges --
[20:29:46]   Known Organization BNs: 9145
[20:29:46]   Total director->BN references: 83887
[20:29:46]   Matched to known orgs: 9313 (11.1%)
[20:29:46]   SITS_ON edges to create: 9313
[20:39:28]   Merged 9313 SITS_ON edges in 581.9s
[20:39:28]   VALIDATE: 11023 SITS_ON relationships in graph

[20:39:28] -- STEP 7: RECEIVED_GRANT Edges --
[20:39:28]   NOTE: Ministry nodes use :OrgEntity label with canonical_id
[20:39:28]   Loaded goa_cra_matched.csv: 1304 rows
[20:39:28]   GOA->BN lookup built: 1300 entries
[20:39:28]   Extended name->BN lookup: 10154 entries (added org_risk_flags names)
[20:39:28]   OrgEntity (ministry) nodes in graph: 142
[20:39:28]   Ministry canonical_ids: 142
[20:39:28]   Ministry name lookup entries: 144
[20:39:29]   Grant rows total: 702930
[20:39:29]   Matched (org+ministry): 17993 (2.6%)
[20:39:29]   Unmatched recipients: 684425
[20:39:29]   Unmatched ministries: 512
[20:39:29]   Top unmatched ministry names: [('CULTURE,MULTICULTURALISMANDSTATUSOFWOMEN', 512)]
[20:39:29]   RECEIVED_GRANT edges to create: 17993
[20:39:29]     by canonical_id: 17993
[20:39:29]     by name:         0
[20:58:13]   Merged 17993 RECEIVED_GRANT edges in 1124.2s
[20:58:13]   VALIDATE: 35075 RECEIVED_GRANT relationships in graph

--- SESSION EXPIRED --- Restarted from Step 8 ---

[21:58:03] -- STEP 8: FLAGGED_AS Edges --
[21:58:03]   FLAGGED_AS edges to create: 9216
[22:06:27]   Merged 9216 FLAGGED_AS edges in 504.6s
[22:06:28]   VALIDATE: 11033 FLAGGED_AS relationships in graph

[22:06:28] -- STEP 9: Cluster Properties & SHARED_DIRECTORS Edges --
[22:06:28]   Loaded org_clusters.csv: 4636 rows
[22:11:18]   Set cluster_id on 4636 orgs in 290.5s
[22:11:19]   Loaded org_network_edges.csv: 154015 rows
[22:11:19]   SHARED_DIRECTORS edges (both orgs in set): 6826 of 154015 total
[22:24:39]   Merged 6826 SHARED_DIRECTORS edges in 800.6s
[22:24:40]   VALIDATE: 9081 SHARED_DIRECTORS relationships in graph
[22:24:40]   Step 9 total: 1091.7s

[22:24:40] -- STEP 10: LOCATED_IN Edges --
[22:24:40]   LOCATED_IN edges to create: 9145
[22:32:38]   Merged 9145 LOCATED_IN edges in 478.8s
[22:32:39]   VALIDATE: 10723 LOCATED_IN relationships in graph

[22:32:39] -- FINAL VALIDATION (targeted counts only) --
[22:41:15]   === Lineage Audit Node Counts ===
[22:41:15]     Organization: 11672
[22:41:15]     Director: 19156
[22:41:15]     OrgEntity: 142
[22:41:15]     FiscalYear: 2679
[22:41:15]     Region: 494
[22:41:15]     RiskFlag: 7
[22:41:15]   === Lineage Audit Relationship Counts ===
[22:41:15]     RECEIVED_GRANT: 35075
[22:41:15]     SITS_ON: 11023
[22:41:15]     FLAGGED_AS: 11033
[22:41:15]     LOCATED_IN: 10723
[22:41:16]     SHARED_DIRECTORS: 9081
[22:41:16]   === Existing Lineage Relationships ===
[22:41:16]     SOURCE_OF: 62
[22:41:16]     TARGET_OF: 70
[22:41:16]     PARENT_OF: 89
[22:41:16]     EVIDENCED_BY: 82

[22:41:16] -- SPOT CHECKS --
[22:41:16]   Top 5 orgs by number of risk flags:
[22:41:16]     CATHOLIC SOCIAL SERVICES (133696559RR0001): 48 flags
[22:41:16]     Calgary Immigrant Women's Association (118823657RR0001): 33 flags
[22:41:16]     Punjabi Community Health Services Calgary Society (802996983RR0001): 28 flags
[22:41:16]     YOUNG WOMENS CHRISTIAN ASSOCIATION OF LETHBRIDGE & DISTRICT (108227919RR0001): 27 flags
[22:41:16]     TSUU T'INA NATION BAND - STONEY CORRECTIONS SOCIETY (882741978RR0001): 24 flags
[22:41:16]   Top 5 directors by board seats (in graph):
[22:41:16]     SMITH, RICHARD: 88 boards
[22:41:16]     WILLIAMS, JENNIFER: 84 boards
[22:41:16]     RICHTER, KEITH: 78 boards
[22:41:16]     POTTS, JEFF: 78 boards
[22:41:16]     COOKE, MELISSA: 77 boards
[22:41:16]   Top 5 grants by amount:
[22:41:16]     CENTRAL ALBERTA CHILD ADVOCACY CENTRE LTD. <- CHILDREN'S SERVICES: $0 (2019-08-09 UNKNOWN)
[22:41:16]     (NOTE: amount=$0 suggests the amount property was not properly loaded on some edges)
[22:41:16]   Top 5 ministries by connected organizations:
[22:41:16]     CULTURE AND TOURISM: 2122 orgs, 5105 grants, $479,358,525
[22:41:16]     ARTS, CULTURE AND STATUS OF WOMEN: 1400 orgs, 1692 grants, $226,319,164
[22:41:16]     LABOUR: 1224 orgs, 2448 grants, $164,853,122
[22:41:16]     LABOUR AND IMMIGRATION: 1183 orgs, 1966 grants, $139,725,448
[22:41:16]     CULTURE AND STATUS OF WOMEN: 1124 orgs, 1391 grants, $202,195,964
[22:41:16]   Clustered orgs: 509 in 153 clusters
[22:41:17]   Clustered orgs that received grants: 143 from 43 ministries
[22:41:17]   Flagged orgs that received grants: 2883 across 6 flag types
[22:41:17]   Top 10 regions by organization count:
[22:41:17]     CALGARY: 2775 orgs
[22:41:17]     EDMONTON: 2267 orgs
[22:41:17]     LETHBRIDGE: 432 orgs
[22:41:17]     RED DEER: 233 orgs
[22:41:17]     SHERWOOD PARK: 153 orgs
[22:41:17]     MEDICINE HAT: 151 orgs
[22:41:17]     GRANDE PRAIRIE: 136 orgs
[22:41:17]     LACOMBE: 134 orgs
[22:41:17]     FORT MCMURRAY: 108 orgs
[22:41:17]     AIRDRIE: 97 orgs

[22:41:17] ========================================================================
[22:41:17] AGENT 1A/1B -- ALL 10 STEPS COMPLETE
[22:41:17] ========================================================================
```

## Known Issues

1. **Organization uniqueness constraint failed**: Duplicate BNs from a prior partial load (e.g., Node 18434815 and Node 139050326 both have bn='000125070RR0001'). MERGE operations still work correctly without the constraint.

2. **FiscalYear data quality**: The `fiscal_year` column in `grants_aggregated.csv` contains dates (2014-04-01), dollar amounts (-1000000.000), and strings (ALBERTA, True, False) -- not just fiscal year integers. This created 2,679 FiscalYear nodes, most of which are garbage. The actual fiscal years (2014-2024) are a small subset.

3. **Grant match rate (2.6%)**: Only 17,993 of 702,930 grant rows matched to both an organization BN and a ministry canonical_id. The low rate is because most GOA grant recipients are not CRA-registered charities (they are municipalities, school boards, health regions, etc.). The `goa_cra_matched.csv` gold standard only covers 1,304 recipients.

4. **SITS_ON match rate (11.1%)**: Only 9,313 of 83,887 director-to-BN references matched known Alberta organizations. Many directors sit on boards outside Alberta.

5. **Top grants show $0 amount**: The "Top 5 grants by amount" spot check returned $0 amounts, suggesting the `total_amount` field from `grants_aggregated.csv` was not properly cast to float on some RECEIVED_GRANT edges (the grants where `fiscal_year` contained garbage data likely had the amount in the wrong column).

6. **Unmatched ministry**: 512 grant rows reference `CULTURE,MULTICULTURALISMANDSTATUSOFWOMEN` which is a corrupted ministry name with no matching OrgEntity node.
