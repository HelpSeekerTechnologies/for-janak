# Smoking Gun Query Log — Agent 2

**Generated:** 2026-02-12 22:41:46

```
[22:41:38] ========================================================================
[22:41:38] AGENT 2 — SMOKING GUN QUERIES — START
[22:41:38] ========================================================================
[22:41:38] Connected to Neo4j Aura
[22:41:38] 
[22:41:38] -- STEP 0: Verify Graph Schema --
[22:41:39]   TARGET_OF direction: evt->OrgEntity=70, OrgEntity->evt=0
[22:41:39]   SOURCE_OF direction: evt->OrgEntity=0, OrgEntity->evt=62
[22:41:40]   TransformEvent sample: [{'eid': 'TE001', 'edate': neo4j.time.Date(2015, 10, 22), 'etype': 'RENAME', 'ctx': 'Notley NDP government (May 2015 election)'}, {'eid': 'TE002', 'edate': neo4j.time.Date(2015, 10, 22), 'etype': 'RENAME', 'ctx': 'Notley NDP government (May 2015 election)'}, {'eid': 'TE003', 'edate': neo4j.time.Date(2015, 10, 22), 'etype': 'RENAME', 'ctx': 'Notley NDP government (May 2015 election)'}, {'eid': 'TE004', 'edate': neo4j.time.Date(2015, 10, 22), 'etype': 'RENAME', 'ctx': 'Notley NDP government (May 2015 election)'}, {'eid': 'TE005', 'edate': neo4j.time.Date(2015, 10, 22), 'etype': 'TRANSFER', 'ctx': 'Notley NDP government (May 2015 election)'}]
[22:41:40]   event_date types: [{'dtype': 'DATE'}]
[22:41:40]   NDP-era TransformEvents: 14
[22:41:40]   TransformEvent->OrgEntity relationships sample: [{"rel_type": "TARGET_OF", "eid": "TE001", "ministry": "INDIGENOUS RELATIONS", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE002", "ministry": "ADVANCED EDUCATION", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE003", "ministry": "AGRICULTURE AND FORESTRY", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE004", "ministry": "ENVIRONMENT AND PARKS", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE005", "ministry": "ENERGY", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE006", "ministry": "ECONOMIC DEVELOPMENT AND TRADE", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE007", "ministry": "CULTURE AND TOURISM", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE008", "ministry": "SENIORS AND HOUSING", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE009", "ministry": "LABOUR", "edate": "2015-10-22"}, {"rel_type": "TARGET_OF", "eid": "TE010", "ministry": "CHILDREN'S SERVICES", "edate": "2016-02-02"}]
[22:41:40]   OrgEntity->TransformEvent relationships sample: [{"rel_type": "SOURCE_OF", "eid": "TE001", "ministry": "ABORIGINAL RELATIONS", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE002", "ministry": "INNOVATION AND ADVANCED EDUCATION", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE003", "ministry": "AGRICULTURE AND RURAL DEVELOPMENT", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE004", "ministry": "ENVIRONMENT AND SUSTAINABLE RESOURCE DEVELOPMENT", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE005", "ministry": "ENVIRONMENT AND SUSTAINABLE RESOURCE DEVELOPMENT", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE006", "ministry": "INTERNATIONAL AND INTERGOVERNMENTAL RELATIONS", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE007", "ministry": "CULTURE", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE007", "ministry": "TOURISM", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE008", "ministry": "SENIORS", "edate": "2015-10-22"}, {"rel_type": "SOURCE_OF", "eid": "TE010", "ministry": "HUMAN SERVICES", "edate": "2016-02-02"}]
[22:41:40]   RECEIVED_GRANT political_era distribution: [{"era": "NDP", "cnt": 13400}, {"era": "UCP_Smith", "cnt": 7491}, {"era": "UCP_Kenney", "cnt": 7070}, {"era": "UNKNOWN", "cnt": 4230}, {"era": "PC", "cnt": 2884}]
[22:41:40]   Clustered orgs: 509 in 153 clusters
[22:41:40] 
[22:41:40] -- STEP 1: Determine TransformEvent relationship patterns --
[22:41:40]   Total TransformEvents: 66
[22:41:40]   NDP events: 14
[22:41:40]   UCP events: 45
[22:41:40]     TE001: 2015-10-22 RENAME - Notley NDP government (May 2015 election)
[22:41:40]     TE002: 2015-10-22 RENAME - Notley NDP government (May 2015 election)
[22:41:40]     TE003: 2015-10-22 RENAME - Notley NDP government (May 2015 election)
[22:41:40]     TE004: 2015-10-22 RENAME - Notley NDP government (May 2015 election)
[22:41:40]     TE005: 2015-10-22 TRANSFER - Notley NDP government (May 2015 election)
[22:41:41]   NDP target ministries (evt-[:TARGET_OF]->OrgEntity): 13
[22:41:41]     EM-031: INDIGENOUS RELATIONS
[22:41:41]     EM-002: ADVANCED EDUCATION
[22:41:41]     EM-004: AGRICULTURE AND FORESTRY
[22:41:41]     EM-022: ENVIRONMENT AND PARKS
[22:41:41]     EM-020: ENERGY
[22:41:41]     EM-018: ECONOMIC DEVELOPMENT AND TRADE
[22:41:41]     EM-015: CULTURE AND TOURISM
[22:41:41]     EM-048: SENIORS AND HOUSING
[22:41:41]     EM-041: LABOUR
[22:41:41]     EM-011: CHILDREN'S SERVICES
[22:41:41]   NDP target ministries (OrgEntity-[:TARGET_OF]->evt): 0
[22:41:41]   Using pattern: (evt:TransformEvent)-[:TARGET_OF]->(m:OrgEntity)
[22:41:41]   NDP-restructured ministries: 13
[22:41:41]   IDs: ['EM-031', 'EM-002', 'EM-004', 'EM-022', 'EM-020', 'EM-018', 'EM-015', 'EM-048', 'EM-041', 'EM-011', 'EM-012', 'EM-054', 'ES-028']
[22:41:41] 
[22:41:41] ========================================================================
[22:41:41] QUERY 1: NDP Ministry Funding Trace
[22:41:41] ========================================================================
[22:41:41]   Query 1 returned 3000 organizations
[22:41:41]   Completed in 0.8s
[22:41:41]   Total NDP-era funding through NDP-restructured ministries: $11,690,276,262
[22:41:41]   Total UCP-era funding through same ministries: $14,151,023,667
[22:41:41]   Total PC-era funding through same ministries: $38,679,849
[22:41:41]   NDP->UCP change: 21.0%
[22:41:41]   Organizations in governance clusters: 111 / 3000
[22:41:41]   Organizations with risk flags: 2155 / 3000
[22:41:41]   Top 10 by NDP funding:
[22:41:41]     NORTHERN ALBERTA INSTITUTE OF TECHNOLOGY: NDP=$805,342,163 UCP=$910,412,423 (+13%) [cluster:750] flags=['in_director_cluster']
[22:41:41]     NORTHERN ALBERTA INSTITUTE OF TECHNOLOGY: NDP=$805,342,163 UCP=$910,412,423 (+13%) [cluster:750] flags=['in_director_cluster']
[22:41:41]     NORTHERN ALBERTA INSTITUTE OF TECHNOLOGY: NDP=$805,342,163 UCP=$910,412,423 (+13%) [cluster:750] flags=['in_director_cluster']
[22:41:41]     NORTHERN ALBERTA INSTITUTE OF TECHNOLOGY: NDP=$805,342,163 UCP=$910,412,423 (+13%) [cluster:750] flags=['in_director_cluster']
[22:41:41]     MOUNT ROYAL UNIVERSITY: NDP=$421,008,424 UCP=$537,129,167 (+28%) [no cluster] flags=[]
[22:41:41]     MOUNT ROYAL UNIVERSITY: NDP=$421,008,424 UCP=$537,129,167 (+28%) [no cluster] flags=[]
[22:41:41]     MOUNT ROYAL UNIVERSITY: NDP=$421,008,424 UCP=$537,129,167 (+28%) [no cluster] flags=[]
[22:41:41]     MOUNT ROYAL UNIVERSITY: NDP=$421,008,424 UCP=$537,129,167 (+28%) [no cluster] flags=[]
[22:41:41]     MOUNT ROYAL UNIVERSITY: NDP=$421,008,424 UCP=$537,129,167 (+28%) [no cluster] flags=[]
[22:41:41]     MOUNT ROYAL UNIVERSITY: NDP=$421,008,424 UCP=$537,129,167 (+28%) [no cluster] flags=[]
[22:41:41]   Wrote q1_ndp_ministry_funding_trace.csv: 3000 rows
[22:41:41] 
[22:41:41] ========================================================================
[22:41:41] QUERY 2: Director-Cluster-Funding-Concentration
[22:41:41] ========================================================================
[22:41:42]   Query 2 returned 2 rows
[22:41:42]   Completed in 0.2s
[22:41:42]   CLUSTERED: 106 orgs, total=$3,457,050,427, avg=$32,613,683, median=$648,259
[22:41:42]   NON-CLUSTERED: 2716 orgs, total=$8,233,028,596, avg=$3,031,307, median=$104,451
[22:41:42]   DISPARITY RATIO (avg clustered / avg non-clustered): 10.76x
[22:41:42]   MEDIAN DISPARITY RATIO: 6.21x
[22:41:42]   Wrote q2_cluster_funding_concentration.csv: 2 rows
[22:41:42] 
[22:41:42] -- Query 2b: Clustered vs Non-Clustered (ALL eras) --
[22:41:42]   CLUSTERED: 116 orgs — PC=$4,068,742 NDP=$3,457,050,427 UCP=$3,797,885,659
[22:41:42]   NON-CLUSTERED: 2980 orgs — PC=$36,359,980 NDP=$8,233,028,596 UCP=$10,353,138,008
[22:41:42] 
[22:41:42] ========================================================================
[22:41:42] QUERY 3: Governance Cluster NDP Audit
[22:41:42] ========================================================================
[22:41:45]   Query 3 returned 44 clusters
[22:41:45]   Completed in 3.0s
[22:41:45]   Total clustered NDP funding: $3,457,050,427
[22:41:45]   Total clustered UCP funding: $3,690,514,501
[22:41:45]   Top 15 clusters by NDP funding:
[22:41:45]     Cluster 750: size=5, recipients=4, NDP=$3,221,368,651, UCP=$3,641,649,694 (+13%), flags=6
[22:41:45]       orgs: NORTHERN ALBERTA INSTITUTE OF TECHNOLOGY
[22:41:45]     Cluster 480: size=14, recipients=13, NDP=$78,055,437, UCP=$17,452,637 (-78%), flags=51
[22:41:45]       orgs: CATHOLIC CHARITIES SOCIETY, CATHOLIC SOCIAL SERVICES
[22:41:45]     Cluster 881: size=6, recipients=5, NDP=$56,442,268, UCP=$1,045,100 (-98%), flags=6
[22:41:45]       orgs: THE CALGARY ZOOLOGICAL SOCIETY
[22:41:45]     Cluster 920: size=7, recipients=7, NDP=$26,107,136, UCP=$0 (-100%), flags=18
[22:41:45]       orgs: EDMONTON SYMPHONY SOCIETY, FRANCIS WINSPEAR CENTRE FOR MUSIC
[22:41:45]     Cluster 228: size=7, recipients=4, NDP=$21,482,271, UCP=$0 (-100%), flags=13
[22:41:45]       orgs: NATIONAL MUSIC CENTRE
[22:41:45]     Cluster 20: size=1, recipients=1, NDP=$11,462,197, UCP=$17,301,500 (+51%), flags=2
[22:41:45]       orgs: BURMAN UNIVERSITY
[22:41:45]     Cluster 183: size=6, recipients=1, NDP=$9,391,684, UCP=$0 (-100%), flags=15
[22:41:45]       orgs: COVENANT HEALTH
[22:41:45]     Cluster 1089: size=3, recipients=2, NDP=$7,234,848, UCP=$0 (-100%), flags=10
[22:41:45]       orgs: THEATRE CALGARY
[22:41:45]     Cluster 1530: size=5, recipients=4, NDP=$6,897,624, UCP=$2,577,064 (-63%), flags=15
[22:41:45]       orgs: ALBERTA CONSERVATION ASSOCIATION
[22:41:45]     Cluster 1144: size=7, recipients=6, NDP=$4,502,244, UCP=$0 (-100%), flags=14
[22:41:45]       orgs: THE YOUNG MEN'S CHRISTIAN ASSOCIATION OF EDMONTON
[22:41:45]     Cluster 138: size=4, recipients=3, NDP=$3,690,828, UCP=$990,852 (-73%), flags=9
[22:41:45]       orgs: INCLUSION ALBERTA SOCIETY
[22:41:45]     Cluster 956: size=2, recipients=1, NDP=$2,388,245, UCP=$0 (-100%), flags=4
[22:41:45]       orgs: HERITAGE PARK SOCIETY
[22:41:45]     Cluster 1252: size=3, recipients=3, NDP=$2,339,182, UCP=$1,333,928 (-43%), flags=7
[22:41:45]       orgs: LAKELAND AGRICULTURAL RESEARCH ASSOCIATION, RIVERLAND RECREATIONAL TRAIL SOCIETY
[22:41:45]     Cluster 715: size=2, recipients=1, NDP=$1,116,541, UCP=$781,500 (-30%), flags=5
[22:41:45]       orgs: LESSER SLAVE WATERSHED COUNCIL
[22:41:45]     Cluster 1167: size=3, recipients=2, NDP=$1,092,289, UCP=$0 (-100%), flags=8
[22:41:45]       orgs: ONE YELLOW RABBIT THEATRE ASSOCIATION
[22:41:45]   Wrote q3_cluster_ndp_audit.csv: 44 rows
[22:41:45] 
[22:41:45] ========================================================================
[22:41:45] SYMMETRY TEST: UCP-Restructured Ministries
[22:41:45] ========================================================================
[22:41:45]   UCP-restructured ministries: 48
[22:41:45]     EM-042: LABOUR AND IMMIGRATION
[22:41:45]     EM-013: CULTURE
[22:41:45]     EM-017: ECONOMIC DEVELOPMENT
[22:41:45]     EM-035: JOBS
[22:41:45]     EM-036: JOBS, ECONOMY AND INNOVATION
[22:41:45]     EM-014: CULTURE AND STATUS OF WOMEN
[22:41:45]     EM-007: AGRICULTURE, FOREST AND RURAL ECON
[22:41:45]     EM-039: JUSTICE
[22:41:45]     EM-046: PUBLIC SAFETY AND EMERGENCY SERVICES
[22:41:45]     EM-023: ENVIRONMENT AND PROTECTED AREAS
[22:41:45]   UCP Symmetry: Clustered vs Non-Clustered through UCP-restructured ministries:
[22:41:45]     CLUSTERED: 110 orgs, UCP=$233,883,093 (avg=$2,126,210)
[22:41:45]     NON-CLUSTERED: 2874 orgs, UCP=$2,197,174,808 (avg=$764,501)
[22:41:45]   UCP DISPARITY RATIO: 2.78x
[22:41:45] 
[22:41:45] ========================================================================
[22:41:45] BONUS: Same NDP-restructured ministries across ALL eras
[22:41:45] ========================================================================
[22:41:45]   Funding through NDP-RESTRUCTURED ministries by political era:
[22:41:45]     NDP: 2822 orgs, $11,690,079,023, 10978 grants, avg=$1,064,864
[22:41:45]     UCP_Kenney: 1220 orgs, $8,668,999,576, 3178 grants, avg=$2,727,816
[22:41:45]     UCP_Smith: 426 orgs, $5,482,024,092, 560 grants, avg=$9,789,329
[22:41:45]     UNKNOWN: 818 orgs, $2,797,854,680, 1078 grants, avg=$2,982,788
[22:41:45]     PC: 752 orgs, $40,428,722, 788 grants, avg=$51,305
[22:41:45] 
[22:41:45] ========================================================================
[22:41:45] BONUS 2: Shared director links among top NDP grant recipients
[22:41:45] ========================================================================
[22:41:46]   Shared director pairs (both NDP grantees): 25
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     CATHOLIC SOCIAL SERVICES <-> CATHOLIC CHARITIES SOCIETY: 12 shared dirs, NDP: $6,499,870 + $57,000
[22:41:46]     BIG BROTHERS AND BIG SISTERS OF RED DEER AND DISTRICT <-> BOYS' AND GIRLS' CLUB OF RED DEER & DISTRICT: 6 shared dirs, NDP: $370,732 + $61,280
[22:41:46]     FRANCIS WINSPEAR CENTRE FOR MUSIC <-> EDMONTON SYMPHONY SOCIETY: 5 shared dirs, NDP: $4,618,698 + $3,062,760
[22:41:46]     FRANCIS WINSPEAR CENTRE FOR MUSIC <-> EDMONTON SYMPHONY SOCIETY: 5 shared dirs, NDP: $4,618,698 + $3,062,760
[22:41:46]   Wrote bonus_shared_director_grantees.csv: 25 rows
[22:41:46] 
[22:41:46] ========================================================================
[22:41:46] PHASE 2 COMPLETE — SUMMARY
[22:41:46] ========================================================================
[22:41:46] Total elapsed: 7.5s (0.1 min)
[22:41:46] 
[22:41:46] Output files:
[22:41:46]   bonus_shared_director_grantees.csv: 25 rows
[22:41:46]   q1_ndp_ministry_funding_trace.csv: 3000 rows
[22:41:46]   q2_cluster_funding_concentration.csv: 2 rows
[22:41:46]   q3_cluster_ndp_audit.csv: 44 rows
```
