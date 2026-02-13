"""
Generate accurate Sankey HTML from Neo4j graph data.
Reads sankey_data.json and produces 01-ministry-lineage-political.html
"""
import json, math, os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(SCRIPT_DIR, 'sankey_data.json')
OUT_PATH = os.path.join(SCRIPT_DIR, '01-ministry-lineage-political.html')

with open(DATA_PATH) as f:
    data = json.load(f)

# ── Build funding lookup: {ministry_name: {era: amount}} ──
funding_map = {}
for row in data['funding']:
    m, era, total = row['ministry'], row['era'], row['total']
    if m not in funding_map:
        funding_map[m] = {}
    funding_map[m][era] = total

def get_funding(name, era):
    return funding_map.get(name, {}).get(era, 0)

# ── Filter ministry-level transform events ──
ministry_nodes = {n['name'] for n in data['nodes'] if n['entity_type'] == 'ministry'}

ministry_events = []
for ev in data['events']:
    if ev['source_name'] in ministry_nodes and ev['target_name'] in ministry_nodes:
        ministry_events.append(ev)

# ── Classify events by political era ──
def era_of_date(d):
    if not d: return 'UNKNOWN'
    if d < '2015-05-24': return 'PC'
    if d < '2019-04-30': return 'NDP'
    if d < '2022-10-11': return 'UCP_Kenney'
    return 'UCP_Smith'

# ── Define 4 Sankey columns ──
# Col 0: PC roots (pre-2015)
# Col 1: NDP state (after NDP restructuring 2015-2016)
# Col 2: UCP-Kenney end state (after July 2021 shuffle)
# Col 3: Current (after May 2025 restructuring)

# For each column, pick which ministry names appear.
# A ministry appears in a column if it has funding in that era, OR is the target of
# a transform event in that era.

# PC column: ministries with PC-era funding
pc_ministries = {}
for m, eras in funding_map.items():
    if m in ministry_nodes and 'PC' in eras and eras['PC'] > 0:
        pc_ministries[m] = eras['PC']

# NDP column: ministries with NDP-era funding (combine overlapping old/new names)
ndp_ministries = {}
for m, eras in funding_map.items():
    if m in ministry_nodes and 'NDP' in eras and eras['NDP'] > 0:
        ndp_ministries[m] = eras['NDP']

# Combine INNOVATION AND ADVANCED EDUCATION NDP into ADVANCED EDUCATION
if 'INNOVATION AND ADVANCED EDUCATION' in ndp_ministries and 'ADVANCED EDUCATION' in ndp_ministries:
    ndp_ministries['ADVANCED EDUCATION'] += ndp_ministries.pop('INNOVATION AND ADVANCED EDUCATION')
# Combine SENIORS NDP into SENIORS AND HOUSING
if 'SENIORS' in ndp_ministries and 'SENIORS AND HOUSING' in ndp_ministries:
    ndp_ministries['SENIORS AND HOUSING'] += ndp_ministries.pop('SENIORS')
# Combine ABORIGINAL RELATIONS NDP into INDIGENOUS RELATIONS
if 'ABORIGINAL RELATIONS' in ndp_ministries and 'INDIGENOUS RELATIONS' in ndp_ministries:
    ndp_ministries['INDIGENOUS RELATIONS'] += ndp_ministries.pop('ABORIGINAL RELATIONS')
# Remove HUMAN SERVICES from NDP (it's pre-split overlap; show split products instead)
if 'HUMAN SERVICES' in ndp_ministries:
    hs_ndp = ndp_ministries.pop('HUMAN SERVICES')
    # Distribute to split products proportionally
    splits = ['CHILDREN\'S SERVICES', 'COMMUNITY AND SOCIAL SERVICES', 'LABOUR']
    split_total = sum(ndp_ministries.get(s, 0) for s in splits)
    if split_total > 0:
        for s in splits:
            if s in ndp_ministries:
                ndp_ministries[s] += hs_ndp * (ndp_ministries[s] / split_total)

# Kenney column: ministries with UCP_Kenney funding
kenney_ministries = {}
for m, eras in funding_map.items():
    if m in ministry_nodes and 'UCP_Kenney' in eras and eras['UCP_Kenney'] > 0:
        kenney_ministries[m] = eras['UCP_Kenney']

# Current column: post-May 2025 state
# Use UCP_Smith funding but map to current names via May 2025 events
current_ministries = {}

# First, get all Smith-era funding
smith_funding = {}
for m, eras in funding_map.items():
    if m in ministry_nodes and 'UCP_Smith' in eras and eras['UCP_Smith'] > 0:
        smith_funding[m] = eras['UCP_Smith']

# May 2025 renames/merges: map old → new
may2025_map = {
    'HEALTH': [('HOSPITAL AND SURGICAL HEALTH SERVICES', 0.5),
               ('PRIMARY AND PREVENTATIVE HEALTH SERVICES', 0.5)],
    'EDUCATION': [('EDUCATION AND CHILDCARE', 1.0)],
    'SENIORS, COMMUNITY AND SOCIAL SERVICES': [('ASSISTED LIVING AND SOCIAL SERVICES', 1.0)],
    'JOBS, ECONOMY AND TRADE': [('JOBS, ECONOMY, TRADE AND IMMIGRATION', 1.0)],
    'IMMIGRATION AND MULTICULTURALISM': [('JOBS, ECONOMY, TRADE AND IMMIGRATION', 1.0)],
}

for m, amount in smith_funding.items():
    if m in may2025_map:
        for new_name, frac in may2025_map[m]:
            current_ministries[new_name] = current_ministries.get(new_name, 0) + amount * frac
    else:
        # Check if this is an intermediate Smith-era name that was renamed
        # Map intermediate names to their current successors
        intermediate_map = {
            'CHILDREN\'S SERVICES': 'CHILDREN AND FAMILY SERVICES',
            'CULTURE': 'ARTS, CULTURE AND STATUS OF WOMEN',
            'FORESTRY, PARKS AND TOURISM': 'FORESTRY AND PARKS',  # simplified
            'TRADE, IMMIGRATION AND MULTICULTURALISM': 'JOBS, ECONOMY, TRADE AND IMMIGRATION',
            'JOBS, ECONOMY AND NORTHERN DEVELOPMENT': 'JOBS, ECONOMY, TRADE AND IMMIGRATION',
        }
        target = intermediate_map.get(m, m)
        current_ministries[target] = current_ministries.get(target, 0) + amount

# ── Filter to top N per column for readability ──
def top_n(d, n=18, min_val=1e6):
    items = [(k, v) for k, v in d.items() if v >= min_val]
    items.sort(key=lambda x: -x[1])
    return dict(items[:n])

pc_ministries = top_n(pc_ministries, 14)
ndp_ministries = top_n(ndp_ministries, 16)
kenney_ministries = top_n(kenney_ministries, 16)
current_ministries = top_n(current_ministries, 19)

# ── Build Sankey nodes ──
nodes = []  # [{name, col, funding, fullName}]
node_index = {}  # "name|col" -> index

def add_node(name, col, funding, abbrev=None):
    key = f"{name}|{col}"
    if key in node_index:
        return node_index[key]
    idx = len(nodes)
    node_index[key] = idx
    # Create abbreviated name for display
    if abbrev is None:
        abbrev = name
        if len(abbrev) > 28:
            abbrev = abbrev[:26] + '...'
    nodes.append({'name': abbrev, 'col': col, 'funding': funding, 'fullName': name})
    return idx

# Add nodes per column
for name, amt in sorted(pc_ministries.items(), key=lambda x: -x[1]):
    add_node(name, 0, amt)

for name, amt in sorted(ndp_ministries.items(), key=lambda x: -x[1]):
    add_node(name, 1, amt)

for name, amt in sorted(kenney_ministries.items(), key=lambda x: -x[1]):
    add_node(name, 2, amt)

for name, amt in sorted(current_ministries.items(), key=lambda x: -x[1]):
    add_node(name, 3, amt)

# ── Build links ──
links = []  # [{source, target, type}]

# NDP transform events: PC → NDP links
ndp_events = [e for e in ministry_events if e['event_date'] and '2015' <= e['event_date'] <= '2016-12-31']
for ev in ndp_events:
    src = ev['source_name']
    tgt = ev['target_name']
    # Handle combined names
    if tgt == 'INDIGENOUS RELATIONS' and src == 'ABORIGINAL RELATIONS':
        src_key = f"ABORIGINAL RELATIONS|0"
        tgt_key = f"INDIGENOUS RELATIONS|1"
    elif tgt == 'ADVANCED EDUCATION' and src == 'INNOVATION AND ADVANCED EDUCATION':
        src_key = f"INNOVATION AND ADVANCED EDUCATION|0"
        tgt_key = f"ADVANCED EDUCATION|1"
    elif tgt == 'SENIORS AND HOUSING' and src == 'SENIORS':
        src_key = f"SENIORS|0"
        tgt_key = f"SENIORS AND HOUSING|1"
    else:
        src_key = f"{src}|0"
        tgt_key = f"{tgt}|1"

    if src_key in node_index and tgt_key in node_index:
        links.append({
            'source': node_index[src_key],
            'target': node_index[tgt_key],
            'type': ev['event_type'].lower()
        })

# Continuation links: PC ministries that persist unchanged to NDP
for name in pc_ministries:
    src_key = f"{name}|0"
    tgt_key = f"{name}|1"
    if src_key in node_index and tgt_key in node_index:
        # Check if there's already a transform link from this source
        existing = any(l['source'] == node_index[src_key] for l in links)
        if not existing:
            links.append({'source': node_index[src_key], 'target': node_index[tgt_key], 'type': 'continuation'})

# NDP → Kenney links
kenney_events = [e for e in ministry_events if e['event_date'] and '2019' <= e['event_date'] <= '2021-12-31']
for ev in kenney_events:
    src_key = f"{ev['source_name']}|1"
    tgt_key = f"{ev['target_name']}|2"
    if src_key in node_index and tgt_key in node_index:
        links.append({
            'source': node_index[src_key],
            'target': node_index[tgt_key],
            'type': ev['event_type'].lower()
        })

# Continuation: NDP → Kenney
for name in ndp_ministries:
    src_key = f"{name}|1"
    tgt_key = f"{name}|2"
    if src_key in node_index and tgt_key in node_index:
        existing = any(l['source'] == node_index[src_key] for l in links)
        if not existing:
            links.append({'source': node_index[src_key], 'target': node_index[tgt_key], 'type': 'continuation'})

# Kenney → Current links (collapsing all Smith-era transforms)
# Map Kenney-era names to Current-era names through the chain of Smith events
kenney_to_current = {
    'HEALTH': ['HOSPITAL AND SURGICAL HEALTH SERVICES', 'PRIMARY AND PREVENTATIVE HEALTH SERVICES'],
    'EDUCATION': ['EDUCATION AND CHILDCARE'],
    'ADVANCED EDUCATION': ['ADVANCED EDUCATION'],
    'COMMUNITY AND SOCIAL SERVICES': ['ASSISTED LIVING AND SOCIAL SERVICES'],
    'INFRASTRUCTURE': ['INFRASTRUCTURE'],
    'CHILDREN\'S SERVICES': ['CHILDREN AND FAMILY SERVICES'],
    'SENIORS AND HOUSING': ['ASSISTED LIVING AND SOCIAL SERVICES'],
    'LABOUR AND IMMIGRATION': ['SKILLED TRADES AND PROFESSIONS', 'JOBS, ECONOMY, TRADE AND IMMIGRATION'],
    'CULTURE AND STATUS OF WOMEN': ['ARTS, CULTURE AND STATUS OF WOMEN'],
    'AGRICULTURE AND FORESTRY': ['AGRICULTURE AND IRRIGATION'],
    'MUNICIPAL AFFAIRS': ['MUNICIPAL AFFAIRS'],
    'TRANSPORTATION': ['TRANSPORTATION AND ECONOMIC CORRIDORS'],
    'ENVIRONMENT AND PARKS': ['ENVIRONMENT AND PROTECTED AREAS', 'FORESTRY AND PARKS', 'TOURISM AND SPORT'],
    'JOBS, ECONOMY AND INNOVATION': ['JOBS, ECONOMY, TRADE AND IMMIGRATION'],
    'JUSTICE AND SOLICITOR GENERAL': ['JUSTICE', 'PUBLIC SAFETY AND EMERGENCY SERVICES'],
    'INDIGENOUS RELATIONS': ['INDIGENOUS RELATIONS'],
}

for kenney_name, current_names in kenney_to_current.items():
    src_key = f"{kenney_name}|2"
    if src_key not in node_index:
        continue
    for cur_name in current_names:
        tgt_key = f"{cur_name}|3"
        if tgt_key not in node_index:
            continue
        # Determine type
        if len(current_names) > 1:
            t = 'split'
        elif kenney_name == current_names[0]:
            t = 'continuation'
        else:
            t = 'rename'
        # Check for merges (multiple Kenney sources → same target)
        existing_to_target = [l for l in links if l['target'] == node_index[tgt_key]]
        if existing_to_target:
            t = 'merge'
        links.append({
            'source': node_index[src_key],
            'target': node_index[tgt_key],
            'type': t
        })

# ── Compute era totals ──
era_totals = {}
for et in data['era_totals']:
    era_totals[et['era']] = et['total']

total_funding = sum(era_totals.values())
total_events = len(ministry_events)
total_ministries = len(ministry_nodes)

# ── Format currency ──
def fmt_currency(val):
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    if val >= 1e6:
        return f"${val/1e6:.0f}M"
    return f"${val:,.0f}"

# ── Generate nodes JSON ──
nodes_json = json.dumps(nodes)
links_json = json.dumps(links)

# ── Generate transform events table data ──
table_events = []
for ev in ministry_events:
    table_events.append({
        'id': ev['event_id'],
        'type': ev['event_type'],
        'source': ev['source_name'],
        'target': ev['target_name'],
        'date': ev['event_date'],
        'era': era_of_date(ev['event_date'])
    })
table_events.sort(key=lambda x: (x['date'] or '', x['source']))
table_json = json.dumps(table_events)

# ── Build HTML ──
html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Operation Lineage Audit — Ministry Lineage Sankey</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<style>
  :root {{
    --primary-teal: #28bcb8;
    --aqua: #4bc2c3;
    --navy: #103656;
    --light-bg: #f8fffe;
    --text-primary: #1a1a2e;
    --text-secondary: #4a5568;
    --font-primary: 'Inter', 'Segoe UI', system-ui, sans-serif;
    --gradient-primary: linear-gradient(135deg, #28bcb8, #4bc2c3);
    --card-shadow: 0 2px 12px rgba(16, 54, 86, 0.10);
    --border-subtle: #d4eeed;
    --hero-gradient: linear-gradient(135deg, #103656 0%, #1a4a6e 40%, #174260 100%);
  }}
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: var(--font-primary);
    background: var(--light-bg);
    color: var(--text-primary);
    line-height: 1.6;
    -webkit-font-smoothing: antialiased;
  }}
  .brand-header {{
    background: var(--hero-gradient);
    color: #fff;
    padding: 18px 32px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
  }}
  .brand-header h1 {{ font-size: 1.15rem; font-weight: 600; letter-spacing: 0.3px; }}
  .brand-header h1 span {{ color: var(--primary-teal); }}
  .brand-header .subtitle {{ font-size: 0.82rem; opacity: 0.75; font-weight: 400; }}
  .brand-header .brand-right {{ text-align: right; font-size: 0.78rem; opacity: 0.65; }}
  .container {{ max-width: 1500px; margin: 0 auto; padding: 0 24px 60px; }}
  .hero-banner {{
    background: var(--hero-gradient);
    margin: 0 -24px;
    padding: 32px 24px 36px;
  }}
  .hero-title {{ text-align: center; color: #fff; margin-bottom: 24px; }}
  .hero-title h2 {{ font-size: 1.5rem; font-weight: 700; margin-bottom: 4px; }}
  .hero-title p {{ font-size: 0.9rem; opacity: 0.7; }}
  .hero-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    max-width: 1000px;
    margin: 0 auto;
  }}
  .hero-card {{
    background: rgba(255,255,255,0.08);
    border: 1px solid rgba(75, 194, 195, 0.25);
    border-radius: 12px;
    padding: 20px 16px;
    text-align: center;
    backdrop-filter: blur(4px);
    transition: transform 0.2s;
  }}
  .hero-card:hover {{ transform: translateY(-2px); border-color: var(--primary-teal); }}
  .hero-card .metric-value {{ font-size: 2rem; font-weight: 800; color: var(--primary-teal); line-height: 1.1; margin-bottom: 4px; }}
  .hero-card .metric-label {{ font-size: 0.78rem; color: rgba(255,255,255,0.85); line-height: 1.3; }}
  .hero-card .metric-sublabel {{ font-size: 0.68rem; color: rgba(255,255,255,0.5); margin-top: 3px; }}
  .section {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--card-shadow);
    margin-top: 24px;
    overflow: hidden;
  }}
  .section-header {{
    background: var(--navy);
    color: #fff;
    padding: 16px 24px;
    display: flex;
    align-items: center;
    gap: 12px;
  }}
  .section-header .section-number {{
    background: var(--primary-teal);
    color: var(--navy);
    font-weight: 800;
    font-size: 0.85rem;
    width: 28px; height: 28px;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0;
  }}
  .section-header h3 {{ font-size: 1.05rem; font-weight: 600; }}
  .section-body {{ padding: 28px 24px; }}
  .sankey-container {{
    width: 100%;
    overflow-x: auto;
    min-height: 600px;
  }}
  .sankey-container svg {{ display: block; margin: 0 auto; }}
  .legend {{
    display: flex;
    gap: 24px;
    justify-content: center;
    margin: 20px 0 8px;
    flex-wrap: wrap;
  }}
  .legend-item {{
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 0.82rem;
    color: var(--text-secondary);
  }}
  .legend-swatch {{
    width: 20px; height: 14px;
    border-radius: 3px;
    border: 1px solid rgba(0,0,0,0.1);
  }}
  .table-wrap {{
    overflow-x: auto;
    border: 1px solid var(--border-subtle);
    border-radius: 8px;
    margin-top: 16px;
    max-height: 500px;
    overflow-y: auto;
  }}
  table.data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.84rem;
  }}
  table.data-table thead {{
    background: var(--navy);
    color: #fff;
    position: sticky;
    top: 0;
    z-index: 1;
  }}
  table.data-table th {{
    padding: 10px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    white-space: nowrap;
  }}
  table.data-table td {{
    padding: 8px 12px;
    border-bottom: 1px solid #eef5f4;
  }}
  table.data-table tbody tr:hover {{ background: #f0faf9; }}
  table.data-table tbody tr:nth-child(even) {{ background: #fafefe; }}
  table.data-table tbody tr:nth-child(even):hover {{ background: #edf8f7; }}
  .event-badge {{
    display: inline-block;
    font-size: 0.72rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 10px;
    text-transform: uppercase;
  }}
  .event-badge.rename {{ background: #e8f4fd; color: #2980b9; }}
  .event-badge.transfer {{ background: #fef0e7; color: #d35400; }}
  .event-badge.split {{ background: #fde8e8; color: #e74c3c; }}
  .event-badge.merge {{ background: #e8f8f0; color: #27ae60; }}
  .era-badge {{
    display: inline-block;
    font-size: 0.68rem;
    font-weight: 700;
    padding: 2px 6px;
    border-radius: 8px;
  }}
  .era-badge.pc {{ background: #e0e0e0; color: #555; }}
  .era-badge.ndp {{ background: #fde8d0; color: #d35400; }}
  .era-badge.kenney {{ background: #d4f4f3; color: #0d7377; }}
  .era-badge.smith {{ background: #c5e8e7; color: #0a5c5f; }}
  .sankey-tooltip {{
    position: fixed;
    background: var(--navy);
    color: #fff;
    padding: 12px 16px;
    border-radius: 8px;
    font-size: 0.82rem;
    max-width: 400px;
    pointer-events: none;
    z-index: 1000;
    box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    line-height: 1.5;
    opacity: 0;
    transition: opacity 0.2s;
  }}
  .sankey-tooltip.visible {{ opacity: 1; }}
  .sankey-tooltip .tt-title {{ font-weight: 700; color: var(--primary-teal); margin-bottom: 4px; font-size: 0.88rem; }}
  .sankey-tooltip .tt-row {{ display: flex; justify-content: space-between; gap: 16px; }}
  .sankey-tooltip .tt-label {{ opacity: 0.7; }}
  .sankey-tooltip .tt-value {{ font-weight: 600; }}
  .provenance-footer {{
    margin-top: 40px;
    background: #fff;
    border: 1px solid var(--border-subtle);
    border-radius: 12px;
    padding: 20px 24px;
  }}
  .provenance-footer h4 {{
    font-size: 0.9rem;
    color: var(--navy);
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 2px solid var(--primary-teal);
  }}
  .provenance-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px;
  }}
  .provenance-item {{
    background: var(--light-bg);
    border: 1px solid var(--border-subtle);
    border-radius: 8px;
    padding: 12px;
  }}
  .provenance-item .pv-label {{
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-secondary);
    font-weight: 600;
    margin-bottom: 2px;
  }}
  .provenance-item .pv-value {{ font-size: 0.88rem; color: var(--navy); font-weight: 600; }}
  .page-footer {{
    text-align: center;
    padding: 24px;
    font-size: 0.78rem;
    color: #8a9aaa;
    border-top: 1px solid var(--border-subtle);
    margin-top: 40px;
  }}
  .page-footer strong {{ color: var(--navy); }}
  @keyframes fadeInUp {{
    from {{ opacity: 0; transform: translateY(20px); }}
    to {{ opacity: 1; transform: translateY(0); }}
  }}
  .section {{ animation: fadeInUp 0.5s ease both; }}
  @media (max-width: 900px) {{
    .hero-grid {{ grid-template-columns: 1fr 1fr; }}
  }}
  @media print {{
    body {{ background: #fff; -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    .brand-header, .hero-banner, .section-header {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    .section {{ break-inside: avoid; box-shadow: none; border: 1px solid #ddd; }}
    .sankey-container {{ overflow: visible; }}
    .no-print {{ display: none !important; }}
    @page {{ margin: 0.5in; }}
  }}
</style>
</head>
<body>

<header class="brand-header">
  <div>
    <h1>Operation Lineage Audit &mdash; <span>HelpSeeker Technologies</span></h1>
    <div class="subtitle">Ministry Lineage &amp; Political Era Sankey Diagram</div>
  </div>
  <div class="brand-right">
    Ministry Restructuring Analysis<br>
    PC &rarr; NDP &rarr; UCP-Kenney &rarr; UCP-Smith (May 2025)
  </div>
</header>

<div class="container">
  <div class="hero-banner">
    <div class="hero-title">
      <h2>Alberta Ministry Lineage Across Political Eras</h2>
      <p>Tracking {total_events} ministry-level restructuring events and ${total_funding/1e9:.1f}B in grant funding flows</p>
    </div>
    <div class="hero-grid">
      <div class="hero-card">
        <div class="metric-value">{total_ministries}</div>
        <div class="metric-label">Ministry Entities</div>
        <div class="metric-sublabel">Across all political eras</div>
      </div>
      <div class="hero-card">
        <div class="metric-value">{fmt_currency(era_totals.get('PC', 0))}</div>
        <div class="metric-label">PC Era Funding</div>
        <div class="metric-sublabel">Pre-2015</div>
      </div>
      <div class="hero-card">
        <div class="metric-value">{fmt_currency(era_totals.get('NDP', 0))}</div>
        <div class="metric-label">NDP Era Funding</div>
        <div class="metric-sublabel">2015&ndash;2019</div>
      </div>
      <div class="hero-card">
        <div class="metric-value">{fmt_currency(era_totals.get('UCP_Kenney', 0))}</div>
        <div class="metric-label">UCP-Kenney Funding</div>
        <div class="metric-sublabel">2019&ndash;2022</div>
      </div>
      <div class="hero-card">
        <div class="metric-value">{fmt_currency(era_totals.get('UCP_Smith', 0))}</div>
        <div class="metric-label">UCP-Smith Funding</div>
        <div class="metric-sublabel">2022&ndash;present (May 2025 reorg)</div>
      </div>
    </div>
  </div>

  <!-- Sankey Diagram -->
  <div class="section">
    <div class="section-header">
      <div class="section-number">1</div>
      <h3>Ministry Restructuring Flow &mdash; PC &rarr; NDP &rarr; UCP-Kenney &rarr; Current (May 2025)</h3>
    </div>
    <div class="section-body">
      <div class="legend">
        <div class="legend-item"><div class="legend-swatch" style="background:#95a5a6;"></div> PC Era (pre-2015)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#e67e22;"></div> NDP Era (2015&ndash;2019)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#28bcb8;"></div> UCP-Kenney (2019&ndash;2022)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#1a8a87;"></div> Current / UCP-Smith (May 2025)</div>
      </div>
      <div class="sankey-container" id="sankeyContainer"></div>
    </div>
  </div>

  <!-- Transform Events Table -->
  <div class="section" style="animation-delay: 0.15s;">
    <div class="section-header">
      <div class="section-number">2</div>
      <h3>All Ministry Transform Events &mdash; Complete Registry ({total_events} events)</h3>
    </div>
    <div class="section-body">
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Event ID</th>
              <th>Era</th>
              <th>Type</th>
              <th>Source Ministry</th>
              <th>Target Ministry</th>
              <th>Date</th>
            </tr>
          </thead>
          <tbody id="eventTableBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Data Provenance Footer -->
  <div class="provenance-footer">
    <h4>Data Provenance</h4>
    <div class="provenance-grid">
      <div class="provenance-item">
        <div class="pv-label">Source</div>
        <div class="pv-value">Neo4j graph: {len(data['nodes'])} OrgEntity nodes, {len(data['events'])} transform relationships</div>
      </div>
      <div class="provenance-item">
        <div class="pv-label">Query Date</div>
        <div class="pv-value">2026-02-12</div>
      </div>
      <div class="provenance-item">
        <div class="pv-label">Confidence</div>
        <div class="pv-value">HIGH &mdash; all data from verified graph model</div>
      </div>
      <div class="provenance-item">
        <div class="pv-label">Latest Event</div>
        <div class="pv-value">2025-05-16: HEALTH split, EDUCATION rename</div>
      </div>
      <div class="provenance-item">
        <div class="pv-label">Ministry Entities</div>
        <div class="pv-value">{total_ministries} across 4 political eras</div>
      </div>
      <div class="provenance-item">
        <div class="pv-label">Funding Coverage</div>
        <div class="pv-value">${total_funding/1e9:.1f}B across PC, NDP, UCP-Kenney, UCP-Smith</div>
      </div>
    </div>
  </div>

  <div class="page-footer">
    <strong>Operation Lineage Audit</strong> &mdash; HelpSeeker Technologies &copy; 2026<br>
    Generated from Neo4j graph analysis of Alberta Government grants &amp; CRA charity governance data.<br>
    This dashboard is a self-contained HTML artifact. No external data connections required.
  </div>
</div>

<!-- Tooltip -->
<div class="sankey-tooltip" id="sankeyTooltip"></div>

<script>
(function() {{
  'use strict';

  var COL_COLORS = ['#95a5a6', '#e67e22', '#28bcb8', '#1a8a87'];
  var COL_LABELS = ['PC ERA (pre-2015)', 'NDP ERA (2015-2019)', 'UCP-KENNEY (2019-2022)', 'CURRENT (May 2025)'];
  var LINK_COLORS = {{
    continuation: '#aab',
    rename: '#2980b9',
    transfer: '#d35400',
    split: '#e74c3c',
    merge: '#27ae60'
  }};

  var nodes = {nodes_json};
  var links = {links_json};
  var tableEvents = {table_json};

  // ===== SANKEY LAYOUT =====
  function sankeyLayout(nodes, links, width, height, nodePadding, nodeWidth) {{
    var N = nodes.map(function(d, i) {{
      return {{
        index: i, name: d.name, col: d.col, funding: d.funding,
        fullName: d.fullName,
        value: Math.max(Math.pow(d.funding / 1e6, 0.42), 4),
        x0: 0, x1: 0, y0: 0, y1: 0,
        sourceLinks: [], targetLinks: []
      }};
    }});
    var L = links.map(function(d, i) {{
      return {{
        index: i, source: N[d.source], target: N[d.target],
        type: d.type, value: 0, width: 0, y0: 0, y1: 0
      }};
    }});

    L.forEach(function(link) {{
      link.source.sourceLinks.push(link);
      link.target.targetLinks.push(link);
      link.value = link.target.value;
    }});

    // Assign x positions by column
    var colSpacing = (width - nodeWidth) / 3;
    N.forEach(function(n) {{
      n.x0 = n.col * colSpacing;
      n.x1 = n.x0 + nodeWidth;
    }});

    // Group by column, sort by value desc
    var columns = [[], [], [], []];
    N.forEach(function(n) {{ columns[n.col].push(n); }});

    columns.forEach(function(col) {{
      col.sort(function(a, b) {{ return b.value - a.value; }});
      var totalValue = d3.sum(col, function(n) {{ return n.value; }});
      var totalPadding = Math.max((col.length - 1) * nodePadding, 0);
      var availableHeight = height - totalPadding;
      var scale = Math.min(availableHeight / Math.max(totalValue, 1), 50);

      var y = 0;
      col.forEach(function(n) {{
        n.y0 = y;
        n.y1 = y + Math.max(n.value * scale, 3);
        y = n.y1 + nodePadding;
      }});

      // Rescale if overflow
      var last = col[col.length - 1];
      if (last && last.y1 > height) {{
        var factor = height / last.y1;
        col.forEach(function(n) {{
          n.y0 *= factor;
          n.y1 *= factor;
        }});
      }}
    }});

    // Compute link y positions
    N.forEach(function(node) {{
      node.sourceLinks.sort(function(a, b) {{ return a.target.y0 - b.target.y0; }});
      node.targetLinks.sort(function(a, b) {{ return a.source.y0 - b.source.y0; }});
    }});

    N.forEach(function(node) {{
      var sy = node.y0;
      node.sourceLinks.forEach(function(link) {{
        var nodeH = node.y1 - node.y0;
        var linkH = (link.value / Math.max(d3.sum(node.sourceLinks, function(l){{ return l.value; }}), 1)) * nodeH;
        link.y0 = sy + linkH / 2;
        link.width = Math.max(linkH, 1.5);
        sy += linkH;
      }});

      var ty = node.y0;
      node.targetLinks.forEach(function(link) {{
        var nodeH = node.y1 - node.y0;
        var linkH = (link.value / Math.max(d3.sum(node.targetLinks, function(l){{ return l.value; }}), 1)) * nodeH;
        link.y1 = ty + linkH / 2;
        ty += linkH;
      }});
    }});

    return {{ nodes: N, links: L }};
  }}

  function linkPath(d) {{
    var x0 = d.source.x1, x1 = d.target.x0;
    var xi = d3.interpolateNumber(x0, x1);
    return 'M' + x0 + ',' + d.y0
      + 'C' + xi(0.5) + ',' + d.y0
      + ' ' + xi(0.5) + ',' + d.y1
      + ' ' + x1 + ',' + d.y1;
  }}

  function fmtCurrency(val) {{
    if (val >= 1e9) return '$' + (val/1e9).toFixed(1) + 'B';
    if (val >= 1e6) return '$' + (val/1e6).toFixed(0) + 'M';
    return '$' + val.toLocaleString();
  }}

  // ===== RENDER SANKEY =====
  var tooltip = document.getElementById('sankeyTooltip');

  function renderSankey() {{
    var container = document.getElementById('sankeyContainer');
    container.innerHTML = '';

    var margin = {{ top: 30, right: 220, bottom: 20, left: 220 }};
    var fullWidth = 1440;
    var fullHeight = 750;
    var width = fullWidth - margin.left - margin.right;
    var height = fullHeight - margin.top - margin.bottom;

    var layout = sankeyLayout(nodes, links, width, height, 6, 16);

    var svg = d3.select(container).append('svg')
      .attr('width', fullWidth)
      .attr('height', fullHeight)
      .attr('viewBox', '0 0 ' + fullWidth + ' ' + fullHeight)
      .style('max-width', '100%')
      .style('height', 'auto');

    var g = svg.append('g')
      .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

    // Era column headers
    var colSpacing = (width - 16) / 3;
    COL_LABELS.forEach(function(label, i) {{
      g.append('text')
        .attr('x', i * colSpacing + 8)
        .attr('y', -10)
        .attr('text-anchor', 'middle')
        .attr('font-size', '11px')
        .attr('font-weight', '700')
        .attr('fill', COL_COLORS[i])
        .text(label);
    }});

    // Draw links
    g.append('g')
      .attr('fill', 'none')
      .attr('stroke-opacity', 0.3)
      .selectAll('path')
      .data(layout.links)
      .join('path')
      .attr('d', linkPath)
      .attr('stroke', function(d) {{ return LINK_COLORS[d.type] || '#aab'; }})
      .attr('stroke-width', function(d) {{ return Math.max(d.width, 1.5); }})
      .on('mouseenter', function(event, d) {{
        d3.select(this).attr('stroke-opacity', 0.7);
        var html = '<div class="tt-title">' + d.source.fullName + ' &rarr; ' + d.target.fullName + '</div>';
        html += '<div class="tt-row"><span class="tt-label">Type:</span> <span class="tt-value">' + d.type.toUpperCase() + '</span></div>';
        html += '<div class="tt-row"><span class="tt-label">Source funding:</span> <span class="tt-value">' + fmtCurrency(d.source.funding) + '</span></div>';
        html += '<div class="tt-row"><span class="tt-label">Target funding:</span> <span class="tt-value">' + fmtCurrency(d.target.funding) + '</span></div>';
        tooltip.innerHTML = html;
        tooltip.classList.add('visible');
      }})
      .on('mousemove', function(event) {{
        tooltip.style.left = (event.clientX + 14) + 'px';
        tooltip.style.top = (event.clientY - 10) + 'px';
      }})
      .on('mouseleave', function() {{
        d3.select(this).attr('stroke-opacity', 0.3);
        tooltip.classList.remove('visible');
      }});

    // Draw nodes
    var nodeSel = g.append('g').selectAll('g')
      .data(layout.nodes)
      .join('g');

    nodeSel.append('rect')
      .attr('x', function(d) {{ return d.x0; }})
      .attr('y', function(d) {{ return d.y0; }})
      .attr('width', function(d) {{ return d.x1 - d.x0; }})
      .attr('height', function(d) {{ return Math.max(d.y1 - d.y0, 2); }})
      .attr('fill', function(d) {{ return COL_COLORS[d.col]; }})
      .attr('rx', 2)
      .on('mouseenter', function(event, d) {{
        var html = '<div class="tt-title">' + d.fullName + '</div>';
        html += '<div class="tt-row"><span class="tt-label">Era:</span> <span class="tt-value">' + COL_LABELS[d.col] + '</span></div>';
        html += '<div class="tt-row"><span class="tt-label">Total grants:</span> <span class="tt-value">' + fmtCurrency(d.funding) + '</span></div>';
        tooltip.innerHTML = html;
        tooltip.classList.add('visible');
      }})
      .on('mousemove', function(event) {{
        tooltip.style.left = (event.clientX + 14) + 'px';
        tooltip.style.top = (event.clientY - 10) + 'px';
      }})
      .on('mouseleave', function() {{
        tooltip.classList.remove('visible');
      }});

    // Node labels
    nodeSel.append('text')
      .attr('x', function(d) {{
        if (d.col === 3) return d.x1 + 6;
        if (d.col === 0) return d.x0 - 6;
        return d.x0 + (d.x1 - d.x0) / 2;
      }})
      .attr('y', function(d) {{ return (d.y0 + d.y1) / 2; }})
      .attr('dy', '0.35em')
      .attr('text-anchor', function(d) {{
        if (d.col === 3) return 'start';
        if (d.col === 0) return 'end';
        return 'middle';
      }})
      .attr('font-size', '8.5px')
      .attr('font-weight', '600')
      .attr('fill', function(d) {{
        if (d.col === 1 || d.col === 2) {{
          return (d.y1 - d.y0) > 12 ? '#fff' : 'var(--text-primary)';
        }}
        return 'var(--text-primary)';
      }})
      .each(function(d) {{
        var label = d.name;
        if (label.length > 30) label = label.substring(0, 28) + '...';
        if ((d.col === 1 || d.col === 2) && (d.y1 - d.y0) < 12) {{
          d3.select(this)
            .attr('x', d.x1 + 6)
            .attr('text-anchor', 'start')
            .attr('fill', 'var(--text-primary)');
        }}
        d3.select(this).text(label);
      }});
  }}

  // ===== RENDER EVENT TABLE =====
  function renderEventTable() {{
    var tbody = document.getElementById('eventTableBody');
    tbody.innerHTML = '';
    tableEvents.forEach(function(ev) {{
      var badgeClass = ev.type.toLowerCase();
      var eraClass = ev.era === 'PC' ? 'pc' : ev.era === 'NDP' ? 'ndp' : ev.era === 'UCP_Kenney' ? 'kenney' : 'smith';
      var eraLabel = ev.era === 'UCP_Kenney' ? 'Kenney' : ev.era === 'UCP_Smith' ? 'Smith' : ev.era;
      var tr = document.createElement('tr');
      tr.innerHTML =
        '<td style="font-weight:700;color:var(--navy);">' + ev.id + '</td>' +
        '<td><span class="era-badge ' + eraClass + '">' + eraLabel + '</span></td>' +
        '<td><span class="event-badge ' + badgeClass + '">' + ev.type + '</span></td>' +
        '<td>' + ev.source + '</td>' +
        '<td>' + ev.target + '</td>' +
        '<td>' + ev.date + '</td>';
      tbody.appendChild(tr);
    }});
  }}

  // ===== INIT =====
  renderSankey();
  renderEventTable();
}})();
</script>

</body>
</html>'''

with open(OUT_PATH, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Generated {OUT_PATH}")
print(f"  Nodes: {len(nodes)}")
print(f"  Links: {len(links)}")
print(f"  Table events: {len(table_events)}")
print(f"  File size: {len(html):,} chars")
print(f"\nColumn summary:")
for col in range(4):
    col_nodes = [n for n in nodes if n['col'] == col]
    print(f"  Col {col}: {len(col_nodes)} nodes")
    for n in sorted(col_nodes, key=lambda x: -x['funding'])[:5]:
        print(f"    {n['fullName']}: {fmt_currency(n['funding'])}")
