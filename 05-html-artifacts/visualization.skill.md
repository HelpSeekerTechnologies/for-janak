# Visualization Skill

Generate self-contained interactive HTML dashboards for demo presentation. Each artifact uses inline CSS/JS (no external dependencies), HelpSeeker 2026 brand styling, and includes data provenance footers.

> **Upstream dependency:** `kgl-skill/brand/helpseeker-brand.skill.md`, `kgl-skill/brand/kgl-iconography/SKILL.md`

---

## When to Use

- Building demo-ready HTML artifacts from graph query results
- Creating the process-overview animation showing agent pipeline
- Generating Sankey, force-directed, and heatmap visualizations
- Packaging results for non-technical stakeholder presentation

---

## 5 Target Artifacts

### 1. `00-process-overview.html` — Pipeline Animation
Shows all 13 agents across 5 phases with animated data flow arrows.
- Phase boxes with agent names
- Dependency arrows (solid = data dependency, dashed = sequential)
- Status indicators (green = complete, yellow = in progress, grey = pending)
- Expandable agent cards showing inputs, logic, outputs

### 2. `01-ministry-lineage-political.html` — Sankey Diagram
Ministry flow across political eras with grant volume as edge width.
- Left: pre-NDP ministries → NDP restructuring events → NDP-era ministries
- Center: UCP restructuring events → UCP-era ministries
- Edge width = total grants through that ministry in that era
- Color: NDP = orange, UCP = blue, PC = grey
- Hover: show total grants, top recipients, OIC citation

### 3. `02-director-donation-network.html` — D3 Force Graph
Director → Organization → Ministry → Political Party network.
- Director nodes (◎ person): sized by n_boards
- Organization nodes (ᚴ): sized by total grants, colored by risk flag count
- Ministry nodes (▣ program): colored by political era
- Political Party nodes (✠ authority): NDP orange, UCP blue
- Edge types: SITS_ON (grey), RECEIVED_GRANT (green width=amount), DONATED_TO (red)
- Filter controls: show/hide by era, flag count, cluster

### 4. `03-cluster-funding-heatmap.html` — Era Heatmap
Governance clusters × fiscal years, colored by funding volume.
- Rows: top 25 governance clusters (sorted by NDP-era funding)
- Columns: 11 fiscal years (FY2014-15 through FY2024-25)
- Color intensity: funding volume (darker = more)
- Political era bands: shaded background columns
- Click cell: expand to show org-level breakdown

### 5. `04-governance-executive.html` — Executive Dashboard
The headline numbers and evidence chain in a single-page dashboard.
- Hero metrics: total NDP-era funding through restructured ministries, # flagged orgs, # director-donation links
- Three finding cards with "So What" chains
- Evidence chain visualization (claim → source document links)
- Symmetry comparison panel (NDP vs UCP side by side)
- Download buttons for CSVs and full report

---

## HelpSeeker 2026 Brand

```css
:root {
    --primary-teal: #28bcb8;
    --aqua: #4bc2c3;
    --navy: #103656;
    --light-bg: #f8fffe;
    --text-primary: #1a1a2e;
    --text-secondary: #4a5568;
    --font-primary: 'Inter', 'Segoe UI', system-ui, sans-serif;
    --gradient-primary: linear-gradient(135deg, #28bcb8, #4bc2c3);
}
```

### KGL Glyph → Material Symbol Icons
```
▣ program  → 'account_balance' (ministry)
ᚴ organization → 'business' (charity)
◎ person → 'person' (director)
⧫ event → 'event' (transformation)
◉ resource → 'payments' (grant)
✠ authority → 'gavel' (political party)
⟡ measurement → 'flag' (risk flag)
```

---

## Self-Contained HTML Template

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{ARTIFACT_TITLE} — Operation Lineage Audit</title>
    <style>
        /* All CSS inline — no external dependencies */
        /* HelpSeeker 2026 brand variables */
        /* Responsive layout */
    </style>
</head>
<body>
    <header>
        <h1>{ARTIFACT_TITLE}</h1>
        <p class="subtitle">Operation Lineage Audit — HelpSeeker Technologies</p>
    </header>
    <main>
        <!-- Visualization content -->
    </main>
    <footer class="provenance">
        <h3>Data Provenance</h3>
        <ul>
            <li>Source: {data_source_files}</li>
            <li>Query date: {date}</li>
            <li>Confidence: {confidence_level}</li>
            <li>Agent: {generating_agent}</li>
        </ul>
        <details>
            <summary>How this was produced</summary>
            <p>{agent_description_and_methodology}</p>
        </details>
    </footer>
    <script>
        /* All JS inline — D3.js, Chart.js, or vanilla as needed */
        /* For D3: include d3.min.js inline or from CDN */
    </script>
</body>
</html>
```

---

## Anti-Patterns

1. **External CSS/JS dependencies** — must work offline (self-contained)
2. **Missing provenance footer** — every artifact must show data source and generation method
3. **Political branding** — use HelpSeeker brand, NOT party colors for primary elements (party colors only for data encoding)
4. **Overwhelming detail** — executive dashboard should have 3-5 hero numbers, not 50 metrics
5. **No interactivity** — each artifact should have at least hover tooltips and one filter control
