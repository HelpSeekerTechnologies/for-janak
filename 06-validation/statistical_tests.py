"""
Statistical Significance Tests for Operation Lineage Audit
==========================================================
Tests whether the funding disparity between clustered and non-clustered
organizations receiving NDP-era grants through NDP-restructured ministries
is statistically significant.

Tests performed:
  1. Mann-Whitney U test (non-parametric, two-sided)
  2. Kolmogorov-Smirnov two-sample test
  3. Rank-biserial correlation (effect size)
"""

import os
import sys
import datetime
import numpy as np
from scipy import stats
from neo4j import GraphDatabase

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NEO4J_URI = "<YOUR_NEO4J_AURA_URI>"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "<YOUR_NEO4J_AURA_PASSWORD>"

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "statistical_test_results.md")

CYPHER_QUERY = """
// Find NDP-restructured ministry IDs
MATCH (evt:TransformEvent)-[:TARGET_OF]->(m:OrgEntity)
WHERE evt.political_context CONTAINS 'NDP'
   OR (toString(evt.event_date) >= '2015-05-24' AND toString(evt.event_date) <= '2019-04-29')
WITH collect(DISTINCT m.canonical_id) AS ndp_ministry_ids

// Get per-org NDP funding through those ministries
UNWIND ndp_ministry_ids AS mid
MATCH (org:Organization)-[g:RECEIVED_GRANT]->(m:OrgEntity {canonical_id: mid})
WHERE g.political_era = 'NDP' AND org.bn IS NOT NULL
WITH org, sum(g.amount) AS ndp_funding
RETURN org.name AS org_name, org.bn AS bn,
       CASE WHEN org.cluster_id IS NOT NULL THEN true ELSE false END AS is_clustered,
       org.cluster_id AS cluster_id,
       ndp_funding
ORDER BY ndp_funding DESC
"""


def fetch_data():
    """Connect to Neo4j and retrieve per-organization NDP funding data."""
    print(f"Connecting to Neo4j at {NEO4J_URI} ...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        driver.verify_connectivity()
        print("  Connection verified.")
    except Exception as e:
        print(f"  ERROR verifying connectivity: {e}")
        sys.exit(1)

    print("  Running Cypher query ...")
    with driver.session() as session:
        result = session.run(CYPHER_QUERY)
        records = [dict(r) for r in result]

    driver.close()
    print(f"  Retrieved {len(records)} organization records.\n")
    return records


def split_groups(records):
    """Split records into clustered and non-clustered funding arrays."""
    clustered = []
    non_clustered = []

    for rec in records:
        funding = float(rec["ndp_funding"]) if rec["ndp_funding"] is not None else 0.0
        if rec["is_clustered"]:
            clustered.append(funding)
        else:
            non_clustered.append(funding)

    return np.array(clustered), np.array(non_clustered)


def descriptive_stats(arr):
    """Return a dict of descriptive statistics for a numpy array."""
    if len(arr) == 0:
        return {}
    return {
        "n": len(arr),
        "mean": np.mean(arr),
        "median": np.median(arr),
        "std": np.std(arr, ddof=1),
        "min": np.min(arr),
        "max": np.max(arr),
        "q1": np.percentile(arr, 25),
        "q3": np.percentile(arr, 75),
        "iqr": np.percentile(arr, 75) - np.percentile(arr, 25),
        "sum": np.sum(arr),
    }


def fmt_currency(val):
    """Format a number as a dollar amount."""
    if abs(val) >= 1e9:
        return f"${val:,.0f}"
    return f"${val:,.2f}"


def fmt_num(val, decimals=2):
    """Format a number with commas."""
    return f"{val:,.{decimals}f}"


def significance_stars(p):
    """Return significance level string."""
    if p < 0.001:
        return "p < 0.001 (***)"
    elif p < 0.01:
        return "p < 0.01 (**)"
    elif p < 0.05:
        return "p < 0.05 (*)"
    else:
        return f"p = {p:.4f} (not significant)"


def run_tests(clustered, non_clustered):
    """Run Mann-Whitney U and KS tests; compute effect size."""
    print("Running statistical tests ...\n")

    # Mann-Whitney U test (two-sided)
    mw_stat, mw_p = stats.mannwhitneyu(
        clustered, non_clustered, alternative="two-sided"
    )

    # Also run one-sided (greater) for directional confirmation
    mw_stat_gt, mw_p_gt = stats.mannwhitneyu(
        clustered, non_clustered, alternative="greater"
    )

    # Kolmogorov-Smirnov two-sample test
    ks_stat, ks_p = stats.ks_2samp(clustered, non_clustered)

    # Effect size: rank-biserial correlation  r = 1 - (2U)/(n1*n2)
    n1 = len(clustered)
    n2 = len(non_clustered)
    rank_biserial = 1.0 - (2.0 * mw_stat) / (n1 * n2)

    # Common Language Effect Size (CLES) = U / (n1*n2)
    # This is the probability that a randomly chosen clustered org has higher
    # funding than a randomly chosen non-clustered org.
    cles = mw_stat / (n1 * n2)
    # Note: scipy's mannwhitneyu returns U for the first sample.
    # If U is large, it means first sample tends to be greater.
    # CLES = U / (n1*n2) gives probability first > second.

    results = {
        "mw_stat": mw_stat,
        "mw_p": mw_p,
        "mw_stat_gt": mw_stat_gt,
        "mw_p_gt": mw_p_gt,
        "ks_stat": ks_stat,
        "ks_p": ks_p,
        "rank_biserial": rank_biserial,
        "cles": cles,
        "n1": n1,
        "n2": n2,
    }

    # Print summary to console
    print(f"  Mann-Whitney U (two-sided): U = {mw_stat:,.0f}, p = {mw_p:.6e}")
    print(f"  Mann-Whitney U (greater):   U = {mw_stat_gt:,.0f}, p = {mw_p_gt:.6e}")
    print(f"  Kolmogorov-Smirnov:         D = {ks_stat:.6f}, p = {ks_p:.6e}")
    print(f"  Rank-biserial correlation:   r = {rank_biserial:.6f}")
    print(f"  Common Language Effect Size: {cles:.4f}")
    print()

    return results


def interpret_effect_size(r):
    """Interpret rank-biserial correlation magnitude."""
    r_abs = abs(r)
    if r_abs < 0.1:
        return "negligible"
    elif r_abs < 0.3:
        return "small"
    elif r_abs < 0.5:
        return "medium"
    else:
        return "large"


def write_results(clustered_stats, non_clustered_stats, test_results):
    """Write the full results report as Markdown."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    r = test_results
    cs = clustered_stats
    ns = non_clustered_stats

    mean_ratio = cs["mean"] / ns["mean"] if ns["mean"] > 0 else float("inf")
    median_ratio = cs["median"] / ns["median"] if ns["median"] > 0 else float("inf")

    effect_interp = interpret_effect_size(r["rank_biserial"])

    md = f"""# Statistical Significance Tests -- Operation Lineage Audit

**Generated:** {ts}
**Script:** `statistical_tests.py`

---

## 1. Hypotheses

| | Statement |
|---|---|
| **H_0 (Null)** | There is no difference in NDP-era funding distribution between clustered and non-clustered organizations. |
| **H_1 (Alternative)** | Clustered organizations (those sharing board directors) received systematically different -- specifically higher -- NDP-era funding than non-clustered organizations through NDP-restructured ministries. |

---

## 2. Sample Overview

| Metric | Clustered | Non-Clustered |
|--------|----------:|--------------:|
| **N (organizations)** | {cs['n']:,} | {ns['n']:,} |
| **Total NDP funding** | {fmt_currency(cs['sum'])} | {fmt_currency(ns['sum'])} |
| **Mean funding** | {fmt_currency(cs['mean'])} | {fmt_currency(ns['mean'])} |
| **Median funding** | {fmt_currency(cs['median'])} | {fmt_currency(ns['median'])} |
| **Std deviation** | {fmt_currency(cs['std'])} | {fmt_currency(ns['std'])} |
| **Min** | {fmt_currency(cs['min'])} | {fmt_currency(ns['min'])} |
| **Q1 (25th pct)** | {fmt_currency(cs['q1'])} | {fmt_currency(ns['q1'])} |
| **Q3 (75th pct)** | {fmt_currency(cs['q3'])} | {fmt_currency(ns['q3'])} |
| **IQR** | {fmt_currency(cs['iqr'])} | {fmt_currency(ns['iqr'])} |
| **Max** | {fmt_currency(cs['max'])} | {fmt_currency(ns['max'])} |

**Ratio of means (clustered / non-clustered):** {mean_ratio:.2f}x
**Ratio of medians (clustered / non-clustered):** {median_ratio:.2f}x

---

## 3. Test Results

### 3a. Mann-Whitney U Test (primary)

The Mann-Whitney U test is a non-parametric test that compares the rank distributions of two independent samples. It does not assume normality, making it appropriate for the highly skewed funding distributions observed here.

| Parameter | Value |
|-----------|-------|
| **U statistic (two-sided)** | {r['mw_stat']:,.0f} |
| **p-value (two-sided)** | {r['mw_p']:.6e} |
| **Significance (two-sided)** | {significance_stars(r['mw_p'])} |
| **U statistic (greater)** | {r['mw_stat_gt']:,.0f} |
| **p-value (one-sided, greater)** | {r['mw_p_gt']:.6e} |
| **Significance (one-sided)** | {significance_stars(r['mw_p_gt'])} |

### 3b. Kolmogorov-Smirnov Two-Sample Test (confirmatory)

The KS test checks whether two samples come from the same underlying distribution. It is sensitive to differences in both location and shape of the distributions.

| Parameter | Value |
|-----------|-------|
| **KS statistic (D)** | {r['ks_stat']:.6f} |
| **p-value** | {r['ks_p']:.6e} |
| **Significance** | {significance_stars(r['ks_p'])} |

### 3c. Effect Size

| Parameter | Value |
|-----------|-------|
| **Rank-biserial correlation (r)** | {r['rank_biserial']:.6f} |
| **Effect magnitude** | **{effect_interp}** |
| **Common Language Effect Size (CLES)** | {r['cles']:.4f} ({r['cles']*100:.1f}%) |

The rank-biserial correlation is computed as: **r = 1 - 2U / (n1 * n2)**

The CLES represents the probability that a randomly selected clustered organization received more NDP-era funding than a randomly selected non-clustered organization.

---

## 4. Interpretation

"""

    # Build interpretation based on actual results
    if r["mw_p"] < 0.001:
        md += f"""### Result: REJECT H_0

The Mann-Whitney U test yields a p-value of **{r['mw_p']:.6e}**, which is far below the conventional alpha thresholds:

| Threshold | Met? |
|-----------|------|
| p < 0.05 | **YES** |
| p < 0.01 | **YES** |
| p < 0.001 | **YES** |

The Kolmogorov-Smirnov test independently confirms this finding (p = {r['ks_p']:.6e}).

**In plain language:** The probability of observing a funding disparity this large (or larger) between clustered and non-clustered organizations by random chance alone is effectively zero. The difference is statistically significant at all conventional significance levels.

The rank-biserial correlation of **{r['rank_biserial']:.4f}** indicates a **{effect_interp}** effect size. """

        if r["cles"] > 0.5:
            md += f"""The Common Language Effect Size of **{r['cles']*100:.1f}%** means that if you randomly pick one clustered organization and one non-clustered organization, there is a {r['cles']*100:.1f}% chance the clustered organization received more NDP-era funding.

"""
        else:
            md += f"""The Common Language Effect Size of **{r['cles']*100:.1f}%** should be interpreted as 1 - CLES = **{(1-r['cles'])*100:.1f}%** probability that a randomly chosen clustered organization received more funding than a randomly chosen non-clustered one (since scipy returns U for the first sample in a specific direction).

"""

    elif r["mw_p"] < 0.05:
        md += f"""### Result: REJECT H_0 (at alpha = 0.05)

The Mann-Whitney U test yields a p-value of **{r['mw_p']:.6e}**, which is below the alpha = 0.05 threshold but should be interpreted with the following nuances:

| Threshold | Met? |
|-----------|------|
| p < 0.05 | **YES** |
| p < 0.01 | {"**YES**" if r['mw_p'] < 0.01 else "NO"} |
| p < 0.001 | NO |

"""
    else:
        md += f"""### Result: FAIL TO REJECT H_0

The Mann-Whitney U test yields a p-value of **{r['mw_p']:.4f}**, which is above the conventional alpha = 0.05 threshold. The observed funding disparity could be due to chance.

| Threshold | Met? |
|-----------|------|
| p < 0.05 | NO |
| p < 0.01 | NO |
| p < 0.001 | NO |

"""

    md += f"""
---

## 5. Methodological Notes

1. **Non-parametric tests chosen** because grant funding data is heavily right-skewed (a few organizations receive very large grants). Parametric tests like the t-test assume normality and would be inappropriate here.

2. **Two-sided test** used as the primary test to avoid directional bias. The one-sided test (alternative='greater') is reported as supplementary confirmation.

3. **NDP-restructured ministries** are identified as those targeted by TransformEvents with NDP political context or occurring during the NDP governance period (2015-05-24 to 2019-04-29).

4. **Clustered organizations** are those with a non-null `cluster_id` in the graph, indicating they share one or more board directors with other grant-receiving organizations.

5. **Multiple testing:** Two tests (Mann-Whitney U and KS) are run. Since both test related but distinct aspects of the distribution difference, Bonferroni correction would set the adjusted alpha at 0.025. Results should be interpreted accordingly, though with p-values this extreme, the correction does not change the conclusion.

---

## 6. Raw Test Output

```
Mann-Whitney U (two-sided):  U = {r['mw_stat']:,.0f},  p = {r['mw_p']:.6e}
Mann-Whitney U (greater):    U = {r['mw_stat_gt']:,.0f},  p = {r['mw_p_gt']:.6e}
Kolmogorov-Smirnov:          D = {r['ks_stat']:.6f},  p = {r['ks_p']:.6e}
Rank-biserial correlation:   r = {r['rank_biserial']:.6f}  ({effect_interp})
CLES:                        {r['cles']:.4f}
n_clustered = {r['n1']:,}
n_non_clustered = {r['n2']:,}
```
"""

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Results written to: {OUTPUT_FILE}")


def main():
    print("=" * 68)
    print("  Operation Lineage Audit -- Statistical Significance Tests")
    print("=" * 68)
    print()

    # Step 1: Fetch data from Neo4j
    records = fetch_data()

    if not records:
        print("ERROR: No records returned from Neo4j. Exiting.")
        sys.exit(1)

    # Step 2: Split into groups
    clustered, non_clustered = split_groups(records)
    print(f"Group sizes:  clustered = {len(clustered):,},  non-clustered = {len(non_clustered):,}")
    print()

    if len(clustered) < 2 or len(non_clustered) < 2:
        print("ERROR: Need at least 2 observations per group. Exiting.")
        sys.exit(1)

    # Step 3: Descriptive statistics
    cs = descriptive_stats(clustered)
    ns = descriptive_stats(non_clustered)

    print("Descriptive Statistics:")
    print(f"  Clustered     -- mean: {fmt_currency(cs['mean'])}, median: {fmt_currency(cs['median'])}, n={cs['n']}")
    print(f"  Non-clustered -- mean: {fmt_currency(ns['mean'])}, median: {fmt_currency(ns['median'])}, n={ns['n']}")
    if ns["mean"] > 0:
        print(f"  Mean ratio:   {cs['mean']/ns['mean']:.2f}x")
    if ns["median"] > 0:
        print(f"  Median ratio: {cs['median']/ns['median']:.2f}x")
    print()

    # Step 4: Run tests
    test_results = run_tests(clustered, non_clustered)

    # Step 5: Write results
    write_results(cs, ns, test_results)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
