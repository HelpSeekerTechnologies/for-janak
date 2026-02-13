# Statistical Significance Tests -- Operation Lineage Audit

**Generated:** 2026-02-12 23:13:13
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
| **N (organizations)** | 106 | 2,716 |
| **Total NDP funding** | $3,457,050,427 | $8,233,028,596 |
| **Mean funding** | $32,613,683.27 | $3,031,306.55 |
| **Median funding** | $648,259.35 | $104,450.87 |
| **Std deviation** | $153,783,839.54 | $26,671,683.33 |
| **Min** | $750.00 | $-92,971.20 |
| **Q1 (25th pct)** | $49,845.28 | $25,656.00 |
| **Q3 (75th pct)** | $5,182,600.46 | $466,970.02 |
| **IQR** | $5,132,755.17 | $441,314.02 |
| **Max** | $805,342,162.84 | $421,008,423.52 |

**Ratio of means (clustered / non-clustered):** 10.76x
**Ratio of medians (clustered / non-clustered):** 6.21x

---

## 3. Test Results

### 3a. Mann-Whitney U Test (primary)

The Mann-Whitney U test is a non-parametric test that compares the rank distributions of two independent samples. It does not assume normality, making it appropriate for the highly skewed funding distributions observed here.

| Parameter | Value |
|-----------|-------|
| **U statistic (two-sided)** | 182,506 |
| **p-value (two-sided)** | 2.797853e-06 |
| **Significance (two-sided)** | p < 0.001 (***) |
| **U statistic (greater)** | 182,506 |
| **p-value (one-sided, greater)** | 1.398927e-06 |
| **Significance (one-sided)** | p < 0.001 (***) |

### 3b. Kolmogorov-Smirnov Two-Sample Test (confirmatory)

The KS test checks whether two samples come from the same underlying distribution. It is sensitive to differences in both location and shape of the distributions.

| Parameter | Value |
|-----------|-------|
| **KS statistic (D)** | 0.286082 |
| **p-value** | 6.990780e-08 |
| **Significance** | p < 0.001 (***) |

### 3c. Effect Size

| Parameter | Value |
|-----------|-------|
| **Rank-biserial correlation (r)** | -0.2679 (absolute value: 0.2679) |
| **Effect magnitude** | **small-to-medium** |
| **Common Language Effect Size (CLES)** | 0.6339 (63.4%) |

The rank-biserial correlation is computed as: **r = 1 - 2U / (n1 * n2)**

The CLES represents the probability that a randomly selected clustered organization received more NDP-era funding than a randomly selected non-clustered organization.

---

## 4. Interpretation

### Result: REJECT H_0

The Mann-Whitney U test yields a p-value of **2.797853e-06**, which is far below the conventional alpha thresholds:

| Threshold | Met? |
|-----------|------|
| p < 0.05 | **YES** |
| p < 0.01 | **YES** |
| p < 0.001 | **YES** |

The Kolmogorov-Smirnov test independently confirms this finding (p = 6.990780e-08).

**In plain language:** The probability of observing a funding disparity this large (or larger) between clustered and non-clustered organizations by random chance alone is effectively zero. The difference is statistically significant at all conventional significance levels.

The rank-biserial correlation has an absolute value of **0.2679**, indicating a **small-to-medium** effect size. The negative sign arises from scipy's U convention (large U = first sample ranks higher), so the absolute value is the meaningful measure. The Common Language Effect Size of **63.4%** means that if you randomly pick one clustered organization and one non-clustered organization, there is a 63.4% chance the clustered organization received more NDP-era funding -- substantially above the 50% expected under the null hypothesis.


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
Mann-Whitney U (two-sided):  U = 182,506,  p = 2.797853e-06
Mann-Whitney U (greater):    U = 182,506,  p = 1.398927e-06
Kolmogorov-Smirnov:          D = 0.286082,  p = 6.990780e-08
Rank-biserial correlation:   r = -0.267857  (small)
CLES:                        0.6339
n_clustered = 106
n_non_clustered = 2,716
```
