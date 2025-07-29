# Automated Precinct‑Level Pivots (AZ)

These notebooks generate recurring, precinct‑level pivot tables we’re frequently asked for. Each flow reads from BigQuery source tables, aggregates to **pctnum × county × district** grain, pivots requested dimensions (age, income, race, and survey Q/A), then writes a clean table back to BigQuery for reporting.

## Contents

* `age_counts_pivot_legacy.ipynb` → `rich_christina_proj.age_counts_pivot`
* `income_counts_legacy.ipynb` → `rich_christina_proj.income_counts_pivot`
* `race_counts_pivot_legacy.ipynb` → `rich_christina_proj.race_counts_pivot`
* `gotvid_counts_pivot_legacy.ipynb` → `rich_christina_proj.gotvid_counts_pivot`

All notebooks target the GCP project:

```
prod-organize-arizon-4e1c0a83
```

and use the precinct crosswalk:
(this is a crosswalk I have made toget around irregular AZ precinct naming conventions, the purpose is to join the catalist precincts to my diy pctnum codes which you can find in my [az_precincts repo](https://github.com/cmarikos/az_precincts))
```
prod-organize-arizon-4e1c0a83.rich_christina_proj.catalist_pctnum_crosswalk_native
```

---

## Prerequisites

* **Access & Permissions**

  * BigQuery read access to the referenced `proj-tmc-mem-mvp` and `prod-organize-arizon-4e1c0a83` datasets.
  * BigQuery write access (Data Editor) on `prod-organize-arizon-4e1c0a83.rich_christina_proj` for table creation/replace.

* **Environment**

  * Python 3 with the following packages available to the notebook runtime:

    * `google-cloud-bigquery`
    * `pandas`
    * `pandas-gbq`
  * Authenticated Google Cloud credentials in the environment where the notebooks run (e.g., Colab or a local Jupyter environment already authenticated).

> **Note:** These notebooks assume **Application Default Credentials** are present and will use them when instantiating `bigquery.Client(project="prod-organize-arizon-4e1c0a83")`.

---

## How to Run

1. **Open a notebook** (Colab or Jupyter) from this repo - I'm using Jupyter so I can write within the same BigQuery console environment
2. **Run all cells top‑to‑bottom** without skipping.

   * Each file:

     * Pulls source rows with a SQL query.
     * Builds a Pandas pivot with **fill\_value=0**.
     * Sanitizes column names to BigQuery‑safe snake\_case.
     * Writes the result with `pandas_gbq.to_gbq(..., if_exists='replace')`.
3. **Verify the output table** is created/overwritten in:

   * `prod-organize-arizon-4e1c0a83.rich_christina_proj.*` (see table names below).

> If you see `NameError: name 'pivot_df' is not defined`, re‑run the notebook from the top. That error appears when a later cell is executed before the pivot cell finishes.

---

## Data Flows (What Each Notebook Does)

### 1) Age Counts → `rich_christina_proj.age_counts_pivot`

* **Sources**

  * `proj-tmc-mem-mvp.catalist_enhanced.enh_catalist__ntl_current` (`a`)
  * `proj-tmc-mem-mvp.catalist_cleaned.cln_catalist__district` (`b`)
  * `rich_christina_proj.catalist_pctnum_crosswalk_native` (`c`)
* **Filter**: `b.state = 'AZ'`
* **Grain / Index**: `pctnum, county, congressionaldistrict, statehousedistrict`
* **Pivot Columns (buckets)**

  * `under 18`, `18-24`, `25-34`, `35-44`, `45-54`, `55-64`, `65+`
* **Metric**: `COUNT(a.dwid)` as `DWIDS`
* **Notes**

  * Buckets are computed via a `CASE` on `a.age`.
  * Columns are sanitized to snake\_case (e.g., `under_18`, `65_` → `65` becomes `65` with trailing underscore removed by `.str.strip('_')`).

---

### 2) Income Counts → `rich_christina_proj.income_counts_pivot`

* **Sources**

  * `proj-tmc-mem-mvp.catalist_cleaned.cln_catalist__models` (`a`)
  * `proj-tmc-mem-mvp.catalist_cleaned.cln_catalist__district` (`b`)
  * `rich_christina_proj.catalist_pctnum_crosswalk_native` (`c`)
* **Filter**: `b.state = 'AZ'`
* **Grain / Index**: `pctnum, county, congressionaldistrict, statehousedistrict`
* **Pivot Columns**: `a.catalistmodel_income_bin`, rendered (and then sanitized) in this order:

  * `Less than $20,000`
  * `$20,000 - $30,000`
  * `$30,000 - $50,000`
  * `$50,000 - $75,000`
  * `$75,000 - $100,000`
  * `$100,000 - $150,000`
  * `Greater than $150,000`
* **Metric**: `COUNT(a.dwid)` as `DWIDS`

---

### 3) Race Counts → `rich_christina_proj.race_counts_pivot`

* **Sources**

  * `proj-tmc-mem-mvp.catalist_enhanced.enh_catalist__ntl_current` (`n`)
  * `proj-tmc-mem-mvp.catalist_cleaned.cln_catalist__district` (`d`)
  * `rich_christina_proj.catalist_pctnum_crosswalk_native` (`p`)
* **Filter**: `d.state = 'AZ'` and `p.pctnum IS NOT NULL`
* **Grain / Index**: `pctnum, countyname, congressionaldistrict, statehousedistrict`
* **Pivot Columns**: `n.race`
* **Metric**: `COUNT(DISTINCT n.dwid)` as `people_count`
* **Notes**

  * This notebook demonstrates the `%%bigquery` cell magic to materialize results into a DataFrame, then pivots.

---

### 4) Survey Question/Response Counts → `rich_christina_proj.gotvid_counts_pivot`

* **Sources**

  * `prod-organize-arizon-4e1c0a83.ngpvan.CTARAA_ContactsSurveyResponses_VF` (`cs`)
  * `prod-organize-arizon-4e1c0a83.ngpvan.CTARAA_SurveyQuestions` (`sq`)
  * `prod-organize-arizon-4e1c0a83.ngpvan.CTARAA_SurveyResponses` (`sr`)
  * `prod-organize-arizon-4e1c0a83.raze_raza_dwid_to_van_id_crosswalk.dwid_vanid_crosswalk` (`cw`)
  * `proj-tmc-mem-mvp.catalist_cleaned.cln_catalist__district` (`d`)
  * `rich_christina_proj.catalist_pctnum_crosswalk_native` (`c`)
* **Filters**

  * `sq.Cycle = '2024'`
  * `c.county IN ('COCONINO','COCHISE','MOHAVE','PINAL','PIMA','YAVAPAI','YUMA')`
* **Grain / Index**: `pctnum, county, congressionaldistrict, statehousedistrict`
* **Pivot Columns**: combined `SurveyQuestionName — SurveyResponseName` (then sanitized to snake\_case)
* **Metric**: `COUNT(cw.DWID)` as `response_count`
* **Notes**

  * The notebook creates a `question_response` column (`Question — Response`) to pivot.

---

## Output Tables (BigQuery)

All outputs are written with `if_exists='replace'`:

* `prod-organize-arizon-4e1c0a83.rich_christina_proj.age_counts_pivot`
* `prod-organize-arizon-4e1c0a83.rich_christina_proj.income_counts_pivot`
* `prod-organize-arizon-4e1c0a83.rich_christina_proj.race_counts_pivot`
* `prod-organize-arizon-4e1c0a83.rich_christina_proj.gotvid_counts_pivot`

Each table includes the shared index fields:

* `pctnum`
* county field (`county` or `countyname`, depending on source)
* `congressionaldistrict`
* `statehousedistrict`

…and a set of **pivoted metric columns** (snake\_case) whose values are integer counts.

---

## Conventions & Implementation Details

* **Column name sanitation**
  After pivoting, columns are normalized with:

  * `[^\\w]+` → `_`
  * leading/trailing `_` trimmed
  * lowercased

* **Zero‑fill**
  Missing combinations are represented as `0` (via `fill_value=0` in the pivot).

* **Overwrite behavior**
  `to_gbq(..., if_exists='replace')` will drop and recreate the destination table on each run.

* **County field name**
  Some sources expose `county` vs `countyname`. Notebooks preserve the upstream name when indexing.

---

## Common Issues

* **`NameError: name 'pivot_df' is not defined`**
  Cause: A later cell ran before the pivot was created.
  Fix: Restart the kernel and **Run all**.

* **Permission denied on write**
  Ensure your principal has BigQuery **Data Editor** on dataset `rich_christina_proj`.

* **Unexpected/missing pivot columns**
  If upstream categories change (e.g., a new income bin), update the ordered list in the notebook before sanitization so columns appear in the desired order.

---

## Change or Fork Safely

If you adapt these notebooks for another project/dataset:

* Update the hard‑coded **project** and **destination table** names near the `to_gbq` call.
* Create the pctnum code catalist crosswalk using [az_precincts repo](https://github.com/cmarikos/az_precincts) and catalist county plus uniqueprecinctcode this facilitates mapping in Looker Studio using the protocol outlined in [my geo-precincts repo](https://github.com/cmarikos/geo-precincts)


---

## Purpose

These tables centralize frequently requested counts so that downstream reports, Looker Studio dashboards, or ad‑hoc requests can read a single, tidy, precinct‑level table per topic—without re‑running heavy joins each time.

---

*Maintainer:* Christina Marikos, christina@ruralazacti0on.org

