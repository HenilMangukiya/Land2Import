# Run this in Google Colab
import re
import pandas as pd
from pathlib import Path

pd.set_option("display.max_columns", 120)

# -----------------------------
# User paths (change if needed)
# -----------------------------
LAND_PATH = "/content/renamed_land_data.csv"
CROP_PATH = "/content/renamed_crop_data.csv"
OUTPUT_PATH = "/content/final_state_year_land_crop_data.csv"

# -----------------------------
# Helper functions
# -----------------------------
def detect_state_column(df):
    """Try common names, otherwise pick best text-like candidate."""
    cols = df.columns.tolist()
    lowered = [c.lower() for c in cols]

    # Common candidates in order
    candidates = ["state", "state_name", "st_name", "state/ut", "state_ut", "region", "name"]
    for cand in candidates:
        for c in cols:
            if cand == c.lower() or cand in c.lower():
                print(f"Detected state column by name match: '{c}'")
                return c

    # Fallback: choose a non-numeric column with moderate unique count (likely region names)
    nrows = len(df)
    text_candidates = []
    for c in cols:
        # treat as text-like if dtype is object or many values are non-numeric
        sample = df[c].dropna().astype(str).head(200)
        non_numeric_frac = (sample.str.match(r'^-?\d+(\.\d+)?$') == False).mean() if len(sample)>0 else 0
        nunique = df[c].nunique(dropna=True)
        if non_numeric_frac > 0.6 and 2 < nunique < max(500, nrows/2):
            text_candidates.append((c, nunique))
    if text_candidates:
        # pick the one with highest distinct count (likely state names list)
        text_candidates.sort(key=lambda x: -x[1])
        chosen = text_candidates[0][0]
        print(f"Detected state column by fallback heuristic: '{chosen}'")
        return chosen

    # Last resort: return first column
    print(f"WARNING: Couldn't confidently detect a state column. Using first column '{cols[0]}' as State.")
    return cols[0]

def collect_year_metric_cols(df):
    """
    Return two lists:
      - cols_with_year: columns that include a 4digit_4digit year token like '2018_2019'
      - other_cols: columns that don't appear year-prefixed
    """
    cols_with_year = []
    other_cols = []
    for c in df.columns:
        if re.search(r'\b(19|20)\d{2}[_-](19|20)\d{2}\b', c):
            cols_with_year.append(c)
        else:
            other_cols.append(c)
    return cols_with_year, other_cols

def extract_year_from_col(colname):
    # find patterns like 2018_2019 or 2018-2019 or 2018/2019
    m = re.search(r'(19|20)\d{2}[_\-\/](19|20)\d{2}', colname)
    if m:
        return m.group(0).replace('-', '_').replace('/', '_')
    # try single year like 2018 (less likely)
    m2 = re.search(r'\b(19|20)\d{2}\b', colname)
    if m2:
        return m2.group(0)
    return None

def metric_keyword_match(colname, keywords):
    """Return True if any keyword found in colname (case-insensitive)."""
    ln = colname.lower()
    return any(k.lower() in ln for k in keywords)

# -----------------------------
# Load data
# -----------------------------
land_df = pd.read_csv(LAND_PATH, low_memory=False)
crop_df = pd.read_csv(CROP_PATH, low_memory=False)

print("Loaded files. Land cols:", len(land_df.columns), " Crop cols:", len(crop_df.columns))

# -----------------------------
# Detect State columns
# -----------------------------
land_state_col = detect_state_column(land_df)
crop_state_col = detect_state_column(crop_df)

# Normalize column name to 'State' in both dataframes for ease
land_df.rename(columns={land_state_col: "State"}, inplace=True)
crop_df.rename(columns={crop_state_col: "State"}, inplace=True)

# -----------------------------
# Handle Year presence:
# If a 'Year' column exists, we will prefer it; otherwise we will extract year from column names.
# -----------------------------
def prepare_long_totals(df, domain="land"):
    """
    domain: 'land' or 'crop'
    Returns: DataFrame with columns ['State','Year','Value'] where 'Value' is the metric to sum.
    For land: we will sum selected land-related metrics into one 'Value' per State-Year.
    For crop: we will sum selected crop-related metrics into one 'Value' per State-Year.
    """
    df = df.copy()
    # If there's an explicit Year column, normalize name
    year_col = None
    for c in df.columns:
        if c.lower() == "year":
            year_col = c
            break
    if year_col:
        df.rename(columns={year_col: "Year"}, inplace=True)
        has_year_col = True
        print(f"'{domain}': Found explicit Year column: '{year_col}'")
    else:
        has_year_col = False
        print(f"'{domain}': No explicit Year column found. Will extract years from column headers where possible.")

    # collect columns that include year token
    with_year, others = collect_year_metric_cols(df)

    # define keywords representing land / crop metrics
    if domain == "land":
        keywords = ["reporting_area", "net_area_sown", "reporting_area_for_lus", "forest", "fallow", "culturable", "pasture", "not_available_for_cultivation"]
    else:
        # crop domain
        keywords = ["cropped_area", "production", "yield", "production_total", "area_harvested", "production_of_all_crops"]

    # Choose columns to treat as metric columns:
    metric_cols = []
    # prefer year-prefixed columns that match keywords
    for c in with_year:
        if metric_keyword_match(c, keywords):
            metric_cols.append(c)

    # fallback: if no year-prefixed metric columns found, look in other columns for keywords
    if not metric_cols:
        for c in others:
            if metric_keyword_match(c, keywords):
                metric_cols.append(c)

    # If still empty, as last fallback consider numeric columns only (excluding State)
    if not metric_cols:
        for c in df.columns:
            if c in ("State", "Year"):
                continue
            # try convert sample to numeric to check
            sample = pd.to_numeric(df[c].dropna().head(50), errors="coerce")
            if sample.notna().sum() > 0:
                metric_cols.append(c)
        print(f"'{domain}': Fallback metric columns chosen (numeric-like): {metric_cols[:8]} ...")

    print(f"'{domain}': Metric columns used (count {len(metric_cols)}). Sample:", metric_cols[:8])

    # Build long-format table
    if has_year_col:
        # We assume metric columns are either single-value columns (no year in name) or multiple columns per year.
        # If metrics are year-prefixed as well, handle both:
        to_melt = [c for c in metric_cols if c in df.columns]
        if to_melt:
            melted = df.melt(id_vars=["State","Year"], value_vars=to_melt,
                             var_name="metric_col", value_name="value")
            # when metric_col contains a year token, extract year and override Year column if empty
            melted["inferred_year"] = melted["metric_col"].apply(lambda x: extract_year_from_col(x) if isinstance(x,str) else None)
            melted["Year"] = melted.apply(lambda r: r["Year"] if pd.notna(r["Year"]) and str(r["Year"]).strip()!="" else r["inferred_year"], axis=1)
            long = melted[["State","Year","value"]].copy()
        else:
            # no per-column metrics: maybe single production column exists
            long = df[["State","Year"] + metric_cols].copy()
            # if multiple metric_cols sum them into 'value'
            if len(metric_cols) > 1:
                long["value"] = long[metric_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
            else:
                long["value"] = pd.to_numeric(long[metric_cols[0]], errors="coerce")
            long = long[["State","Year","value"]]
    else:
        # No explicit Year column: melt all metric_cols and extract year from their column names
        to_melt = metric_cols
        melted = df.melt(id_vars=["State"], value_vars=to_melt, var_name="metric_col", value_name="value")
        melted["Year"] = melted["metric_col"].astype(str).apply(extract_year_from_col)
        # drop rows where year couldn't be inferred
        long = melted.dropna(subset=["Year"])[["State","Year","value"]].copy()

    # Clean Year format (replace - or / with _)
    long["Year"] = long["Year"].astype(str).str.replace("-", "_").str.replace("/", "_")
    # numeric convert
    long["value"] = pd.to_numeric(long["value"], errors="coerce").fillna(0)
    # Aggregate per State + Year
    agg = long.groupby(["State","Year"], as_index=False)["value"].sum()
    return agg

# -----------------------------
# Prepare aggregated totals
# -----------------------------
land_agg = prepare_long_totals(land_df, domain="land")
land_agg.rename(columns={"value": "Total_Land"}, inplace=True)

crop_agg = prepare_long_totals(crop_df, domain="crop")
crop_agg.rename(columns={"value": "Total_Crop_Production"}, inplace=True)

print("\nSample land_agg:")
print(land_agg.head(8))
print("\nSample crop_agg:")
print(crop_agg.head(8))

# -----------------------------
# Now merge on State + Year
# -----------------------------
# Before merging, normalize State strings (strip, uppercase)
def norm_state_col(df):
    df = df.copy()
    df["State"] = df["State"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    return df

land_agg = norm_state_col(land_agg)
crop_agg = norm_state_col(crop_agg)

merged = pd.merge(land_agg, crop_agg, on=["State","Year"], how="outer")

# fill NaN numeric with 0
merged["Total_Land"] = pd.to_numeric(merged["Total_Land"], errors="coerce").fillna(0)
merged["Total_Crop_Production"] = pd.to_numeric(merged["Total_Crop_Production"], errors="coerce").fillna(0)

# Sort and save
merged = merged.sort_values(["State","Year"]).reset_index(drop=True)
print("\nMerged preview:")
print(merged.head(15))

# Save output
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
merged.to_csv(OUTPUT_PATH, index=False)
print(f"\n✅ Final merged file written to: {OUTPUT_PATH}")
