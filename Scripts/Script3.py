# --- Step 1: Import libraries ---
import pandas as pd

# --- Step 2: Load the cleaned CSV files ---
crop_df = pd.read_csv("/content/cleaned_crop_state_year.csv")
land_df = pd.read_csv("/content/cleaned_land_state_year.csv")

# --- Step 3: Define a function to simplify column names ---
def rename_columns(df):
    """
    Simplifies column names to short, easy-to-read forms.
    Keeps year prefix and converts rest to readable camel-style names.
    Example: '2018_2019_net_area_sown' → '2018_2019_Net_Area_Sown'
    """
    new_cols = {}
    for col in df.columns:
        # Skip simple columns
        if col.lower() in ["state", "year"]:
            new_cols[col] = col.capitalize()
            continue

        # Split year and metric
        parts = col.split("_")
        if len(parts) > 2 and parts[0].isdigit():
            year = "_".join(parts[:2])  # e.g. 2018_2019
            metric = "_".join(parts[2:])
        else:
            year, metric = "", col

        # Clean up metric name
        metric = metric.replace("reporting_area_for_lus", "Reporting_Area")
        metric = metric.replace("forests", "Forest_Area")
        metric = metric.replace("area_under_non_agricultural_uses", "Non_Agricultural_Use")
        metric = metric.replace("barren_and_unculturable_land", "Barren_Land")
        metric = metric.replace("not_available_for_cultivation_total", "Not_Available_For_Cultivation")
        metric = metric.replace("permanent_pasture_and_other_grazing_land", "Pasture_Grazing_Land")
        metric = metric.replace("land_under_misc_tree_crops_and_groves_not_included_in_net_area_sown", "Tree_Crop_Land")
        metric = metric.replace("culturable_waste_land", "Culturable_Waste_Land")
        metric = metric.replace("fallow_lands_other_than_current_fallows", "Other_Fallow_Land")
        metric = metric.replace("current_fallow", "Current_Fallow")
        metric = metric.replace("fallow_land_total", "Fallow_Land_Total")
        metric = metric.replace("net_area_sown", "Net_Area_Sown")
        metric = metric.replace("cropped_area", "Cropped_Area")
        metric = metric.replace("area_sown_more_than_once", "Area_Sown_More_Than_Once")

        # Create final name with year prefix
        new_name = f"{year}_{metric}" if year else metric
        new_cols[col] = new_name

    df.rename(columns=new_cols, inplace=True)
    return df

# --- Step 4: Apply renaming function ---
crop_df = rename_columns(crop_df)
land_df = rename_columns(land_df)

# --- Step 5: Save renamed versions ---
crop_df.to_csv("/content/renamed_crop_data.csv", index=False)
land_df.to_csv("/content/renamed_land_data.csv", index=False)

# --- Step 6: Preview and confirmation ---
print("✅ Column renaming complete.\n")
print("📘 Crop data columns:")
print(crop_df.columns.tolist()[:15], "...")

print("\n🌿 Land data columns:")
print(land_df.columns.tolist()[:15], "...")

print("\n✅ Files saved as:")
print("/content/renamed_crop_data.csv")
print("/content/renamed_land_data.csv")
