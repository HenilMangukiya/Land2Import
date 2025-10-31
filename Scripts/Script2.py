# --- Step 1: Import libraries ---
import pandas as pd

# --- Step 2: Load both datasets ---
crop_df = pd.read_csv("/content/cleaned_crop_data.csv", low_memory=False)
land_df = pd.read_csv("/content/cleaned_land_data.csv", low_memory=False)

# --- Step 3: Function to clean column names ---
def clean_columns(df):
    """
    Simplifies messy long column names like:
    'classification_of_land_in_each_district_of_state_ut_for_the_year_2018_2019__hectare__classification_of_reporting_area_forests_forests_4'
    into clean format like:
    '2018_2019_forests'
    """
    new_cols = []
    for col in df.columns:
        # Extract year
        year = ""
        if "2015_2016" in col:
            year = "2015_2016"
        elif "2016_2017" in col:
            year = "2016_2017"
        elif "2017_2018" in col:
            year = "2017_2018"
        elif "2018_2019" in col:
            year = "2018_2019"
        elif "2019_2020" in col:
            year = "2019_2020"
        elif "2020_2021" in col:
            year = "2020_2021"
        elif "2021_2022" in col:
            year = "2021_2022"
        elif "2022_2023" in col:
            year = "2022_2023"
        elif "2023_2024" in col:
            year = "2023_2024"

        # Extract key label (like forests, net_area_sown, fallow_land, etc.)
        label = col.split("__")[-1].replace("_", " ").strip()
        label = label.replace(" ", "_")

        # Combine year + label for clarity
        new_name = f"{year}_{label}" if year else label
        new_cols.append(new_name)
    df.columns = new_cols
    return df

# --- Step 4: Clean column names ---
crop_df = clean_columns(crop_df)
land_df = clean_columns(land_df)

# --- Step 5: Drop district-level columns and keep state-level only ---
# Assuming 'district' column represents district-level data and 'state' represents state-wise
# We'll drop district columns if found
for df in [crop_df, land_df]:
    drop_cols = [c for c in df.columns if "district" in c.lower()]
    df.drop(columns=drop_cols, inplace=True, errors="ignore")

# --- Step 6: Optional: Keep only state, year, and key measures ---
# (e.g., forests, net_area_sown, etc.)
key_columns = ["state", "year"] + [c for c in land_df.columns if any(k in c for k in [
    "forests", "net_area_sown", "cropped_area", "fallow_land", "reporting_area"])]
land_df = land_df[key_columns] if all(k in land_df.columns for k in ["state", "year"]) else land_df
crop_df = crop_df[key_columns] if all(k in crop_df.columns for k in ["state", "year"]) else crop_df

# --- Step 7: Print a quick preview ---
print("Cleaned Crop Data:")
display(crop_df.head(3))

print("\nCleaned Land Data:")
display(land_df.head(3))

# --- Step 8: Save cleaned outputs ---
crop_df.to_csv("/content/cleaned_crop_state_year.csv", index=False)
land_df.to_csv("/content/cleaned_land_state_year.csv", index=False)

print("\n✅ Cleaned files saved as:")
print("/content/cleaned_crop_state_year.csv")
print("/content/cleaned_land_state_year.csv")
