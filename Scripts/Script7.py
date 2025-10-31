import pandas as pd
import csv
import numpy as np

# File paths
land_path = "/content/renamed_land_data.csv"
crop_path = "/content/renamed_crop_data.csv"
output_path = "/content/final_state_year_land_crop_data.csv"

# --- Step 1: Detect columns ---
land_header = pd.read_csv(land_path, nrows=1)
crop_header = pd.read_csv(crop_path, nrows=1)

state_col_land = land_header.columns[0]
state_col_crop = crop_header.columns[0]

land_years = [c for c in land_header.columns if any(ch.isdigit() for ch in c)]
crop_years = [c for c in crop_header.columns if any(ch.isdigit() for ch in c)]
common_years = sorted(set(land_years) & set(crop_years))

print("✅ Common years found:", common_years[:10], "..." if len(common_years) > 10 else "")

# --- Step 2: Create crop lookup (state → year → value) ---
crop_lookup = {}
for chunk in pd.read_csv(crop_path, chunksize=50):
    for _, row in chunk.iterrows():
        state = str(row[state_col_crop]).strip()
        crop_lookup[state] = {}
        for year in common_years:
            val = pd.to_numeric(row.get(year, np.nan), errors="coerce")
            crop_lookup[state][year] = val if not pd.isna(val) else np.nan

# --- Step 3: Prepare merged CSV ---
with open(output_path, "w", newline="") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["State", "Year", "Total_Land", "Total_Crop_Production"])

    for chunk in pd.read_csv(land_path, chunksize=50):
        for _, row in chunk.iterrows():
            state = str(row[state_col_land]).strip()

            for year in common_years:
                land_val = pd.to_numeric(row.get(year, np.nan), errors="coerce")
                crop_val = crop_lookup.get(state, {}).get(year, np.nan)

                # Skip invalid data
                if pd.isna(land_val) or pd.isna(crop_val) or (land_val == 0) or (crop_val == 0):
                    continue

                writer.writerow([state, year, land_val, crop_val])

print("\n✅ Merged file saved:", output_path)

# --- Step 4: Clean states (remove numbers or invalid entries) ---
df = pd.read_csv(output_path)

# Replace any number codes with empty and drop them
df["State"] = df["State"].astype(str).str.strip()

# Remove rows where 'State' contains any digit
df = df[~df["State"].str.contains(r"\d", na=False)]

# Optional: drop duplicates just in case
df = df.drop_duplicates()

# Save cleaned version
final_output = "/content/final_clean_land_crop_data.csv"
df.to_csv(final_output, index=False)

print("\n🎯 Final cleaned file ready:", final_output)
print("✅ Preview:")
display(df.head())
