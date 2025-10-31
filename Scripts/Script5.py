import pandas as pd
import csv

land_path = "/content/renamed_land_data.csv"
crop_path = "/content/renamed_crop_data.csv"
output_path = "/content/final_state_year_land_crop_data.csv"

# --- Step 1: Read headers only to find year columns ---
land_header = pd.read_csv(land_path, nrows=1)
crop_header = pd.read_csv(crop_path, nrows=1)

state_col_land = land_header.columns[0]
state_col_crop = crop_header.columns[0]

land_years = [c for c in land_header.columns if any(ch.isdigit() for ch in c)]
crop_years = [c for c in crop_header.columns if any(ch.isdigit() for ch in c)]
common_years = sorted(set(land_years) & set(crop_years))

print("✅ Common years detected:", common_years[:10], "..." if len(common_years) > 10 else "")

# --- Step 2: Write header for output CSV ---
with open(output_path, "w", newline="") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["State", "Year", "Total_Land", "Total_Crop_Production"])

# --- Step 3: Build quick lookup for crop data (state → year → production) ---
crop_lookup = {}

for chunk in pd.read_csv(crop_path, chunksize=50):
    for _, row in chunk.iterrows():
        state = str(row[state_col_crop]).strip()
        crop_lookup[state] = {}
        for year in common_years:
            try:
                crop_lookup[state][year] = float(row[year])
            except Exception:
                crop_lookup[state][year] = 0.0

print("✅ Crop lookup created for", len(crop_lookup), "states")

# --- Step 4: Stream through land data, match crop data, write line-by-line ---
with open(output_path, "a", newline="") as f_out:
    writer = csv.writer(f_out)

    for chunk in pd.read_csv(land_path, chunksize=50):
        for _, row in chunk.iterrows():
            state = str(row[state_col_land]).strip()

            for year in common_years:
                try:
                    land_val = float(row[year])
                except Exception:
                    land_val = 0.0

                crop_val = crop_lookup.get(state, {}).get(year, 0.0)
                writer.writerow([state, year, land_val, crop_val])

print("\n🎯 Done! File saved as:", output_path)
