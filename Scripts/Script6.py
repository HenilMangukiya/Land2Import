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

# --- Step 3: Prepare output CSV ---
with open(output_path, "w", newline="") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["State", "Year", "Total_Land", "Total_Crop_Production"])

    for chunk in pd.read_csv(land_path, chunksize=50):
        for _, row in chunk.iterrows():
            state = str(row[state_col_land]).strip()

            for year in common_years:
                land_val = pd.to_numeric(row.get(year, np.nan), errors="coerce")
                crop_val = crop_lookup.get(state, {}).get(year, np.nan)

                # Skip rows where either is missing or zero
                if pd.isna(land_val) or pd.isna(crop_val) or (land_val == 0) or (crop_val == 0):
                    continue

                writer.writerow([state, year, land_val, crop_val])

print("\n✅ Clean merged file saved at:", output_path)

# --- Step 4: Map proper state names if they are numeric codes ---
df = pd.read_csv(output_path)

# Example mapping — adjust this to match your dataset
state_map = {
    "1": "Andhra Pradesh",
    "2": "Arunachal Pradesh",
    "3": "Assam",
    "4": "Bihar",
    "5": "Chhattisgarh",
    "6": "Goa",
    "7": "Gujarat",
    "8": "Haryana",
    "9": "Himachal Pradesh",
    "10": "Jharkhand",
    "11": "Karnataka",
    "12": "Kerala",
    "13": "Madhya Pradesh",
    "14": "Maharashtra",
    "15": "Manipur",
    "16": "Meghalaya",
    "17": "Mizoram",
    "18": "Nagaland",
    "19": "Odisha",
    "20": "Punjab",
    "21": "Rajasthan",
    "22": "Sikkim",
    "23": "Tamil Nadu",
    "24": "Telangana",
    "25": "Tripura",
    "26": "Uttar Pradesh",
    "27": "Uttarakhand",
    "28": "West Bengal",
}

# Replace state numbers with names
df["State"] = df["State"].astype(str).replace(state_map)

# Remove invalid state names like "2015-16", etc.
df = df[~df["State"].str.contains(r"\d{4}", na=False)]

# Save cleaned version
df.to_csv("/content/final_clean_land_crop_data.csv", index=False)

print("\n🎯 Final cleaned file ready: /content/final_clean_land_crop_data.csv")
print("✅ Preview:")
display(df.head())
