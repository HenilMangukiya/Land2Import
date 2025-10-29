import pandas as pd
from sqlalchemy import create_engine

# Load CSV
weather_df = pd.read_csv(r"D:\Land2Import\merged_land_crop_summary.csv")

# Create PostgreSQL engine
engine = create_engine("postgresql+psycopg2://postgres:2202@localhost:5432/intern078")

# Write to PostgreSQL
weather_df.to_sql("merged_land_crop_summary", engine, if_exists="replace", index=False)

print("✅ crop data loaded successfully")
