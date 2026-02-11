import pandas as pd

PARQUET_PATH = "data/raw/yellow_tripdata_2025-01.parquet"
ZONES_PATH = "data/lookup/taxi_zone_lookup.csv"  
df = pd.read_parquet(PARQUET_PATH)
zones = pd.read_csv(ZONES_PATH)

print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print("\nHead:\n", df.head(3))

top = df["PULocationID"].value_counts().head(10).reset_index()
top.columns = ["LocationID", "TripCount"]
top = top.merge(zones, on="LocationID", how="left")
print("\nTop 10 pickup zones:\n", top[["LocationID", "Borough", "Zone", "TripCount"]])

for c in ["tpep_pickup_datetime", "pickup_datetime"]:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")
        print(f"\nPickup datetime range ({c}):", df[c].min(), "→", df[c].max())
        break
