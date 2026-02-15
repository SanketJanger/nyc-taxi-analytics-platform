from fastapi import FastAPI, Query
import awswrangler as wr
from fastapi import HTTPException

app = FastAPI(title="NYC Taxi Analytics API")

DATABASE = "nyc_taxi"
S3_OUTPUT = "s3://sanket-nyc-taxi-raw/athena-results/"

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/trips/top-pickups")
def top_pickups(
    year: str = Query("2025"),
    month: str = Query("01"),
    limit: int = Query(10, ge=1, le=50),
):
    sql = f"""
    SELECT z.Borough AS borough,
           z.Zone AS zone,
           COUNT(*) AS trips,
           AVG(t.total_amount) AS avg_total
    FROM nyc_taxi.yellow_trips t
    JOIN nyc_taxi.taxi_zone_lookup z
      ON t.PULocationID = z.LocationID
    WHERE t.year='{year}' AND t.month='{month}'
    GROUP BY 1,2
    ORDER BY trips DESC
    LIMIT {limit};
    """
    df = wr.athena.read_sql_query(sql=sql, database=DATABASE, s3_output=S3_OUTPUT)
    return df.to_dict(orient="records")

@app.get("/trips/avg-by-hour")
def avg_by_hour(
    year: str = Query("2025"),
    month: str = Query("01"),
):
    sql = f"""
    SELECT hour(tpep_pickup_datetime) AS hour,
           AVG(total_amount) AS avg_total,
           COUNT(*) AS trips
    FROM nyc_taxi.yellow_trips
    WHERE year='{year}' AND month='{month}'
    GROUP BY 1
    ORDER BY 1;
    """
    df = wr.athena.read_sql_query(sql=sql, database=DATABASE, s3_output=S3_OUTPUT)
    return df.to_dict(orient="records")

@app.get("/zones/{location_id}")
def zone_details(location_id: int):
    sql = f"""
    SELECT LocationID, Borough, Zone, service_zone
    FROM nyc_taxi.taxi_zone_lookup
    WHERE LocationID = {location_id}
    LIMIT 1;
    """
    df = wr.athena.read_sql_query(sql=sql, database=DATABASE, s3_output=S3_OUTPUT)
    if df.empty:
        raise HTTPException(status_code=404, detail="Zone not found")
    return df.iloc[0].to_dict()
