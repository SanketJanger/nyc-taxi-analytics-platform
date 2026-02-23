from fastapi import FastAPI, HTTPException, Query
from cachetools import TTLCache
import awswrangler as wr
import re
from typing import Any

app = FastAPI(title="NYC Taxi Analytics API")

DATABASE = "nyc_taxi"
S3_OUTPUT = "s3://sanket-nyc-taxi-raw/athena-results/"

# Cache results for 10 minutes to reduce Athena scans
CACHE = TTLCache(maxsize=128, ttl=600)

YEAR_RE = re.compile(r"^\d{4}$")
MONTH_RE = re.compile(r"^(0[1-9]|1[0-2])$")


def validate_year_month(year: str, month: str) -> None:
    if not YEAR_RE.match(year):
        raise HTTPException(status_code=400, detail="year must be 4 digits (e.g., 2025)")
    if not MONTH_RE.match(month):
        raise HTTPException(status_code=400, detail="month must be 01..12")


def run_athena(sql: str) -> Any:
    if sql in CACHE:
        return CACHE[sql]
    df = wr.athena.read_sql_query(sql=sql, database=DATABASE, s3_output=S3_OUTPUT)
    out = df.to_dict(orient="records")
    CACHE[sql] = out
    return out


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/zones/{location_id}")
def zone_details(location_id: int):
    sql = f"""
    SELECT LocationID AS locationid, Borough AS borough, Zone AS zone, service_zone
    FROM nyc_taxi.taxi_zone_lookup
    WHERE LocationID = {int(location_id)}
    LIMIT 1;
    """
    rows = run_athena(sql)
    if not rows:
        raise HTTPException(status_code=404, detail="Zone not found")
    return rows[0]


@app.get("/trips/top-pickups")
def top_pickups(
    year: str = Query("2025"),
    month: str = Query("01"),
    limit: int = Query(10, ge=1, le=50),
):
    validate_year_month(year, month)
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
    LIMIT {int(limit)};
    """
    return run_athena(sql)


@app.get("/trips/avg-by-hour")
def avg_by_hour(
    year: str = Query("2025"),
    month: str = Query("01"),
):
    validate_year_month(year, month)
    sql = f"""
    SELECT hour(tpep_pickup_datetime) AS hour,
           AVG(total_amount) AS avg_total,
           COUNT(*) AS trips
    FROM nyc_taxi.yellow_trips
    WHERE year='{year}' AND month='{month}'
    GROUP BY 1
    ORDER BY 1;
    """
    return run_athena(sql)


@app.get("/trips/revenue-by-borough")
def revenue_by_borough(
    year: str = Query("2025"),
    month: str = Query("01"),
):
    validate_year_month(year, month)
    sql = f"""
    SELECT z.Borough AS borough,
           SUM(t.total_amount) AS total_revenue,
           COUNT(*) AS trips,
           AVG(t.total_amount) AS avg_total
    FROM nyc_taxi.yellow_trips t
    JOIN nyc_taxi.taxi_zone_lookup z
      ON t.PULocationID = z.LocationID
    WHERE t.year='{year}' AND t.month='{month}'
    GROUP BY 1
    ORDER BY total_revenue DESC;
    """
    return run_athena(sql)