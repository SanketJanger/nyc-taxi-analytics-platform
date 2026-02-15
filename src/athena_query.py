import awswrangler as wr

DATABASE = "nyc_taxi"
S3_OUTPUT = "s3://sanket-nyc-taxi-raw/athena-results/"

QUERY = """
SELECT z.Borough, z.Zone,
       COUNT(*) AS trips,
       AVG(t.total_amount) AS avg_total
FROM nyc_taxi.yellow_trips t
JOIN nyc_taxi.taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
WHERE t.year='2025' AND t.month='01'
GROUP BY 1,2
ORDER BY trips DESC
LIMIT 10;
"""

df = wr.athena.read_sql_query(
    sql=QUERY,
    database=DATABASE,
    s3_output=S3_OUTPUT,
)
print(df)
