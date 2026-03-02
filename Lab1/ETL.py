from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests

from datetime import timedelta
from datetime import datetime


def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(locations, past_days: int, url):

 
    payloads = []

    
    for loc in locations:
        # loc is a tuple
        latitude = loc[0]
        longitude = loc[1]
        location_name = loc[2]


        params = {
            "latitude": latitude,
            "longitude": longitude,
            "past_days": int(past_days),
            "forecast_days": 0,  # only historical
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
                "weather_code",
            ],
            "timezone": "America/Los_Angeles",
        }

        r = requests.get(url, params=params)
        data = r.json()

        payloads.append(
            {
                "location_name": location_name,
                "latitude": float(latitude),
                "longitude": float(longitude),
                "daily": data.get("daily", {}),
            }
        )

    return payloads


@task
def transform(payloads):
    records = []

    for p in payloads:
        location_name = p["location_name"]
        latitude = p["latitude"]
        longitude = p["longitude"]
        daily = p.get("daily", {})

        times = daily.get("time", [])
        tmax = daily.get("temperature_2m_max", [])
        tmin = daily.get("temperature_2m_min", [])
        tmean = daily.get("temperature_2m_mean", [])
        prcp = daily.get("precipitation_sum", [])
        wcode = daily.get("weather_code", [])

        n = len(times)
        n = min(n, len(tmax), len(tmin), len(tmean), len(prcp), len(wcode))

        for i in range(n):
            records.append(
                {
                    "location_name": location_name,
                    "latitude": latitude,
                    "longitude": longitude,
                    "date": times[i],  # ISO 'YYYY-MM-DD'
                    "temp_max": tmax[i],
                    "temp_min": tmin[i],
                    "temp_mean": tmean[i],
                    "precipitation": prcp[i],
                    "weather_code": wcode[i],
                }
            )

    return records

@task
def load(records, target_table: str):
    cur = return_snowflake_conn()

    def sql_num(x):
        if x is None:
            return "NULL"
        try:
            # handle NaN without pandas
            if isinstance(x, float) and (x != x):
                return "NULL"
        except Exception:
            pass
        return str(float(x))

    def sql_int(x):
        if x is None:
            return "NULL"
        try:
            if isinstance(x, float) and (x != x):
                return "NULL"
        except Exception:
            pass
        return str(int(x))

    def sql_str(x):
        if x is None:
            return "NULL"
        return "'" + str(x).replace("'", "''") + "'"

    try:
        cur.execute("BEGIN;")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
              location_name VARCHAR NOT NULL,
              latitude NUMBER(9,6) NOT NULL,
              longitude NUMBER(9,6) NOT NULL,
              date DATE NOT NULL,
              temp_max FLOAT,
              temp_min FLOAT,
              temp_mean FLOAT,
              precipitation FLOAT,
              weather_code INTEGER,
              CONSTRAINT pk_open_meteo PRIMARY KEY (latitude, longitude, date)
            );
            """
        )

        # Mimic the sample style: re-load full table each run
        cur.execute(f"DELETE FROM {target_table};")

        for r in records:
            location_name = r["location_name"]
            latitude = r["latitude"]
            longitude = r["longitude"]
            dt = r["date"]
            temp_max = r["temp_max"]
            temp_min = r["temp_min"]
            temp_mean = r["temp_mean"]
            precipitation = r["precipitation"]
            weather_code = r["weather_code"]

            # dt is already 'YYYY-MM-DD' from Open-Meteo daily.time
            dt_str = str(dt)

            print(location_name, latitude, longitude, dt_str, "-", temp_max, temp_min, temp_mean, precipitation, weather_code)

            sql = f"""
            INSERT INTO {target_table}
              (location_name, latitude, longitude, date, temp_max, temp_min, temp_mean, precipitation, weather_code)
            VALUES
              ({sql_str(location_name)}, {float(latitude)}, {float(longitude)}, '{dt_str}', 
              {sql_num(temp_max)}, {sql_num(temp_min)}, {sql_num(temp_mean)}, {sql_num(precipitation)}, {sql_int(weather_code)})
            """
            cur.execute(sql)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'ETL',
    start_date = datetime(2026,2,28),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "raw.city_weather"
    url = Variable.get("open_meteo_url")

    # two location
    LOCATION_1=Variable.get("LOCATION_1")
    LOCATION_2=Variable.get("LOCATION_2")
    LATITUDE_1 = float(Variable.get("LATITUDE_1"))
    LONGITUDE_1 = float(Variable.get("LONGITUDE_1"))
    LATITUDE_2 = float(Variable.get("LATITUDE_2"))
    LONGITUDE_2 = float(Variable.get("LONGITUDE_2"))
    locations=[(LATITUDE_1, LONGITUDE_1, LOCATION_1), (LATITUDE_2, LONGITUDE_2, LOCATION_2)]

    data = extract(locations, 60, url)
    lines = transform(data)
    load(lines, target_table)