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
def extract(past_days: int, url):
    latitude = float(Variable.get("LATITUDE"))
    longitude = float(Variable.get("LONGITUDE"))

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": int(past_days),
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "weather_code",
        ],
        "timezone": "America/Los_Angeles",
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    return {
        "latitude": latitude,
        "longitude": longitude,
        "daily": data.get("daily", {}),
    }


@task
def transform(data):
    records = []

    latitude = float(data.get("latitude"))
    longitude = float(data.get("longitude"))
    daily = data.get("daily", {})

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

            print(latitude, longitude, dt_str, "-", temp_max, temp_min, temp_mean, precipitation, weather_code)

            sql = f"""
            INSERT INTO {target_table}
              (latitude, longitude, date, temp_max, temp_min, temp_mean, precipitation, weather_code)
            VALUES
              ({float(latitude)}, {float(longitude)}, '{dt_str}', 
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
    start_date = datetime(2026,3,4),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "raw.HW5_weather"
    url = Variable.get("open_meteo_url")
 
    data = extract(60, url)
    lines = transform(data)
    load(lines, target_table)