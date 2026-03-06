from datetime import datetime, timedelta
import os
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_logical_date():
    # Get the current context
    context = get_current_context()
    return str(context['logical_date'])[:10]


def get_next_day(date_str):
    """
    Given a date string in 'YYYY-MM-DD' format, returns the next day as a string in the same format.
    """
    # Convert the string date to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Add one day using timedelta
    next_day = date_obj + timedelta(days=1)

    # Convert back to string in "YYYY-MM-DD" format
    return next_day.strftime("%Y-%m-%d")


def return_snowflake_conn(con_id):

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


def get_past_weather(start_date, end_date, latitude, longitude):
    # Gets past weather data for the specified date range and location

    url = "https://api.open-meteo.com/v1/forecast"

    # we use start and end date rather than past_days and forecast_days
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)
    data = response.json()

    # Convert data into DataFrame
    df = pd.DataFrame({
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    df["date"] = pd.to_datetime(df["date"])

    return df


def save_weather_data(city, latitude, longitude, start_date, end_date, file_path):
    data = get_past_weather(start_date, end_date, latitude, longitude)
    data['city'] = city
    data.to_csv(file_path, index=False)
    return


def populate_table_via_stage(cur, database, schema, table, file_path):
    """
    Populate a table with data from a given CSV file using Snowflake's COPY INTO command.
    """
    # Create a temporary named stage instead of using the table stage
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path)  # extract only filename from the path

    # First set the schema since table stage or temp stage needs to have the schema as the target table
    cur.execute(f"USE SCHEMA {database}.{schema}")

    # Create a temporary named stage
    cur.execute(f"CREATE TEMPORARY STAGE {stage_name}")

    # Copy the given file to the temporary stage
    cur.execute(f"PUT file://{file_path} @{stage_name}")

    # Run copy into command with fully qualified table name
    copy_query = f"""
        COPY INTO {schema}.{table}
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
        )
    """
    cur.execute(copy_query)


@task
def extract(city, longitude, latitude):
    date_to_fetch = get_logical_date()                         # TO DO: use get_logical_date function
    next_day_of_date_to_fetch = get_next_day(date_to_fetch)    # TO DO: use get_next_day function to get the next day of date_to_fetch
    
    print(f"========= Reading {date_to_fetch}'s weather data ===========")
    file_path = f"/tmp/{city}_{date_to_fetch}.csv"
    
    save_weather_data(city, latitude, longitude, date_to_fetch, next_day_of_date_to_fetch, file_path)
    
    return file_path


@task
def load(file_path, database, schema, target_table):
    date_to_fetch = get_logical_date()
    next_day_of_date_to_fetch = get_next_day(date_to_fetch)

    print(f"========= Updating {date_to_fetch}'s data ===========")
    cur = return_snowflake_conn("snowflake_conn")

    try:
        cur.execute("BEGIN;")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {database}.{schema}.{target_table} (
            date date, temp_max float, temp_min float, precipitation float, weather_code varchar, city varchar
        )""")
        
        
        # TO DO:
        # DELETE existing records for the date being fetched to avoid duplicates
        # using date_to_fetch and next_day_of_date_to_fetch to filter the records to be deleted
        
        cur.execute(f""" DELETE FROM {database}.{schema}.{target_table} 
                    WHERE date >= '{date_to_fetch}' AND date <= '{next_day_of_date_to_fetch}' """)

        # TO DO:
        # Call populate_table_via_stage function to load data from the file into the target table in Snowflake
        populate_table_via_stage(cur, database, schema, target_table, file_path)    # pass the required parameters to the function
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'weather_ETL_incremental',       # TO DO: add incremental as a suffix to the DAG id
    start_date = datetime(2026,2,28),          # TO DO: Set the start date to Feb 28, 2026
    catchup=False ,                            # TO DO: Set catchup to False
    tags=['ETL'],
    schedule = '30 3 * * *',
    max_active_runs=1
) as dag:
    LATITUDE = Variable.get("LATITUDE")             # TO DO: Use Variable.get to get latitude value from Airflow Variables
    LONGITUDE = Variable.get("LONGITUDE")            # TO DO: Use Variable.get to get longitude value from Airflow Variables
    CITY = "LA"                                      # TO DO: Set the city name to your coordinates' city name
    
    database = "USER_DB_GRIZZLY"                     # TO DO: Set the database name  
    schema = "raw"
    target_table = "weather_incremental"             # TO DO: Set the target table name

    file_path = extract(CITY, LONGITUDE, LATITUDE)
    load(file_path, database, schema, target_table)