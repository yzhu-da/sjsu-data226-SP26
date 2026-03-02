from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
     - Create a view with training related columns
     - Create a model with the view above
    """
    # Create a view selecting specific columns required for time-series forecasting.
    # Specifically uses DATE, TEMP_MAX, and CITY from the source weather table.
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        date, temp_max, location_name
        FROM {train_input_table};"""

    # Define and train the ML model targeting Maximum Temperature.
    # CITY is used as the series column to allow forecasts for multiple locations.
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'location_name',
        TIMESTAMP_COLNAME => 'date',
        TARGET_COLNAME => 'temp_max',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model. 
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise


@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
     - Generate predictions and store the results to a table named forecast_table.
     - Union your predictions with your historical data, then create the final table
    """
    make_prediction_sql = f"""BEGIN
        -- This is the step that creates your predictions.
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT location_name, date, temp_max as actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as location_name, ts as date, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table}
        ORDER BY location_name, date DESC;"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise


with DAG(
    dag_id = 'TrainPredict',
    start_date = datetime(2026,2,28),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '30 2 * * *'
) as dag:

    train_input_table = "raw.city_weather"
    train_view = "raw.city_weather_view"
    forecast_table = "raw.city_weather_forecast"
    forecast_function_name = "raw.predict_city_weather"
    final_table = "raw.city_weather_final"
    cur = return_snowflake_conn()

    train_task = train(cur, train_input_table, train_view, forecast_function_name)
    predict_task = predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)
    train_task >> predict_task
