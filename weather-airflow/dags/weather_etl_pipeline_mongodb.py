from datetime import datetime, timezone, timedelta
import json
import certifi
import pandas as pd
from pymongo import MongoClient
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning,)

LOCATION = Variable.get("LOCATION")

# -------------------------------------------------------
# Helper Function
# -------------------------------------------------------


def kelvin_to_fahrenheit(kelvin):
    """Convert temperature from Kelvin to Fahrenheit."""
    return round((kelvin - 273.15) * 9 / 5 + 32, 2)


# -------------------------------------------------------
# Transform and Load Function
# -------------------------------------------------------

def transform_load_data(ti):

    weather = ti.xcom_pull(task_ids="extract_weather_data")

    local_tz = timezone(timedelta(seconds=weather["timezone"]))

    transformed_data = {
        "city": weather["name"],
        "country": weather["sys"]["country"],
        "weather": weather["weather"][0]["main"],
        "description": weather["weather"][0]["description"],
        "temperature_f": kelvin_to_fahrenheit(weather["main"]["temp"]),
        "feels_like_f": kelvin_to_fahrenheit(weather["main"]["feels_like"]),
        "min_temp_f": kelvin_to_fahrenheit(weather["main"]["temp_min"]),
        "max_temp_f": kelvin_to_fahrenheit(weather["main"]["temp_max"]),
        "pressure": weather["main"]["pressure"],
        "humidity": weather["main"]["humidity"],
        "wind_speed": weather["wind"]["speed"],
        "cloudiness": weather["clouds"]["all"],
        "visibility": weather.get("visibility"),
        "recorded_at": datetime.fromtimestamp(
            weather["dt"], tz=local_tz
        ),
        "sunrise": datetime.fromtimestamp(
            weather["sys"]["sunrise"], tz=local_tz
        ),
        "sunset": datetime.fromtimestamp(
            weather["sys"]["sunset"], tz=local_tz
        ),
        "created_at": datetime.now(timezone.utc),
    }

    # ----------------------------
    # Upload CSV to S3
    # ----------------------------

    df = pd.DataFrame([transformed_data])

    aws_credentials = {
        "key": Variable.get("AWS_ACCESS_KEY_ID"),
        "secret": Variable.get("AWS_SECRET_ACCESS_KEY"),
    }

    bucket = Variable.get("S3_BUCKET_NAME")

    filename = (
        f"weather/current_weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    df.to_csv(
        f"s3://{bucket}/{filename}",
        index=False,
        storage_options=aws_credentials,
    )

    print(f"Uploaded {filename} to S3.")

    # ----------------------------
    # Insert into MongoDB
    # ----------------------------

    client = MongoClient(
        Variable.get("MONGODB_URI"),
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=30000,
    )

    db = client[Variable.get("MONGODB_DATABASE")]

    collection = db[Variable.get("MONGODB_COLLECTION")]

    result = collection.insert_one(weather)

    print(f"Inserted MongoDB document: {result.inserted_id}")

    client.close()


# -------------------------------------------------------
# Default DAG Arguments
# -------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# -------------------------------------------------------
# DAG Definition
# -------------------------------------------------------

with DAG(
    dag_id="openweathermap_etl_pipeline_mongodb",
    description="Daily ETL pipeline for OpenWeatherMap data to AWS S3 and MongoDB",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["weather", "etl", "aws", "s3", "mongodb"],
) as dag:

    # Check API availability
    check_weather_api = HttpSensor(
        task_id="check_weather_api",
        http_conn_id="openweathermap_api",
        endpoint=(
            f"/data/2.5/weather?"
            f"q={LOCATION}&appid={Variable.get('OPENWEATHER_API_KEY')}"
        ),
        poke_interval=30,
        timeout=300,
    )

    # Extract weather data
    extract_weather_data = HttpOperator(
        task_id="extract_weather_data",
        http_conn_id="openweathermap_api",
        endpoint=(
            f"/data/2.5/weather?"
            f"q={LOCATION}&appid={Variable.get('OPENWEATHER_API_KEY')}"
        ),
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    # Transform and Load
    transform_load_weather_data = PythonOperator(
        task_id="transform_load_weather_data",
        python_callable=transform_load_data,
    )

    (
        check_weather_api
        >> extract_weather_data
        >> transform_load_weather_data
    )
