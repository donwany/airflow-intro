
### ETL Pipeline
- Extract weather data from the OpenWeatherMap API
- Transform the JSON into a structured Pandas DataFrame
- Load the CSV into an AWS S3 bucket
- Schedule the pipeline to run daily
- Uses Airflow Variables instead of hardcoding API keys and AWS credentials (recommended)

### Airflow Env Variables
```
| Variable                | Example               |
| ----------------------- | --------------------- |
| `OPENWEATHER_API_KEY`   | `xxxxxxxxxxxxxxxx`    |
| `AWS_ACCESS_KEY_ID`     | `AKIA...`             |
| `AWS_SECRET_ACCESS_KEY` | `xxxxxxxx`            |
| `S3_BUCKET_NAME`        | `weather-data-bucket` |
```

### Add Connection
```
| Field               | Value                            |
| ------------------- | -------------------------------- |
| **Connection Id**   | `openweathermap_api`             |
| **Connection Type** | `HTTP`                           |
| **Host**            | `https://api.openweathermap.org` |
| **Login**           | *(leave blank)*                  |
| **Password**        | *(leave blank)*                  |
| **Port**            | *(leave blank)*                  |
```

### Airflow DAG
```
                  +----------------------+
                  |   HttpSensor         |
                  | Check API Available  |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | HttpOperator         |
                  | Extract Weather JSON |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | PythonOperator       |
                  | Transform Data       |
                  | Convert to DataFrame |
                  | Upload CSV to S3     |
                  +----------+-----------+
                             |
                             v
                     AWS S3 Bucket


```

### Using Airflow Hook
- MONGODB_URI = "mongodb+srv://worldboss:H2WZ9zL3YWRlkI5H@ballotflow.0uvaa.mongodb.net/?appName=ballotflow"
- MONGODB_DATABASE=openweather
- MONGODB_COLLECTION=weather
- 
```python
from airflow.providers.mongo.hooks.mongo import MongoHook

hook = MongoHook(conn_id="mongodb_default")

collection = (
    hook.get_conn()
        [Variable.get("MONGODB_DATABASE")]
        [Variable.get("MONGODB_COLLECTION")]
)

collection.insert_one(transformed_data)
```


### Check if Packages Installed
```bash
docker exec -it airflow-worker bash

python -c "import pandas, numpy, boto3, s3fs; print('All packages installed successfully!')"
```

### References
- https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html