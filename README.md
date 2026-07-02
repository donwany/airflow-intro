###  Install Airflow
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml'

mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
echo 'FERNET_KEY=tdRkPal3VWuqVY_Mt92jfEAazYgM4kNsANNzTBAA-Q0=' >> .env

# initialize database
docker compose up airflow-init

# running airflow
docker compose up -d

# running CLI commands
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.2.2/airflow.sh'

chmod +x airflow.sh

./airflow.sh info
./airflow.sh bash

# access the web UI
http://localhost:8080 

The default account has the login `airflow` and the password `airflow`.

# clean-up
docker compose down --volumes --rmi all

# reference
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

```

### Scripts
- Put all your scripts inside the `dags` folder


### DAG CATCHUP Variable
- Imagine Todays date is 2026/01/10

```
# start date = 2026/01/01
# when CATCHUP = TRUE
Jan 1  ✅
Jan 2  ✅
Jan 3  ✅
Jan 4  ✅
Jan 5  ✅
Jan 6  ✅
Jan 7  ✅
Jan 8  ✅
Jan 9  ✅
Jan 10 ✅
```

```
# start date = 2026/01/01
# when CATCHUP = FALSE

Jan 1  ❌ Skipped
Jan 2  ❌ Skipped
Jan 3  ❌ Skipped
Jan 4  ❌ Skipped
Jan 5  ❌ Skipped
Jan 6  ❌ Skipped
Jan 7  ❌ Skipped
Jan 8  ❌ Skipped
Jan 9  ❌ Skipped
Jan 10 ✅ First run
Jan 11 ✅
Jan 12 ✅

```