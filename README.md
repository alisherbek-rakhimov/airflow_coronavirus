`docker compose up airflow-init`
<br>
`docker compose up -d
`

- **check the health of containers firs**
- **all credentials are hardcoded**
- **no dbt was used**: having issues in DockerOperator on Windows(cannot share network/bridge with created container),
  BashOperator with dbt installed in venv is not an
  ideal practice
- just **pandas** for cleaning in first task and **directly execute** on **DataWarehouse compute** instead of uploading
  to airflow
  instance
  memory for second task

<hr>

# DW credentials from docker-compose file:

- POSTGRES_PASSWORD: postgres
- POSTGRES_USER: postgres
- POSTGRES_DB: postgres
- PGPORT: 15432

# Airflow credentiasl defaults to:

- user: airflow
- passowrd: airflow

# please remove limit 10 from 2nd task to check, coz when its sorted from low to high, there is only 0 % recovered
