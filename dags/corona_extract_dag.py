from datetime import timedelta, datetime

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow import DAG

default_args = {
    'owner': 'Alisher',
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}


def parse_covid():
    from bs4 import BeautifulSoup
    import pandas as pd
    import requests
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus
    # from etl.settings import dsn
    from sqlalchemy import text

    url = "https://www.worldometers.info/coronavirus/#countries"
    website_text = requests.get(url).text
    soup = BeautifulSoup(website_text, 'html.parser')
    table = soup.find('tbody')

    table_data = []
    for row in table.find_all('tr', {"class": False}):
        # Regions rows have class attr, so set class=False to get Country rows
        row_data = []
        for cell in row.findAll('td'):
            row_data.append(cell.text.replace(',', '').replace(':', '').replace('+', '').replace('N/A', ''))
        if len(row_data) > 0:
            data_item = {
                "Country,Other": row_data[1],
                "TotalCases": row_data[2],
                "NewCases": row_data[3],
                "TotalDeaths": row_data[4],
                "NewDeaths": row_data[5],
                "TotalRecovered": row_data[6],
                "NewRecovered": row_data[7],
                "ActiveCases": row_data[8]
            }
            table_data.append(data_item)

    df = pd.DataFrame(table_data)

    cols = ['TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths', 'TotalRecovered', 'NewRecovered', 'ActiveCases']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce').astype('Int64')

    # uri = f"postgresql+psycopg2://{quote_plus(dsn['user'])}:{quote_plus(dsn['password'])}@{dsn['host']}:{dsn['port']}/{dsn['dbname']}"
    uri = f"postgresql+psycopg2://{quote_plus('postgres')}:{quote_plus('postgres')}@{'data_warehouse'}:{15432}/{'postgres'}"
    engine = create_engine(uri, echo=False)

    with engine.connect() as conn:
        conn.execute(text("""
                drop table if exists coronavirus1;
                drop table if exists coronavirus2;
                drop table if exists coronavirus3;
            """))
        conn.commit()
        print(df)
        df.to_sql('coronavirus1', conn, if_exists='replace', index=False)
        df.to_sql('coronavirus2', conn, if_exists='replace', index=False)
        df.to_sql('coronavirus3', conn, if_exists='replace', index=False)

def transform_covid():
    from urllib.parse import quote_plus
    from sqlalchemy import create_engine
    from sqlalchemy import text

    engine = create_engine(
        f"postgresql+psycopg2://{quote_plus('postgres')}:{quote_plus('postgres')}@{'data_warehouse'}:{15432}/{'postgres'}",
        echo=False)

    with engine.connect() as conn:
        conn.execute(text("""
            drop table if exists transform_coronavirus1;
            create table transform_coronavirus1 as (select *, concat(round(precise_ratio, 2), ' %') as ratio_percent
                                            from (select *,
                                                         ("TotalRecovered" * 100) / ("TotalCases" * 1.0) as precise_ratio
                                                  from coronavirus1) tmp 
                                                  order by precise_ratio desc 
                                            limit 10)
        """))
        conn.commit()


with DAG(
        dag_id='covid_dag_v26',
        default_args=default_args,
        description='dag for parsing covid table and storing in DW',
        start_date=datetime(2023, 6, 18),
        schedule_interval='@daily',
        catchup=False
) as dag:
    parse_covid_task = PythonVirtualenvOperator(
        task_id="parse_covid_task",
        requirements=["beautifulsoup4==4.12.2", "pandas==2.0.2", "requests==2.31.0", "psycopg2-binary==2.9.6",
                      "sqlalchemy==2.0.15"],
        python_callable=parse_covid,
        system_site_packages=False,
    )

    # dbt_transform_task = DockerOperator(
    #     # docker_url="unix://var/run/docker.sock",
    #     # docker_url='tcp://host.docker.internal:2375',
    #     docker_url='tcp://docker-socket-proxy:2375',
    #     api_version='auto',
    #     # docker_url='tcp://docker-proxy:2375',
    #     network_mode="host",
    #     task_id='dbt_transform_task',
    #     image='alisherrakhimov/dbt_for_covid:1.5.1',
    #     # image='python:3.8-slim-buster',
    #     command='debug',
    #     dag=dag
    # )

    # dbt_transform_task = BashOperator(
    #     task_id='dbt_transform_task',
    #     bash_command="""
    #         cd /opt/airflow/dbt
    #         ls -lah
    #         pip3 install dbt-postgres
    #         dbt debug
    #     """,
    #         # source venv/bin/activate
    #         # python3 -m venv venv
    # )

    transform_covid_task = PythonVirtualenvOperator(
        task_id="transform_covid_task",
        requirements=["psycopg2-binary==2.9.6", "sqlalchemy==2.0.15"],
        python_callable=transform_covid,
        system_site_packages=False,
    )

    parse_covid_task >> transform_covid_task
