import datetime
import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# pg_hook = PostgresHook(postgres_conn_id='')

default_args = {"owner": "airflow"}

with DAG(
    dag_id="postgres_operator_dag_pets",
    start_date=dt.datetime.now(),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres-rfu-test-delete-after-Jun2021",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )

    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="postgres-rfu-test-delete-after-Jun2021",
        sql="""
            INSERT INTO pet VALUES (1, 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet VALUES (2, 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet VALUES (3, 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet VALUES (4, 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )

    get_all_pets = PostgresOperator(
        task_id="get_all_pets", postgres_conn_id="postgres-rfu-test-delete-after-Jun2021", sql="SELECT * FROM pet;"
    )

    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id="postgres-rfu-test-delete-after-Jun2021",
        sql="""
            SELECT * FROM pet
            WHERE birth_date > {{ params.begin_date }} AND birth_date < {{ params.end_date }};
            """,
        params={'begin_date': '2020-01-01', 'end_date': '2020-12-31'},
    )

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
