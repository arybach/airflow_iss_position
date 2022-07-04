import json
from datetime import date, datetime, timedelta
import requests
import pandas as pd
from numpy import double, integer
import psycopg2

from pandas.io.json._normalize import nested_to_record
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
import logging

args = {
    'owner': 'groot',
    'start_date': datetime(2022, 7, 1),
    'retries': 5,
    'retry_delay': timedelta(hours=1),
    'provide_context':True
}

# initialize default logger specified in airflow.cfg
logger = logging.getLogger("airflow.task")

def extract_data(**kwargs):
    ti = kwargs['ti']
    # Запрос на прогноз со следующего часа
    url = "http://api.open-notify.org/iss-now.json"
    response = requests.get(url)
    # response should look like:
    # res = {"timestamp": 1656407218, "message": "success",
    #       "iss_position": {"longitude": "84.3682", "latitude": "-36.8500"}}

    if response.status_code==200:
        json_data = json.loads(response.content)
        ti.xcom_push(key='iss_json', value=json_data)
    else:
        logger.error(print(response))


def transform_data(**kwargs):
    ti = kwargs['ti']
    # set enable_xcom_pickling = True in airflow.cfg for basic deserialization
    json_data = ti.xcom_pull(key='iss_json', task_ids=['extract_data'])[0]
    flat = nested_to_record(json_data, sep='_')
        
    # logger.info(print(flat))
    df = pd.DataFrame(flat, index=['timestamp']).drop(columns=['message']).rename(
        columns={'iss_position_longitude': 'longitude', 'iss_position_latitude': 'latitude'})
    
    # looks like this:
    #             timestamp longitude  latitude
    # timestamp  1656407218   84.3682  -36.8500
    logger.info(print(df))
    ti.xcom_push(key='iss_df', value=df)

def load_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(key='iss_df', task_ids=['transform_data'])[0]
    logger.info(print(df))
    
    try:
        
        # connection password in .pgpass file
        conn = psycopg2.connect("""
           host=rc1b-fmzs0tguc8g3o5m0.mdb.yandexcloud.net
           port=6432
           dbname=postgresdb
           user=groot
           target_session_attrs=read-write
           sslmode=verify-full
        """)
    
    except Exception as error:
        logger.error(error)
    
    cursor  = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS issposition (
            timestamp TIMESTAMP,
            longitude NUMERIC,
            latitude NUMERIC,
            issposid SERIAL PRIMARY KEY
        );
    """
    )
 
    cursor.execute("""
        INSERT INTO issposition
        VALUES ('{}', '{}', '{}')
    """.format(
        datetime.fromtimestamp(df['timestamp'].values[0]),
        double(df['longitude'].values[0]),
        double(df['latitude'].values[0])
        )
    )
    conn.commit()
                

with DAG('zero_in_on_iss', description='load_iss_geo', schedule_interval='*/10 * * * *',  catchup=False,default_args=args) as dag: #0 * * * *   */1 * * * *
        extract_data    = PythonOperator(task_id='extract_data', python_callable=extract_data)
        transform_data  = PythonOperator(task_id='transform_data', python_callable=transform_data)
        load_data       = PythonOperator(task_id='load_data', python_callable=load_data)

        extract_data >> transform_data >> load_data