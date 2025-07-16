#DAGs
#Tasks: Extract Data from RAPiDAPI, Transform the extracted data and store the data in a table in my postgres database 
#Operators: Python Opterator and PostgresOperator 
#Hooks: Necessary for DB connectiom

from datetime import datetime,timedelta 
from airflow.models import DAG
import requests
import os
import pandas as pd
from dotenv import load_dotenv
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

load_dotenv()
API_KEY = os.getenv('APIKEY')

# Set up API request headers to authenticate requests
headers = {
    'X-RapidAPI-Key': API_KEY,
    'X-RapidAPI-Host': 'real-time-amazon-data.p.rapidapi.com'
}

# Set up API URL and parameters
url = "https://real-time-amazon-data.p.rapidapi.com/products-by-category"
querystring = {"category_id":"281407","page":"2","country":"US","sort_by":"RELEVANCE","product_condition":"ALL","is_prime":"false","deals_and_discounts":"NONE"}


def extract(**context):
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()

        extracted_data = response.json()
        print("Fetched data:", extracted_data)

        context['ti'].xcom_push(key='raw_export', value=extracted_data)
    except requests.exceptions.RequestException as error:
        print("Reason for failed request:", error)
        context['ti'].xcom_push(key='raw_export', value={})

def transform(**context):
    raw_export = context['ti'].xcom_pull(task_ids='fetch_data', key='raw_export')

    if not raw_export:
        print("No data to transform")
        context['ti'].xcom_push(key='transformed_data', value=[])
        return

    print("Pulled from XCom:", raw_export) #For Debugging
    product_information = []

    for product in raw_export.get('data', {}).get('products', []):
        product_entry = {
            'asin': product.get('asin'),
            'title': product.get('product_title'),
            'currency': product.get('currency'),
            'price': product.get('product_price'),
            'original_price': product.get('product_original_price'),
            'rating': product.get('product_star_rating'),
            'num_ratings': product.get('product_num_ratings'),
            'sales_volume': product.get('sales_volume')
        }
        product_information.append(product_entry)

    print("Transformed data:", product_information)
    context['ti'].xcom_push(key='transformed_data', value=product_information)

def load_data(**context):
    transformed_data = context['ti'].xcom_pull(task_ids='transform_data',key='transformed_data')

    if not transformed_data:
        print("No data to insert.")
        return

    insert_query = """
    INSERT INTO amazon_products (
        asin, title, currency, price,
        original_price, rating, num_ratings, sales_volume
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (asin) DO NOTHING;
    """

    try:
        pg_hook = PostgresHook(postgres_conn_id='rapidapi_amazon_data_connection')  

        for product in transformed_data:
            pg_hook.run(insert_query, parameters=(
                product['asin'],
                product['title'],
                product['currency'],
                product['price'],
                product['original_price'],
                product['rating'],
                product['num_ratings'],
                product['sales_volume']
            ))

        print("Data inserted successfully using PostgresHook.")
    except Exception as e:
        print(f"Error inserting data using PostgresHook: {e}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'start_date': datetime(2025, 7, 15, 10, 0), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'extract_and_load_amazon_data',
    default_args=default_args,
    description='Simple DAG to extract data from RapidAPI and store it in Postgre',
    schedule='0 10 * * *',
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=extract,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='rapidapi_amazon_data_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS amazon_products (
        asin TEXT PRIMARY KEY,
        title TEXT,
        currency TEXT NOT NULL,
        price TEXT,
        original_price TEXT,
        rating NUMERIC,
        num_ratings TEXT,
        sales_volume TEXT
    );
    """,
    dag=dag,
)

insert__data_task = PythonOperator(
    task_id='insert_data',
    python_callable=load_data,
    dag=dag,
)

#dependencies
fetch_data_task >> transform_data_task >> create_table_task >> insert__data_task