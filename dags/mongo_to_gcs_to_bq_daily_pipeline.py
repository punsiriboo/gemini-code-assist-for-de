from airflow import DAG
from google.cloud import storage
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
import json
import pandas as pd

gcs_client = storage.Client()

# Define your DAG arguments
default_args = {
    'owner': 'your_email@example.com',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
with DAG(
    'daily_order_pipeline',
    default_args=default_args,
    description='Daily pipeline to ingest orders from MongoDB, transform, and load to BigQuery',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2024,6,1),
    catchup=True,
    tags=['data-pipeline', 'bigquery', 'gcs', 'mongodb'],
) as dag:

    # Task 1: Extract data from MongoDB and save to GCS
    def extract_orders_from_mongo(execution_date) -> None:
        # Connect to MongoDB
        mongo_hook = MongoHook(conn_id='ecommerce_mongo_conn')
        collection = mongo_hook.get_collection('orders')

        # Query orders for the current day
        yesterday = execution_date - timedelta(days=1)
        query = {'created_at': {'$gte': yesterday.strftime("%Y-%m-%d"), '$lt': execution_date.strftime("%Y-%m-%d")}}
        orders = list(collection.find(query))
        print(query)
        print(yesterday, execution_date)
        print(len(orders))
        
        # Convert MongoDB documents to JSON
        documents_json = json.dumps(orders, default=str)

        # Save orders to GCS as JSON
        gcs_bucket_name = 'de-data-th-gemini'
        gcs_file_name = f'landing/orders/{execution_date.strftime("%Y-%m-%d")}.json'

        bucket = gcs_client.bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_file_name)
        blob.upload_from_string(documents_json)

        print(f"Successfully uploaded data to gs://{gcs_bucket_name}/{gcs_file_name}")

    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders_from_mongo,
        provide_context=True,
    )

    # Task 2: Transform JSON data and save to GCS
    def transform_orders(execution_date) -> None:
        #Read JSON data from GCS using gcs_client
        gcs_bucket_name = 'de-data-th-gemini'
        gcs_file_name = f'landing/orders/{execution_date.strftime("%Y-%m-%d")}.json'
        blob = gcs_client.bucket(gcs_bucket_name).blob(gcs_file_name)
        raw_orders = json.loads(blob.download_as_string())

        # Flatten the JSON data into two tables: order and order_item and save result to parquet
        # Initialize lists for orders and order_items
        orders = []
        order_items = []

        # Process the JSON data
        for order in raw_orders:
            # Extract order information
            order_info = {k: v for k, v in order.items() if k != 'items'}
            orders.append(order_info)
            
            # Extract order items
            for item in order['items']:
                item_info = item.copy()
                item_info['order_id'] = order['order_id']  # Link item to order
                order_items.append(item_info)

        # Convert to DataFrame for better visualization
        orders_df = pd.DataFrame(orders)
        order_items_df = pd.DataFrame(order_items)
        
        #Upload orders_df, order_items_df to GSC folder transfroming use to_parquet gs://{gcs_bucket_name}/
        orders_df.to_parquet(f'gs://{gcs_bucket_name}/transforming/order/{execution_date.strftime("%Y-%m-%d")}.parquet', index=False)
        order_items_df.to_parquet(f'gs://{gcs_bucket_name}/transforming/order_items/{execution_date.strftime("%Y-%m-%d")}.parquet', index=False)


    #TODO:
    #Use PythonOperator to call transform_orders


    # Task 3: Load transformed data to BigQuery
