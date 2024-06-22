# DAG_1 - ingest data from Google Cloud Storage to BigQuery

PROMPT_1:
Create airflow dag with 1 task that use GCSToBigQueryOperator to upload products.csv to BigQuery
bucket_name=`de-data-th-gemini`
destination_project_dataset_table=`gemini-nt-test-2.data_th_de_ecommerce.products`
skip first header row

PROMPT_2:
create dag doc for dag_id upload_products_to_bq

PROMPT_3:
create import and create a BashOperator to print success message after upload_products


# DAG_2 - ingest data from Mongo database to Google Cloud BigQuery

PROMPT:

Create an Airflow DAG named daily_order_pipeline to run a daily ETL process. The pipeline should have the following characteristics:

Start date: June 1, 2024
Schedule interval: Daily
The pipeline should include three main tasks:

1. Extract data from MongoDB:

Use a PythonOperator named `extract_orders`.
Connect to MongoDB using MongoHook with the connection ID `ecommerce_mongo_conn`.
Query the orders collection for records created on the previous day using the execution_date.
Convert the queried MongoDB documents to JSON.
Upload the JSON data to Google Cloud Storage (GCS) in the bucket `de-data-th-gemini` with the file path `landing/orders/{execution_date}.json`

2. Transform the data:

Use a PythonOperator named `transform_orders`
Read the JSON data from GCS.
Flatten the JSON data into two tables: orders and order_items.
Save the flattened data as parquet files in GCS in the paths transforming/order/{execution_date}.parquet and transforming/order_items/{execution_date}.parquet.

3. Load the data to BigQuery:

Use two GCSToBigQueryOperators named `load_order` and `load_order_item`.
Load the parquet files from GCS into BigQuery tables:
transforming/order/*.parquet to gemini-nt-test-2.data_th_de_ecommerce.orders.
transforming/order_items/*.parquet to gemini-nt-test-2.data_th_de_ecommerce.order_items


