from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

#TODO: use Gemini Code Assit to write DAG doc : https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
#PROMPT: create dag doc for dag_id upload_products_to_bq

# Define your DAG
with DAG(
    dag_id="upload_products_to_bq",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # Run daily
    catchup=False,
    doc_md=__doc__,
) as dag:

    # Define the task to upload the products.csv file to BigQuery
    upload_products = GCSToBigQueryOperator(
        task_id="upload_products",
        bucket="de-data-th-gemini",
        source_objects=["products.csv"],
        destination_project_dataset_table="gemini-nt-test-2.data_th_de_ecommerce.products",
        source_format="CSV",
        skip_leading_rows=1,  # If your CSV has a header row
        write_disposition="WRITE_TRUNCATE",  # Replace existing data
        create_disposition="CREATE_IF_NEEDED",  # Create the table if it doesn't exist
    )   

    #PROMPT: import and create a bash operator to print success message after upload_products
    print_success = BashOperator(
        task_id='print_success',
        bash_command='echo "Successfully uploaded products to BigQuery!"'
    )

    upload_products >> print_success