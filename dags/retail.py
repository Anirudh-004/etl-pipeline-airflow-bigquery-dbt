import os
import sys
from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
# from airflow.providers.dbt.cloud.operators.dbt import DbtRunOperator
from airflow.operators.bash import BashOperator


def load_gcs_to_bigquery():
    # Use Airflow's BigQueryHook to get credentials
    bq_hook = BigQueryHook(gcp_conn_id="gcp")
    bq_client = bq_hook.get_client()

    # Define GCS bucket and file path
    bucket_name = 'online_retail_data_storage'
    file_path = 'raw/Online_Retail.csv'
    destination_table_id = 'airflow-retail-data-pipeline.retail.invoice_data'

    # Define the BigQuery table schema if the table doesn't exist
    schema = [
        bigquery.SchemaField("InvoiceNo", "STRING"),
        bigquery.SchemaField("StockCode", "STRING"),
        bigquery.SchemaField("Description", "STRING"),
        bigquery.SchemaField("Quantity", "INTEGER"),
        bigquery.SchemaField("InvoiceDate", "STRING"),
        bigquery.SchemaField("UnitPrice", "FLOAT"),
        bigquery.SchemaField("CustomerID", "FLOAT"),
        bigquery.SchemaField("Country", "STRING"),
    ]

    # Check if the table exists in BigQuery, if not, create it
    try:
        bq_client.get_table(destination_table_id)
        print(f"Table {destination_table_id} already exists.")
    except NotFound:
        table = bigquery.Table(destination_table_id, schema=schema)
        table = bq_client.create_table(table)
        print(f"Table {destination_table_id} created.")

    # Load data from GCS to BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row in the CSV
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to table
    )

    # GCS URI format: gs://<bucket-name>/<file-path>
    uri = f"gs://{bucket_name}/{file_path}"

    load_job = bq_client.load_table_from_uri(
        uri, destination_table_id, job_config=job_config
    )  # Load data into BigQuery

    load_job.result()  # Wait for the job to complete

    print(f"Data loaded successfully into {destination_table_id}.")

expected_schema = {
    "InvoiceNo": "object",
    "StockCode": "object",
    "Description": "object",
    "Quantity": "Int64",
    "InvoiceDate": "object",
    "UnitPrice": "float64",
    "CustomerID": "float64",
    "Country": "object"
}

# Function to check schema
def check_schema(df, expected_schema):
    actual_schema = df.dtypes.apply(lambda x: x.name).to_dict()  # Get actual dtypes as a dictionary
    schema_check = True

    for column, expected_dtype in expected_schema.items():
        actual_dtype = actual_schema.get(column)
        if actual_dtype != expected_dtype:
            print(f"Data type mismatch in column '{column}': expected {expected_dtype}, got {actual_dtype}")
            schema_check = False
        else:
            print(f"Column '{column}' has correct data type: {actual_dtype}")

    if schema_check:
        print("Schema check passed.")
    else:
        print("Schema check failed.")

# Function to run quality checks
def run_quality_checks(**kwargs):
    # Use BigQueryHook to get the BigQuery client
    bq_hook = BigQueryHook(gcp_conn_id="gcp")  # The connection ID you set up in Airflow
    bq_client = bq_hook.get_client()

    # Define the SQL query
    dataset_id = "retail"
    table_id = "invoice_data"
    query = f"SELECT * FROM `{dataset_id}.{table_id}`"

    # Execute the query and get the results as a Pandas dataframe
    df = bq_client.query(query).to_dataframe()

    # Perform schema check
    check_schema(df, expected_schema)


@dag(
    start_date=datetime(2024, 9, 11),
    schedule=None,
    catchup=False,
    tags=['retail'],
)
def retail():
    ingest_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='ingest_csv_to_gcs',
        src='/usr/local/airflow/data/dataset/Online_Retail.csv',
        dst='raw/Online_Retail.csv',
        bucket='online_retail_data_storage',
        gcp_conn_id='gcp',
        mime_type='text/csv'
    )
    build_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="build_retail_dataset",
        dataset_id="retail",
        gcp_conn_id="gcp",
    )
    load_to_bigquery = PythonOperator(
        task_id="load_gcs_to_bigquery",
        python_callable=load_gcs_to_bigquery
    )
    schema_check_task = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )
    # Add the dbt run task
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'docker run --rm -v /usr/app/dbt:/usr/app/dbt '
            'dbt-airflow-bigquery dbt run --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt/my_dbtproject'
        ),
    )

    # Pipeline run upstream tasks
    ingest_csv_to_gcs >> build_retail_dataset >> load_gcs_to_bigquery >> run_quality_checks >> dbt_run

retail()