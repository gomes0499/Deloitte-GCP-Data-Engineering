from google.cloud import bigquery
from google.cloud import storage
import configparser

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/5-Project/config/config.ini")

project_id = config.get("bigquery", "project_id")
bucket_name = config.get("bigquery", "bucket_name")
input_folder = config.get("bigquery", "input_folder")
dataset_id = config.get("bigquery", "dataset_id")

# Initialize the BigQuery and Storage clients
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)

# Function to list all the parquet files in the input folder
def list_parquet_files(bucket_name, input_folder):
    bucket = storage_client.get_bucket(bucket_name)
    return [blob.name for blob in bucket.list_blobs(prefix=input_folder) if blob.name.endswith('.parquet')]

# Function to load a parquet file into BigQuery
def load_parquet_to_bigquery(file_path, table_id):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET
    )
    uri = f"gs://{bucket_name}/{file_path}"
    load_job = bigquery_client.load_table_from_uri(
        uri, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config
    )
    load_job.result()
    print(f"Loaded parquet file '{file_path}' into '{table_id}'")

# List and load all the parquet files into BigQuery
parquet_files = list_parquet_files(bucket_name, input_folder)
for file_path in parquet_files:
    table_id = file_path.split('/')[-1].split('.')[0]  # Extract the table name from the file path
    load_parquet_to_bigquery(file_path, table_id)
