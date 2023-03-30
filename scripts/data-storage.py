import os
import csv
from io import StringIO
import psycopg2
import configparser
from google.cloud import storage

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/5-Project/config/config.ini")

# GCP PostgreSQL instance details
host = config.get("cloudsql", "host")
dbname = config.get("cloudsql", "dbname") 
user = config.get("cloudsql", "user")
password = config.get("cloudsql", "password")
port = config.get("cloudsql", "port")

# GCP Cloud Storage details
project_id = config.get("cloudstorage", "project_id")
bucket_name = config.get("cloudstorage", "bucket_name")

# Connect to GCP PostgreSQL instance
def connect_gcp_postgres(host, dbname, user, password, port=5432):
    conn_str = f"host='{host}' dbname='{dbname}' user='{user}' password='{password}' port={port}"
    conn = psycopg2.connect(conn_str)
    return conn

# Get table data from PostgreSQL
def get_table_data(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name}")
        data = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
    return data, column_names

# Convert data to CSV
def data_to_csv(data, column_names):
    csv_file = StringIO()
    writer = csv.writer(csv_file)
    writer.writerow(column_names)
    writer.writerows(data)
    csv_file.seek(0)
    return csv_file

# Upload CSV to GCP Cloud Storage
def upload_csv_to_gcs(project_id, bucket_name, csv_file, destination_path):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_file(csv_file, content_type='text/csv')
    print(f"Uploaded {destination_path} to GCS bucket {bucket_name}.")

# Main script
table_names = [
    "clients",
    "projects",
    "employees",
    "financial_transactions",
    "customer_behavior",
    "sales",
    "marketing_campaigns"
]

conn = connect_gcp_postgres(host, dbname, user, password, port)

for table_name in table_names:
    data, column_names = get_table_data(conn, table_name)
    csv_file = data_to_csv(data, column_names)
    destination_path = f"raw/{table_name}.csv"
    upload_csv_to_gcs(project_id, bucket_name, csv_file, destination_path)

conn.close()
