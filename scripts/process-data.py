import argparse
import logging
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp import gcsio

# Define the schema dictionaries for each table
schemas = {
    "clients": ("client_id,client_name,industry,country,state,city,contact_name,contact_email,contact_phone_number", "clients_schema"),
    "projects": ("project_id,client_id,project_name,project_type,start_date,end_date,project_status", "projects_schema"),
    "employees": ("employee_id,first_name,last_name,email,job_title,department,hire_date,country,state,city", "employees_schema"),
    "financial_transactions": ("transaction_id,project_id,employee_id,client_id,transaction_date,transaction_type,amount,currency,description", "financial_transactions_schema"),
    "customer_behavior": ("customer_id,age,gender,country,state,city,total_spending,preferred_communication_channel", "customer_behavior_schema"),
    "sales": ("sales_id,product_service_id,customer_id,employee_id,sale_date,quantity,unit_price,discount,total_amount", "sales_schema"),
    "marketing_campaigns": ("campaign_id,campaign_name,start_date,end_date,channel,target_audience,budget,leads_generated,conversions","marketing_campaigns_schema"),
}

class ReadCSVFiles(beam.PTransform):
    def __init__(self, project_id, bucket_name):
        self.project_id = project_id
        self.bucket_name = bucket_name

    def list_csv_files(self):
        from google.cloud import storage
        gcs = storage.Client(project=self.project_id)
        bucket = gcs.get_bucket(self.bucket_name)
        return [f"gs://{self.bucket_name}/{blob.name}" for blob in bucket.list_blobs(prefix='raw/') if blob.name.endswith(".csv")]

    def expand(self, pcoll):
        return (
            pcoll
            | "List CSV Files" >> beam.Create(self.list_csv_files())
            | "Read CSV Files" >> beam.ParDo(self.read_csv_file())
        )

    class read_csv_file(beam.DoFn):
        def process(self, file_name):
            gcs = gcsio.GcsIO()
            with gcs.open(file_name) as f:
                table_name = file_name.split("/")[-1].split(".")[0]
                schema, table_schema = schemas[table_name]
                yield (table_name, pd.read_csv(f, header=None, names=schema.split(","), na_values=""))

class WriteParquetFiles(beam.PTransform):
    def __init__(self, gcp_project, bucket_name):
        self.gcp_project = gcp_project
        self.bucket_name = bucket_name

    def expand(self, pcoll):
        return (pcoll
                | "Drop NA Rows" >> beam.MapTuple(lambda table_name, df: (table_name, df.dropna()))
                | "Write Parquet Files" >> beam.ParDo(self.write_parquet_file(self.bucket_name)))

    class write_parquet_file(beam.DoFn):
        def __init__(self, bucket_name):
            self.bucket_name = bucket_name

        def process(self, table_name_df):
            table_name, df = table_name_df
            output_file = f"gs://{self.bucket_name}/process/{table_name}.parquet"
            gcs = gcsio.GcsIO()
            with gcs.open(output_file, mode="wb") as f:
                df.to_parquet(f, index=False)

def run(argv=None,save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="wu5projectgcp",help="The Google Cloud Project ID.")
    parser.add_argument("--input_bucket",default="wu5raw",help="The name of the input bucket.")
    parser.add_argument("--output_bucket",default="wu5process",help="The name of the output bucket.")
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_options = PipelineOptions(
    flags=argv,
    runner='DataflowRunner',
    project='wu5projectgcp',
    job_name='wu5job',
    temp_location='gs://wu5raw/temp',
    region='us-central1')
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        (p
     | "Read CSV Files" >> ReadCSVFiles(known_args.project, known_args.input_bucket)
     | "Write Parquet Files" >> WriteParquetFiles(known_args.project, known_args.output_bucket))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()