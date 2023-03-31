# Data-Engineer - Deloitte - Study Case

## Deloitte

Delloite is a multinational professional services company that aims to gain insights from their business data to optimize operations and make data-driven decisions. This project will enable Delloite to ingest generated dummy data, process it, and visualize the results using a modern data stack on GCP.

### Data Pipeline Steps

1. **Infrastructure**: Provision necessary GCP resources using Terraform.
2. **CI/CD**: Use GitHub Actions as a CI/CD platform for the Terraform infrastructure.
3. **Data Modeling**: Use Python to create a script that generates dummy data for the Deloitte project context, simulating: (Clients, Projects, Employees, Financial Transactions, Customer Behavior, Sales, Marketing Campaigns).
4. **Data Ingestion**: Set up GCP Cloud SQL and insert generated data.
5. **Data Lake Raw Zone**: Store the retrieved data in the Data Lake Raw Zone on GCP Raw Storage using Python.
6. **Data Processing**: Create a Dataflow pipeline to read data from the Raw Zone in GCP Storage, and apply transformations.
7. **Data Lake Processing Zone**: Write the processed data back to the Data Lake Process Zone on GCP Storage.
8. **Data Warehouse**: Load transformed data into BigQuery Data Warehouse.
9. **Analytics Engineer**: Set up DBT and create the necessary transformation models.
10. **Data Orchestration**:  Set up GCP Composer and configure an dag for Apache Airflow environment.

### Pipeline Diagram

![alt text](https://github.com/makima0499/5.Data-Engineer/blob/main/5.DataPipeline.png)

### Tools

* Python
* Terraform
* Github Actions
* GCP SQL
* GCP Storage
* GCP Dataflow
* GCP BigQuery
* GCP Composer(Airflow)
* DBT

### Note

This repository is provided for study purposes only, focusing on data engineering pipelines.

## License

[MIT](https://choosealicense.com/licenses/mit/)
