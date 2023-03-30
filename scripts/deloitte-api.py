import random
from faker import Faker
import csv
import psycopg2
import os
import configparser
from psycopg2 import sql

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/5-Project/config/config.ini")


fake = Faker()
# Variables for generating dummy data for Deloitte
NUM_CLIENTS = 100
NUM_PROJECTS = 200
NUM_EMPLOYEES = 50
NUM_TRANSACTIONS = 500
NUM_CUSTOMERS = 200
NUM_SALES = 400
NUM_CAMPAIGNS = 50

# GCP PostgreSQL instance details
host = config.get("cloudsql", "host")
dbname = config.get("cloudsql", "dbname") 
user = config.get("cloudsql", "user")
password = config.get("cloudsql", "password")
port = config.get("cloudsql", "port")

# Clients
with open('clients.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['client_id', 'client_name', 'industry', 'country', 'state', 'city', 'contact_name', 'contact_email', 'contact_phone_number'])

    for i in range(NUM_CLIENTS):
        writer.writerow([
            i+1, 
            fake.company(), 
            fake.job(), 
            fake.country(), 
            fake.state(), 
            fake.city(), 
            fake.name(), 
            fake.email(), 
            fake.phone_number()
        ])

# Projects
with open('projects.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['project_id', 'client_id', 'project_name', 'project_type', 'start_date', 'end_date', 'project_status'])

    project_types = ['Consulting', 'Audit', 'Tax', 'Advisory']
    project_statuses = ['In Progress', 'Completed', 'Cancelled']

    for i in range(NUM_PROJECTS):
        writer.writerow([
            i+1, 
            random.randint(1, NUM_CLIENTS), 
            fake.bs(), 
            random.choice(project_types), 
            fake.date_between(start_date='-2y', end_date='today'), 
            fake.date_between(start_date='today', end_date='+2y'), 
            random.choice(project_statuses)
        ])

# Employees
with open('employees.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['employee_id', 'first_name', 'last_name', 'email', 'job_title', 'department', 'hire_date', 'country', 'state', 'city'])

    departments = ['Consulting', 'Audit', 'Tax', 'Advisory']

    for i in range(NUM_EMPLOYEES):
        writer.writerow([
            i+1, 
            fake.first_name(), 
            fake.last_name(), 
            fake.email(), 
            fake.job(), 
            random.choice(departments), 
            fake.date_between(start_date='-10y', end_date='today'), 
            fake.country(), 
            fake.state(), 
            fake.city()
        ])

# Financial Transactions
with open('financial_transactions.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['transaction_id', 'project_id', 'employee_id', 'client_id', 'transaction_date', 'transaction_type', 'amount', 'currency', 'description'])

    transaction_types = ['Invoice', 'Payment', 'Expense']

    for i in range(NUM_TRANSACTIONS):
        writer.writerow([
            i+1, 
            random.randint(1, NUM_PROJECTS), 
            random.randint(1, NUM_EMPLOYEES), 
            random.randint(1, NUM_CLIENTS), 
            fake.date_between(start_date='-1y', end_date='today'), 
            random.choice(transaction_types), 
            round(random.uniform(100, 10000), 2), 
            'USD', 
            fake.text(max_nb_chars=100)
        ])

# Customer Behavior
with open('customer_behavior.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['customer_id', 'age', 'gender',
    'country', 'state', 'city', 'total_spending', 'preferred_communication_channel'])

    communication_channels = ['email', 'phone', 'in-person']

    for i in range(NUM_CUSTOMERS):
        writer.writerow([
            i+1, 
            random.randint(18, 70), 
            random.choice(['M', 'F']), 
            fake.country(), 
            fake.state(), 
            fake.city(), 
            round(random.uniform(100, 10000), 2), 
            random.choice(communication_channels)
        ])

# Sales
with open('sales.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['sales_id', 'product_service_id', 'customer_id', 'employee_id', 'sale_date', 'quantity', 'unit_price', 'discount', 'total_amount'])

    for i in range(NUM_SALES):
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(50, 500), 2)
        discount = round(random.uniform(0, 0.3), 2)
        total_amount = round(quantity * unit_price * (1 - discount), 2)
        writer.writerow([
            i+1, 
            fake.uuid4(), 
            random.randint(1, NUM_CUSTOMERS), 
            random.randint(1, NUM_EMPLOYEES), 
            fake.date_between(start_date='-1y', end_date='today'), 
            quantity, 
            unit_price, 
            discount, 
            total_amount
        ])

# Marketing Campaigns
with open('marketing_campaigns.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['campaign_id', 'campaign_name', 'start_date', 'end_date', 'channel', 'target_audience', 'budget', 'leads_generated', 'conversions'])
    channels = ['email', 'social media', 'print']

    for i in range(NUM_CAMPAIGNS):
        writer.writerow([
            i+1, 
            fake.catch_phrase(), 
            fake.date_between(start_date='-1y', end_date='today'), 
            fake.date_between(start_date='today', end_date='+1y'), 
            random.choice(channels), 
            fake.job(), 
            round(random.uniform(1000, 10000), 2), 
            random.randint(10, 100), 
            random.randint(1, 10)
        ])

def connect_gcp_postgres(host, dbname, user, password, port=5432):
    conn_str = f"host='{host}' dbname='{dbname}' user='{user}' password='{password}' port={port}"
    conn = psycopg2.connect(conn_str)
    return conn

def create_table(conn, table_name, schema):
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")
        conn.commit()

def insert_data(conn, table_name, data):
    with conn.cursor() as cur:
        for row in data:
            columns = ', '.join(row.keys())
            values = ', '.join([f"%s" for _ in row.values()])
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(columns),
                sql.SQL(values)
            )
            cur.execute(query, tuple(row.values()))
        conn.commit()

def read_csv(filename):
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        data = [row for row in reader]
    return data


# Table schemas
clients_schema = """
    client_id SERIAL PRIMARY KEY,
    client_name VARCHAR(255),
    industry VARCHAR(255),
    country VARCHAR(255),
    state VARCHAR(255),
    city VARCHAR(255),
    contact_name VARCHAR(255),
    contact_email VARCHAR(255),
    contact_phone_number VARCHAR(255)
"""

projects_schema = """
    project_id SERIAL PRIMARY KEY,
    client_id INTEGER,
    project_name VARCHAR(255),
    project_type VARCHAR(255),
    start_date DATE,
    end_date DATE,
    project_status VARCHAR(255)
"""

employees_schema = """
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    job_title VARCHAR(255),
    department VARCHAR(255),
    hire_date DATE,
    country VARCHAR(255),
    state VARCHAR(255),
    city VARCHAR(255)
"""

financial_transactions_schema = """
    transaction_id SERIAL PRIMARY KEY,
    project_id INTEGER,
    employee_id INTEGER,
    client_id INTEGER,
    transaction_date DATE,
    transaction_type VARCHAR(255),
    amount NUMERIC(10, 2),
    currency VARCHAR(3),
    description TEXT
"""

customer_behavior_schema = """
    customer_id SERIAL PRIMARY KEY,
    age INTEGER,
    gender CHAR(1),
    country VARCHAR(255),
    state VARCHAR(255),
    city VARCHAR(255),
    total_spending NUMERIC(10, 2),
    preferred_communication_channel VARCHAR(255)
"""

sales_schema = """
    sales_id SERIAL PRIMARY KEY,
    product_service_id UUID,
    customer_id INTEGER,
    employee_id INTEGER,
    sale_date DATE,
    quantity INTEGER,
    unit_price NUMERIC(10, 2),
    discount NUMERIC(4, 2),
    total_amount NUMERIC(10, 2)
"""

marketing_campaigns_schema = """
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    channel VARCHAR(255),
    target_audience VARCHAR(255),
    budget NUMERIC(10, 2),
    leads_generated INTEGER,
    conversions INTEGER
"""


# Connect to the database
conn = connect_gcp_postgres(host, dbname, user, password, port)

# Read CSV data and create tables
csv_files_schemas = {
    'clients.csv': clients_schema,
    'projects.csv': projects_schema,
    'employees.csv': employees_schema,
    'financial_transactions.csv': financial_transactions_schema,
    'customer_behavior.csv': customer_behavior_schema,
    'sales.csv': sales_schema,
    'marketing_campaigns.csv': marketing_campaigns_schema
}


for filename, schema in csv_files_schemas.items():
    table_name = os.path.splitext(filename)[0]
    data = read_csv(filename)

    if data:
        create_table(conn, table_name, schema)
        insert_data(conn, table_name, data)

# Close the connection
conn.close()