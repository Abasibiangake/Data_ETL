import re
import time
import os
import csv
import pymysql
import subprocess
from google.cloud import bigquery

def establish_vpn_connection(vpn_config_path):
    # Example command to establish an OpenVPN connection using the provided config file
    subprocess.run(['openvpn', '--config', vpn_config_path])

def connect_to_mysql_server(host, port, user, password, database):
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    return connection

def get_table_schema(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        schema = cursor.fetchall()
    return schema

#get table list of necessary tables from the csv file
def get_all_table_names_from_csv(file_path):
    table_names = []
    unique_names = set()

    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) >= 2 and row[2] != "" and row[2] != "Tabla":
                table_name = row[2].strip()
                if table_name not in unique_names:
                    table_names.append(table_name)
                    unique_names.add(table_name)
    return table_names

def sanitize_field_name(field_name):
    # Replace invalid characters with underscores
    sanitized_name = re.sub(r"[^a-zA-Z0-9_]", "_", field_name)

    # Ensure the field name is within the allowed character limit (300 characters)
    return sanitized_name[:300]

def upload_data_to_bigquery(project_id, dataset_id, table_name, schema):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    
    # Define the table schema
    table_schema = []
    for field in schema:
        field_name = field[0]
        sanitized_name = sanitize_field_name(field_name)
        field_type = 'STRING'
        table_schema.append(bigquery.SchemaField(sanitized_name, field_type))
    
    # Add the new column "LAST_DATE" to the schema
    table_schema.append(bigquery.SchemaField("LAST_DATE", "STRING"))

    # Create the empty table with the specified schema
    table = bigquery.Table(table_ref, schema=table_schema)
    table = client.create_table(table)  # API request

    print(f"Empty table '{table.project}.{table.dataset_id}.{table.table_id}' created in BigQuery.")
def delete_dataset(project_id, dataset_id):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
    print(f"Deleted dataset '{dataset_id}' from BigQuery.")


def create_dataset(project_id, dataset_id):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    dataset = client.create_dataset(dataset)
    print(f"Created dataset '{dataset_id}' in BigQuery.")
# MySQL connection details
host = 'innova-0001-wafr-istst-cluster.cluster-cyyx3ysmbggw.us-east-1.rds.amazonaws.com'
port = 3306
user = 'usr_rvillareal'
password = 'b2MSnV7p0aB$vDB8'
database = 'datamanagement'
project_id = 'tst-peinnovabi-data-storage'
dataset_id = 'ds_dm_raw_level'

# VPN configuration file path
vpn_config_path = './credentials/client.ovpn'

# csv file path
csv_file_path = './data_folder/Listado de Tablas-DM.csv'

# Establish the VPN connection
establish_vpn_connection(vpn_config_path)

# Wait for the VPN connection to be established
time.sleep(10)
print('Processing tables:')

connection = connect_to_mysql_server(host, port, user, password, database)
print('Connected:')
delete_dataset(project_id, dataset_id)
create_dataset(project_id, dataset_id)

table_names = get_all_table_names_from_csv(csv_file_path)
print(table_names)
# Save table names to a text file
with open('table_names.txt', 'w') as file:
    for table_name in table_names:
        file.write(table_name + '\n')
    print('text file save')

processed_table_names = set()  # Set to store processed table names

for table_name in table_names:
    if table_name in processed_table_names:
        print(f'Skipping table {table_name} due to duplication.')
        continue
    schema = get_table_schema(connection, table_name)
    print(f'Table schema for {table_name}:', schema)
    bq_table_name = f'dm_{table_name}'
    upload_data_to_bigquery(project_id, dataset_id, bq_table_name, schema)
    processed_table_names.add(table_name)  # Add the processed table name to the set

connection.close()
