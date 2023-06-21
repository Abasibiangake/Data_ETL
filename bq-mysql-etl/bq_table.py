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

def get_all_table_names(connection):
    with connection.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
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

    # # Delete the table if it already exists
    # table = client.get_table(table_ref, retry=None)
    # if table:
    #     client.delete_table(table_ref)
    
    #Check if the table already exists
    # if client.get_table(table_ref, retry=None):
    #     print(f"Table {table_name} already exists in BigQuery.")
    #     return
    
    # Define the table schema
    table_schema = []
    for field in schema:
        field_name = field[0]
        sanitized_name = sanitize_field_name(field_name)
        field_type = 'STRING'
        table_schema.append(bigquery.SchemaField(sanitized_name, field_type))

    # Create the empty table with the specified schema
    table = bigquery.Table(table_ref, schema=table_schema)
    table = client.create_table(table)  # API request

    print(f"Empty table '{table.project}.{table.dataset_id}.{table.table_id}' created in BigQuery.")

# MySQL connection details
host = 'innova-0001-wafr-istst-cluster.cluster-cyyx3ysmbggw.us-east-1.rds.amazonaws.com'
port = 3306
user = 'usr_rvillareal'
password = 'b2MSnV7p0aB$vDB8'
database = 'datamanagement'
project_id = 'tst-peinnovabi-data-storage'
dataset_id = 'ds_dwh_raw3_mysql'

# VPN configuration file path
vpn_config_path = './credentials/client.ovpn'

# Establish the VPN connection
establish_vpn_connection(vpn_config_path)

# Wait for the VPN connection to be established
time.sleep(10)
print('Processing tables:')

connection = connect_to_mysql_server(host, port, user, password, database)
print('Connected:')

table_names = get_all_table_names(connection)
# Save table names to a text file
with open('table_names.txt', 'w') as file:
    for table_name in table_names:
        file.write(table_name + '\n')
    print('text file save')

for table_name in table_names:
    schema = get_table_schema(connection, table_name)
    print(f'Table schema for {table_name}:', schema)
    bq_table_name = f'dwh_{table_name}'
    upload_data_to_bigquery(project_id, dataset_id, bq_table_name, schema)

connection.close()
