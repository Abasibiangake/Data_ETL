
import pandas as pd
import numpy as np
import time
import os
import csv
import jaydebeapi
import subprocess
from google.cloud import bigquery


def establish_vpn_connection(vpn_config_path):
    # Example command to establish an OpenVPN connection using the provided config file
    subprocess.run(['openvpn', '--config', vpn_config_path])


def connect_to_mssql_server(jdbc_driver, jdbc_url, username, password):
    # Provide the path to the JDBC driver JAR file
    jdbc_driver_jar = "./credentials/mssql-jdbc-12.2.0.jre11 (1).jar"
    connection = jaydebeapi.connect(jdbc_driver, jdbc_url, [username, password], jdbc_driver_jar)
    return connection


def get_table_schema(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute("EXEC sp_columns ?;", (table_name,))
        schema = cursor.fetchall()
    target_index = next((index for index, field in enumerate(schema) if field[2] == table_name), None)
    if target_index is None:
        return []  # Return an empty list if the table_name is not found in the schema
    
    #if the schema is not empty. then we perform filtering operation on the schema
    #such table schema comprises of a list of arrays. In each array " 'PEA_TST', 'dbo', table_name" repeats it self  and it causes conflict. 
    #modify the schema such that it only retrieves unique fields 
    new_schema = []
    for row in schema:
        new_row = row[3:]  # Exclude the first three elements from each tuple
        new_schema.append(new_row)
    print(new_schema)
    return new_schema
    # relevant_fields = schema[target_index]
    # print("this is the relevant fields: ", relevant_fields)
    # filtered_schema = [relevant_fields]
    


def get_all_table_names(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM [AOL-ORACARG-IN].sys.tables")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        print ('tabke in peat db', table_names)
    return table_names
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

def create_table_if_not_exists(project_id, dataset_id, table_name, schema):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)

    # if client.get_table(table_ref):
    #     print(f"Table '{table_name}' already exists in BigQuery.")
    # else:
    # Define the table schema
    table_schema = []
    for field in schema:
        field_name = field[0]
        field_type = 'STRING'
        table_schema.append(bigquery.SchemaField(field_name, field_type))

    # Create the empty table with the specified schema
    table = bigquery.Table(table_ref, schema=table_schema)
    table = client.create_table(table)  # API request
    print(f"Empty table '{table.project}.{table.dataset_id}.{table.table_id}' created in BigQuery.")

def clean_data(df):
    # Remove duplicates
    df = df.drop_duplicates()

    # Replace missing values with NaN
    df = df.replace('', np.nan)

    # Remove rows with missing values
    df = df.dropna()

    # Convert data types if needed
    df['column_name'] = df['column_name'].astype(int)

    # Perform additional cleaning and validation operations as necessary

    return df

# MsSQL connection details

project_id = 'tst-peinnovabi-data-storage'
dataset_id = 'ds_dwh_raw3_sql2'

#sql conn detail
# JDBC connection details
jdbc_url = "jdbc:sqlserver://172.35.30.7:1433;databaseName=PEA_TST;encrypt=false"
jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
username = "usr_rvillareal"
password = "TT3v9$vRTNNUsdML"

# VPN configuration file path
vpn_config_path = './credentials/client.ovpn'

# Establish the VPN connection
establish_vpn_connection(vpn_config_path)

# Wait for the VPN connection to be established
time.sleep(10)
print('Processing tables:')

connection = connect_to_mssql_server(jdbc_driver, jdbc_url, username, password)
print('Connected:')
delete_dataset(project_id, dataset_id)
create_dataset(project_id, dataset_id)

table_names = get_all_table_names(connection)
# Delete 'sql_table_names.txt' if it exists
if os.path.exists('sql_table_names.txt'):
    os.remove('sql_table_names.txt')
    print('text file deleted')

    

for table_name in table_names:
    schema = get_table_schema(connection, table_name)
    print(f'Table schema for {table_name}:', schema)
    bq_table_name = f'dwh_{table_name}'
    if not schema:  # Check if the schema is empty
        print(f'Skipping table {table_name} due to empty schema.')
        continue
    # Save table names to a text file
    with open('sql_table_names.txt', 'a') as file:  # Open in append mode
        file.write(table_name + '\n')
        print('text file save')
    create_table_if_not_exists(project_id, dataset_id, bq_table_name, schema)

    #clean data processing
    query = f'SELECT * FROM [AOL-ORACARG-IN].dbo.{table_name}'
    df = pd.read_sql(query, con=connection)

    # Clean and validate the data
    df = clean_data(df)

    # After cleaning and validating the data, you can save it to a CSV file
    cleaned_data_file = f'cleaned_{table_name}.csv'
    df.to_csv(cleaned_data_file, index=False)

connection.close()
