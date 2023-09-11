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
        # Define the SQL query
        sql_query = f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"

        # Execute the SQL query
        cursor.execute(sql_query)
        # cursor.execute("EXEC sp_columns ?;", (table_name,))
        schema = cursor.fetchall()
    
    #if the schema is not empty. then we perform filtering operation on the schema
    #such table schema comprises of a list of arrays. In each array " 'PEA_TST', 'dbo', table_name" repeats it self  and it causes conflict. 
    #modify the schema such that it only retrieves unique fields 
    new_schema = []
    for row in schema:
        # new_row = row[3:]  # Exclude the first three elements from each tuple
        new_schema.append(row)
    print('schema', schema)
    return new_schema

    
#get table list of necessary tables from the csv file
def get_all_table_names_from_csv(file_path):
    table_names = []
    unique_names = set()

    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) >= 2 and row[2] != "" and row[2] != "Tabla":
                if row[2] not in unique_names:
                    table_names.append(row[2].strip())
                    unique_names.add(row[2].strip())

    print('this are the list of table names', table_names)
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
    
    # Add the new column "LAST_DATE" to the schema
    table_schema.append(bigquery.SchemaField("LAST_DATE", "STRING"))

    # Create the empty table with the specified schema
    table = bigquery.Table(table_ref, schema=table_schema)
    table = client.create_table(table)  # API request
    print(f"Empty table '{table.project}.{table.dataset_id}.{table.table_id}' created in BigQuery.")

# MsSQL connection details

project_id = 'tst-peinnovabi-data-storage'
dataset_id = 'ds_aol_raw_level'

#sql conn detail
# JDBC connection details
jdbc_url = "jdbc:sqlserver://172.35.30.7:1433;databaseName=AOL-ORACARG-IN;encrypt=false"
jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# username = "usr_rvillareal"
# password = "TT3v9$vRTNNUsdML"
username = "usr_echuan"
password = "mRA2f?dkwCWfmd5w"
print ('username', username)

# VPN configuration file path
vpn_config_path = './credentials/client.ovpn'

# csv file path
csv_file_path = './data_folder/Listado de Tablas - AOL.csv'

# Establish the VPN connection
establish_vpn_connection(vpn_config_path)

# Wait for the VPN connection to be established
time.sleep(10)
print('Processing tables:')

connection = connect_to_mssql_server(jdbc_driver, jdbc_url, username, password)
print('Connected:')
delete_dataset(project_id, dataset_id)
create_dataset(project_id, dataset_id)

table_names = get_all_table_names_from_csv(csv_file_path)
# Delete 'sql_table_names.txt' if it exists
if os.path.exists('sql_table_names_aol.txt'):
    os.remove('sql_table_names_aol.txt')
    print('text file deleted')

processed_table_names = set()  # Set to store processed table names

for table_name in table_names:
    if table_name in processed_table_names:
        print(f'Skipping table {table_name} due to duplication.')
        continue
    schema = get_table_schema(connection, table_name)
    print(f'Table schema for {table_name}:', schema)
    bq_table_name = f'aol_{table_name}'
    if not schema:  # Check if the schema is empty
        print(f'Skipping table {table_name} due to empty schema.')
        continue
    # Save table names to a text file
    with open('sql_table_names_aol.txt', 'a') as file:  # Open in append mode
        file.write(table_name + '\n')
        print('text file save')
    create_table_if_not_exists(project_id, dataset_id, bq_table_name, schema)
    processed_table_names.add(table_name)  # Add the processed table name to the set

connection.close()
