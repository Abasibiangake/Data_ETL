from pathlib import Path
import time
import os
from google.cloud import bigquery
from google.cloud import storage

def table_reference(project_id, dataset_id, table_name):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, table_name)
    return table_ref

def delete_dataset_tables(project_id, dataset_id):
    client = bigquery.Client(project=project_id)
    tables = client.list_tables(f'{project_id}.{dataset_id}')
    for table in tables: 
        client.delete_table(table)
    print('Tables deleted')

def upload_csv(client, table_ref, csv_file):
    client.delete_table(table_ref, not_found_ok=True)
    load_job_config = bigquery.LoadJobConfig()

    load_job_config.schema = [
        bigquery.SchemaField('Name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Age', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('City', 'STRING', mode='NULLABLE'),
    ]
    load_job_config.source_format = bigquery.SourceFormat.CSV
    load_job_config.skip_leading_rows = 1
    load_job_config.allow_quoted_newlines = True

    # API request
    with open(csv_file, "rb") as source_file:
        upload_job = client.load_table_from_file(
            source_file,
            destination=table_ref,
            location="US",
            job_config=load_job_config,
        )

    while upload_job.state != 'DONE':
        time.sleep(2)
        upload_job.reload()
        print(upload_job.state)

    print(upload_job.result())

project_id = 'tst-peinnovabi-data-storage'
dataset_id = 'ds_dwh_raw'

client = bigquery.Client(project=project_id)
data_file_folder = Path('./data_folder')

print("here")
print(os.listdir(data_file_folder))

for file in os.listdir(data_file_folder):
    print(file)

    if file.endswith('.csv'):
        print('Processing file: {0}'.format(file))
        table_name = '_'.join(file.split()[:2])
        csv_file = data_file_folder.joinpath(file)
        
        table_ref = table_reference(project_id, dataset_id, table_name)
        upload_csv(client, table_ref, csv_file)
        print()

# Define the table schema
schema = [
    bigquery.SchemaField('Name', 'STRING'),
    bigquery.SchemaField('Age', 'INTEGER'),
    bigquery.SchemaField('City', 'STRING'),
]

# Call the function to delete the dataset tables
#delete_dataset_tables(project_id, dataset_id)
