from pathlib import Path
import time
import os
from google.cloud import bigquery
from google.cloud import storage
import csv
import gspread
from google.oauth2 import service_account

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

    # Read CSV headers to dynamically set schema
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)

    load_job_config.schema = [
        bigquery.SchemaField(name, 'STRING', mode='NULLABLE') for name in headers
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


def get_google_sheets_data(credentials_path, sheet_url):
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = gspread.authorize(credentials)
    print("passed here")
    print(sheet_url)
    print(client)
    sheet = client.open_by_url(sheet_url).sheet1
    print("passed here too")
    data = sheet.get_all_records()
    return data


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
        #initial_table_name = '_'.join(file.split()[:2])
        #table_name = 'DWH_'+ file
        table_name = 'DWH_' + os.path.splitext(file)[0]  # Extract table name without file extension
        csv_file = data_file_folder.joinpath(file)

        table_ref = table_reference(project_id, dataset_id, table_name)
        upload_csv(client, table_ref, csv_file)
        print()

    if file.endswith('.json'):
        print('Processing JSON file: {0}'.format(file))
        credentials_path = data_file_folder.joinpath(file)
        print (credentials_path)
        sheet_url = 'https://docs.google.com/spreadsheets/d/1jNoTbMhiCaGwggRp65v9pagQJfkIC9VUP7busTJ-TbI/edit#gid=1462544512'

        data = get_google_sheets_data(credentials_path, sheet_url)

        # Define the table schema dynamically based on the first row of the data
        schema = [
            bigquery.SchemaField(name, 'STRING', mode='NULLABLE') for name in data[0].keys()
        ]

        # Create BigQuery dataset and table
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        table_ref = bigquery.TableReference(dataset_ref, 'test-table-id')

        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
