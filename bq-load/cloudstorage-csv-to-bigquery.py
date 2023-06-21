from pathlib import Path
import time
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
        client.delete_table(table.reference)
    print('Tables deleted')


def upload_csv(client, table_ref, csv_blob):
    client.delete_table(table_ref, not_found_ok=True)
    load_job_config = bigquery.LoadJobConfig()

    # Read CSV headers to dynamically set schema
    headers = csv_blob.split('\n')[0].split(',')

    load_job_config.schema = [
        bigquery.SchemaField(name, 'STRING', mode='NULLABLE') for name in headers
    ]
    load_job_config.source_format = bigquery.SourceFormat.CSV
    load_job_config.skip_leading_rows = 1
    load_job_config.allow_quoted_newlines = True

    # Create a temporary file to upload the CSV data
    temp_file_path = "./data_folder/temp_file1.csv"
    with open(temp_file_path, "w") as temp_file:
        temp_file.write(csv_blob)

    # API request
    upload_job = client.load_table_from_file(
        temp_file_path,
        table_ref,
        location="US",
        job_config=load_job_config,
    )

    while upload_job.state != 'DONE':
        time.sleep(2)
        upload_job.reload()
        print(upload_job.state)

    print(upload_job.result())
    Path(temp_file_path).unlink()


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
data_bucket_name = 'csv_to_bigquery_bucket'  # Cloud Storage bucket name
data_blob_name = 'test-2023-06-07.csv'  # Replace with the name of your CSV file in Cloud Storage

# Create a blob object referencing the CSV file in Cloud Storage
storage_client = storage.Client()
bucket = storage_client.bucket(data_bucket_name)
blob = bucket.blob(data_blob_name)

table_name = 'DWH_' + Path(data_blob_name).stem  # Extract table name without file extension
table_ref = table_reference(project_id, dataset_id, table_name)
print('Trying to Upload file: {0}'.format(table_name))
upload_csv(client, table_ref, blob.download_as_text())
print()

# Rest of the code remains the same
