import re
import time
import os
import csv
import subprocess
from google.cloud import bigquery
from google.cloud import storage
from datetime import date
import datetime

# Create a BigQuery client
bigquery_client = bigquery.Client()

with open('sql_table_names_nif.txt', 'r') as file:
    table_names = file.readlines()
    for table_name in table_names:
        # Remove the newline character at the end of each line
        table_name = table_name.strip()

        # Perform operations with the table name
        print("Processing table:", table_name)

        # Clear previous data from the target table
        table_ref = bigquery_client.dataset('tst-peinnovabi-data-storage.ds_nif_raw_level').table(table_name)  
        delete_job_config = bigquery.QueryJobConfig()
        delete_job_config.use_legacy_sql = False
        delete_query = f'DELETE FROM `{table_ref}`'
        delete_job = bigquery_client.query(delete_query, job_config=delete_job_config)
        delete_job.result()  # Wait for the delete job to complete
        print("Deleted table data")