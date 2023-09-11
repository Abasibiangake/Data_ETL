import re
import time
import os
import csv
import pymysql
import subprocess
from google.cloud import bigquery
from google.cloud import storage
from datetime import date
import datetime

with open('sql_table_names_aol.txt', 'r') as file:
    table_names = file.readlines()
    for table_name in table_names:
        # Remove the newline character at the end of each line
        table_name = table_name.strip()
        
        # Perform operations with the table name
        print("Processing table:", table_name)
        # Get today's date
        today = datetime.date.today()

        # Convert the date to the desired format, if necessary
        today_str = today.strftime('%Y%m%d')  # Example format: '20230706'

        # Specify the bucket name and file path
        bucket_name = f'query-bucket-aol'
        query_filename = f"query_{table_name}.sql"

        # Create a storage client
        storage_client = storage.Client()

        # Get the bucket object
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object for the query file
        blob = bucket.blob(query_filename)

        # Upload the query string to the blob
        blob.upload_from_string(f"SELECT *, {today_str} AS LAST_DATE FROM [AOL-ORACARG-IN].dbo.{table_name}")

        # Get the GCS URI of the uploaded file
        gcs_uri = f'gs://{bucket_name}/{query_filename}'

        command_template = 'gcloud dataflow jobs run job_dtf_aol_AOL_CHATBOT_CREDENCIALES ' \
            '--gcs-location gs://dataflow-templates-us-east4/latest/Jdbc_to_BigQuery ' \
            '--region us-east4 --num-workers 2 --staging-location gs://dtf_temp_storage/temp/ ' \
            '--subnetwork https://www.googleapis.com/compute/v1/projects/trv-peinnovabi-net/regions/us-east4/subnetworks/sn-us-east4-tst ' \
            '--network tst-peinnovabi-lake-snet-usea4-01 --additional-experiments use_runner_v2 ' \
            '--parameters connectionURL="{connection_url}",driverClassName={driver_class_name},' \
            'query="{query_file}",' \
            'outputTable={output_table},' \
            'driverJars={driver_jars},' \
            'bigQueryLoadingTemporaryDirectory={loading_temp_dir},' \
            'username={username},' \
            'password={password}'
        
        # Replace gen_sede with the table name
        command_template = command_template.replace('AOL_CHATBOT_CREDENCIALES', table_name)
        
        connection_url = 'jdbc:sqlserver://172.35.30.7:1433;databaseName=AOL-ORACARG-IN;encrypt=false'
        driver_class_name = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        query_file = gcs_uri
        output_table = f'tst-peinnovabi-data-storage:ds_aol_raw_level.aol_{table_name}'  # Replace gen_sede with the table name
        driver_jars = 'gs://driver-from-azuresql_net/mssql-jdbc-12.2.0.jre8.jar'
        loading_temp_dir = 'gs://prueba_dataflow2'
        username = 'usr_echuan'
        password = 'mRA2f?dkwCWfmd5w'

        command = command_template.format(
            connection_url=connection_url,
            driver_class_name=driver_class_name,
            query_file=query_file,
            output_table=output_table,
            driver_jars=driver_jars,
            loading_temp_dir=loading_temp_dir,
            username=username,
            password=password
        )

        subprocess.run(command, shell=True)
        
        # Wait for a specified duration before processing the next table
        time.sleep(200)  # Wait for 8 mins in secs

    print("Finished processing all tables.")

        