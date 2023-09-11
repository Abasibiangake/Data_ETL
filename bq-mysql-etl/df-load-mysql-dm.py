import subprocess
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

with open('table_names copy.txt', 'r') as file:
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
        bucket_name = f'query-bucket-dm'
        query_filename = f"query_{table_name}.sql"

        # Create a storage client
        storage_client = storage.Client()

        # Get the bucket object
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object for the query file
        blob = bucket.blob(query_filename)

        # Upload the query string to the blob
        blob.upload_from_string(f"SELECT *, {today_str} AS LAST_DATE FROM datamanagement.{table_name}")

        # Get the GCS URI of the uploaded file
        gcs_uri = f'gs://{bucket_name}/{query_filename}'
                
        command_template = 'gcloud dataflow jobs run job_dtf_dm_new_raw_gen_sede ' \
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
        command_template = command_template.replace('gen_sede', table_name)
        
        connection_url = 'jdbc:mysql://innova-0001-wafr-istst-cluster.cluster-cyyx3ysmbggw.us-east-1.rds.amazonaws.com:3306/datamanagement'
        driver_class_name = 'com.mysql.cj.jdbc.Driver'
        query_file = gcs_uri
        print (query_file)
        output_table = f'tst-peinnovabi-data-storage:ds_dm_raw_level.dm_{table_name}'  # Replace gen_sede with the table name
        driver_jars = 'gs://driver-mysql/mysql-connector-j-8.0.33.jar'
        loading_temp_dir = 'gs://prueba_dataflow2'
        username = 'usr_echuan'
        password = '#50WqMHt0#K?o2RT'

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
        time.sleep(200)  # Wait for 200 seconds


    print("Finished processing all tables.")
