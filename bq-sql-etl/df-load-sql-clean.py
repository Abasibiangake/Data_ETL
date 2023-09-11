import re
import time
import os
import csv
import pymysql
import subprocess
import pandas as pd
from google.cloud import bigquery

with open('sql_table_names.txt', 'r') as file:
    table_names = file.readlines()
    for table_name in table_names:
        # Remove the newline character at the end of each line
        table_name = table_name.strip()
        
        # Perform operations with the table name
        print("Processing table:", table_name)
        command_template = 'gcloud dataflow jobs run job_dtf_dwh_AOL_CHATBOT_CREDENCIALES_ab7 ' \
            '--gcs-location gs://dataflow-templates-us-east4/latest/Jdbc_to_BigQuery ' \
            '--region us-east4 --num-workers 2 --staging-location gs://dtf_temp_storage/temp/ ' \
            '--subnetwork https://www.googleapis.com/compute/v1/projects/trv-peinnovabi-net/regions/us-east4/subnetworks/sn-us-east4-tst ' \
            '--network tst-peinnovabi-lake-snet-usea4-01 --additional-experiments use_runner_v2 ' \
            '--parameters connectionURL="{connection_url}",driverClassName={driver_class_name},' \
            'inputFilePattern="{input_file_pattern}",outputTable={output_table},' \
            'driverJars={driver_jars},bigQueryLoadingTemporaryDirectory={loading_temp_dir},' \
            'username={username},password={password}'
        
        # Replace gen_sede with the table name
        command_template = command_template.replace('AOL_CHATBOT_CREDENCIALES_ab7', table_name)
        
        cleaned_data_file = f'cleaned_{table_name}.csv'
        connection_url = 'jdbc:sqlserver://172.35.30.7:1433;databaseName=PEA_TST;encrypt=false'
        driver_class_name = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        output_table = f'tst-peinnovabi-data-storage:ds_dwh_raw3_sql2.dwh_{table_name}'  # Replace gen_sede with the table name
        driver_jars = 'gs://driver-from-azuresql_net/mssql-jdbc-12.2.0.jre8.jar'
        loading_temp_dir = 'gs://prueba_dataflow2'
        username = 'usr_echuan'
        password = 'mRA2f?dkwCWfmd5w'

        command = command_template.format(
            connection_url=connection_url,
            driver_class_name=driver_class_name,
            input_file_pattern=cleaned_data_file,
            output_table=output_table,
            driver_jars=driver_jars,
            loading_temp_dir=loading_temp_dir,
            username=username,
            password=password
        )

        subprocess.run(command, shell=True)
           
        # Wait for a specified duration before processing the next table
        time.sleep(200)  # Wait for 5 seconds (adjust the duration as needed)

        