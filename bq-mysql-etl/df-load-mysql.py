import subprocess
import re
import time
import os
import csv
import pymysql
import subprocess
from google.cloud import bigquery

with open('table_names.txt', 'r') as file:
    table_names = file.readlines()
    for table_name in table_names:
        # Remove the newline character at the end of each line
        table_name = table_name.strip()
        
        # Perform operations with the table name
        print("Processing table:", table_name)
                        
        command_template = 'gcloud dataflow jobs run job_dtf_dwh_gen_sede_ab4 ' \
            '--gcs-location gs://dataflow-templates-us-east4/latest/Jdbc_to_BigQuery ' \
            '--region us-east4 --num-workers 2 --staging-location gs://dtf_temp_storage/temp/ ' \
            '--subnetwork https://www.googleapis.com/compute/v1/projects/trv-peinnovabi-net/regions/us-east4/subnetworks/sn-us-east4-tst ' \
            '--network tst-peinnovabi-lake-snet-usea4-01 --additional-experiments use_runner_v2 ' \
            '--parameters connectionURL="{connection_url}",driverClassName={driver_class_name},query="{query}",' \
            'outputTable={output_table},driverJars={driver_jars},bigQueryLoadingTemporaryDirectory={loading_temp_dir},' \
            'username={username},password={password}'

         # Replace gen_sede with the table name
        command_template = command_template.replace('gen_sede', table_name)
        
        connection_url = 'jdbc:mysql://innova-0001-wafr-istst-cluster.cluster-cyyx3ysmbggw.us-east-1.rds.amazonaws.com:3306/datamanagement'
        driver_class_name = 'com.mysql.cj.jdbc.Driver'
        query = f'SELECT * FROM datamanagement.{table_name}'  # Replace gen_sede with the table name
        print (query)
        output_table = f'tst-peinnovabi-data-storage:ds_dwh_raw3_mysql.dwh_{table_name}'  # Replace gen_sede with the table name
        driver_jars = 'gs://driver-mysql/mysql-connector-j-8.0.33.jar'
        loading_temp_dir = 'gs://prueba_dataflow2'
        username = 'usr_echuan'
        password = '#50WqMHt0#K?o2RT'

        command = command_template.format(
            connection_url=connection_url,
            driver_class_name=driver_class_name,
            query=query,
            output_table=output_table,
            driver_jars=driver_jars,
            loading_temp_dir=loading_temp_dir,
            username=username,
            password=password
        )


        subprocess.run(command, shell=True)

    print("Finished processing all tables.")
