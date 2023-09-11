import os

jdbc_driver_jar = "./credentials/mssql-jdbc-12.2.0.jre11 (1).jar"

# Get the absolute path
absolute_path = os.path.abspath(jdbc_driver_jar)

# Check if the file exists
if os.path.exists(absolute_path):
    print("The file exists.")

    # Check if the file is a regular file (not a directory)
    if os.path.isfile(absolute_path):
        print("The path points to a file.")
        
        # Check the file permissions
        if os.access(absolute_path, os.R_OK):
            print("The file is readable.")
        else:
            print("The file is not readable.")
    else:
        print("The path does not point to a file.")
else:
    print("The file does not exist or the path is incorrect.")
        
        # # Split the command into a list of arguments
        # command = [
        #     'gcloud', 'dataflow', 'jobs', 'run', 'job_dtf_nif_AOL_CHATBOT_CREDENCIALES',
        #     '--gcs-location', 'gs://dataflow-templates-us-east4/latest/Jdbc_to_BigQuery',
        #     '--region', 'us-east4', '--num-workers', '2',
        #     '--staging-location', 'gs://dtf_temp_storage/temp/',
        #     '--subnetwork', 'https://www.googleapis.com/compute/v1/projects/trv-peinnovabi-net/regions/us-east4/subnetworks/sn-us-east4-tst',
        #     '--network', 'tst-peinnovabi-lake-snet-usea4-01',
        #     '--additional-experiments', 'use_runner_v2',
        #     '--parameters',
        #     f'connectionURL="{connection_url}"',
        #     f'driverClassName="{driver_class_name}"',
        #     f'query="{query}"',
        #     f'outputTable="{output_table}"',
        #     f'driverJars="{driver_jars}"',
        #     f'bigQueryLoadingTemporaryDirectory="{loading_temp_dir}"',
        #     f'username="{username}"',
        #     f'password="{password}"'
        # ]

        # # Execute the command using the subprocess module
        # subprocess.run(command)

        # command = command_template.format(
        #     connection_url=connection_url,
        #     driver_class_name=driver_class_name,
        #     query=query,
        #     output_table=output_table,
        #     driver_jars=driver_jars,
        #     loading_temp_dir=loading_temp_dir,
        #     username=username,
        #     password=password
        # )

        # subprocess.run(command, shell=True)

         # Construct the command as a list of arguments
        # command = [
        #     'gcloud', 'dataflow', 'jobs', 'run', 'job_dtf_nif_AOL_CHATBOT_CREDENCIALES',
        #     '--gcs-location', 'gs://dataflow-templates-us-east4/latest/Jdbc_to_BigQuery',
        #     '--region', 'us-east4',
        #     '--num-workers', '2',
        #     '--staging-location', 'gs://dtf_temp_storage/temp/',
        #     '--subnetwork', 'https://www.googleapis.com/compute/v1/projects/trv-peinnovabi-net/regions/us-east4/subnetworks/sn-us-east4-tst',
        #     '--network', 'tst-peinnovabi-lake-snet-usea4-01',
        #     '--additional-experiments', 'use_runner_v2',
        #     '--parameters',
        #     f'connectionURL={connection_url}',
        #     f'driverClassName={driver_class_name}',
        #     f'query={query}',
        #     f'outputTable={output_table}',
        #     f'driverJars={driver_jars}',
        #     f'bigQueryLoadingTemporaryDirectory={loading_temp_dir}',
        #     f'username={username}',
        #     f'password={password}'
        # ]

                # Escape special characters in the query
        # query = query.replace("", "")
      
        # Escape special characters in the query using list2cmdline
        #query = subprocess.list2cmdline([query])

        # import re
import time
import os
import csv
import pymysql
import subprocess
from google.cloud import bigquery
from google.cloud import storage

# Schedule the script to run every day with a 24-hour interval
while True:
    with open('sql_table_names_pmo.txt', 'r') as file:
        table_names = file.readlines()
        for table_name in table_names:
            # Remove the newline character at the end of each line
            table_name = table_name.strip()
            
            # Perform operations with the table name
            print("Processing table:", table_name)
            
            # Construct the BigQuery client
            client = bigquery.Client()

            # Build the query
            query = f"SELECT *, 20230706 AS LASTDATE FROM [PMO-ORACARGA-IN].dbo.{table_name}"
            
            # Run the query and retrieve the results
            query_job = client.query(query)
            results = query_job.result()
            
            # Create a storage client
            storage_client = storage.Client()
            
            # Specify the bucket name and file path
            bucket_name = 'your-bucket-name'
            file_name = f'results_{table_name}.csv'
            
            # Create a new bucket object
            bucket = storage_client.bucket(bucket_name)
            
            # Create a new blob object
            blob = bucket.blob(file_name)
            
            # Save the query results to the file
            with blob.open("w") as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow([field.name for field in results.schema])
                csv_writer.writerows(results)
                
            # Get the GCS URI of the saved file
            gcs_uri = f'gs://{bucket_name}/{file_name}'

            command_template = 'gcloud dataflow jobs run job_dtf_pmo_AOL_CHATBOT_CREDENCIALES ' \
                '--gcs-location gs://dataflow-templates-us-east4/latest/Jdbc_to_BigQuery ' \
                '--region us-east4 --num-workers 2 --staging-location gs://dtf_temp_storage/temp/ ' \
                '--subnetwork https://www.googleapis.com/compute/v1/projects/trv-peinnovabi-net/regions/us-east4/subnetworks/sn-us-east4-tst ' \
                '--network tst-peinnovabi-lake-snet-usea4-01 --additional-experiments use_runner_v2 ' \
                '--parameters connectionURL="{connection_url}",driverClassName={driver_class_name},' \
                'queryFile="{query_file}",' \
                'outputTable={output_table},' \
                'driverJars={driver_jars},' \
                'bigQueryLoadingTemporaryDirectory={loading_temp_dir},' \
                'username={username},' \
                'password={password}'
            
            # Replace AOL_CHATBOT_CREDENCIALES with the table name
            command_template = command_template.replace('AOL_CHATBOT_CREDENCIALES', table_name)
            
            connection_url = 'jdbc:sqlserver://172.35.30.7:1433;databaseName=PMO-ORACARGA-IN;encrypt=false'
            driver_class_name = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
            query_file = gcs_uri
            output_table = f'tst-peinnovabi-data-storage:ds_pmo_raw_level.dwh_{table_name}'  # Replace gen_sede with the table name
            driver_jars = 'gs://driver-fromIn the updated code, after executing the query and obtaining the results, the code uses the `google-cloud-storage` library to create a client, specify the bucket name and file path, and save the query results as a CSV file in the bucket. The GCS URI (`gcs_uri`) is then used as the `query_file` parameter in the `gcloud` command to specify the location of the saved file.

Make sure to replace `'your-bucket-name'` with the actual name of your Google Cloud Storage bucket.

Please note that using Google Cloud Storage to store query results and then accessing them in the subsequent `gcloud` job may introduce additional complexity and potential latency due to the data transfer and storage operations involved. You may need to consider the size of the query results and the frequency of running the script to ensure efficient and cost-effective usage of resources.
O#6lsRv0H4ZJv#fH
