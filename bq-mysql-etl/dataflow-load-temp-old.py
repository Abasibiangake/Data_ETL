import re
import time
import os
import csv
import pymysql
import subprocess
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import DataflowRunner
import apache_beam as beam


def establish_vpn_connection(vpn_config_path):
    # Example command to establish an OpenVPN connection using the provided config file
    subprocess.run(['openvpn', '--config', vpn_config_path])


def connect_to_mysql_server(host, port, user, password, database):
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    return connection


def process_table(row):
    # Process each row and return a dictionary representing BigQuery row
    # Modify this function according to your specific data transformation needs
    return {'column1': row[0], 'column2': row[1], 'column3': row[2]}


# MySQL connection details
host = 'innova-0001-wafr-istst-cluster.cluster-cyyx3ysmbggw.us-east-1.rds.amazonaws.com'
port = 3306
user = 'usr_rvillareal'
password = 'b2MSnV7p0aB$vDB8'
database = 'datamanagement'
project_id = 'tst-peinnovabi-data-storage'
dataset_id = 'ds_dwh_raw3_mysql'

# VPN configuration file path
vpn_config_path = './credentials/client.ovpn'

# Establish the VPN connection
establish_vpn_connection(vpn_config_path)

# Wait for the VPN connection to be established
time.sleep(10)
print('Processing tables:')

# Connect to MySQL server
connection = connect_to_mysql_server(host, port, user, password, database)
print('Connected:')

# Create a BigQuery client
bq_client = bigquery.Client(project=project_id)

# Define the Dataflow pipeline options
options = PipelineOptions(
    runner='DataflowRunner',
    project=project_id,
    job_name='jdbc-to-bigquery-job',
    temp_location='gs://your-bucket/temp',
    region='us-east1',
    staging_location='gs://your-bucket/staging'
)

# Define the Dataflow pipeline
with beam.Pipeline(options=options) as p:
    # Read data from JDBC source
    jdbc_data = (
        p
        | 'Read from JDBC' >> beam.io.ReadFromJdbc(
            driver_class_name='com.mysql.jdbc.Driver',
            url='jdbc:mysql://{host}:{port}/{database}'.format(
                host=host,
                port=port,
                database=database
            ),
            username=user,
            password=password,
            query='SELECT * FROM datamanagement.gen_sede'
        )
    )

    # Process each row and prepare for BigQuery write
    bq_data = jdbc_data | 'Process Data' >> beam.Map(process_table)

    # Write data to BigQuery
    bq_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        project=project_id,
        dataset=dataset_id,
        table='your_table',
        schema='column1:STRING,column2:STRING,column3:STRING',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

# Print the pipeline execution status
print('Pipeline submitted')
