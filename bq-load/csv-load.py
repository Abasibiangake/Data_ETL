from google.cloud import bigquery
from google.cloud import storage

#construct a BigQuery client object
client = bigquery.Client()

table_id = 'tst-peinnovabi-data-storage.ds_dwh_raw.person_2023-06-05'

#job config
 
