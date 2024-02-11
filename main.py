# Authentication: create a service account, generate a key in JSON format 
# Next save its contents (everything except opening `{` and closing `}`) as google_key secret.
# how-to: https://medium.com/@yu51a5/bigquery-programmatic-access-hello-stack-overflow-data-in-bigquery-world-query-5d0806e146a9

# packages: run
# pip install apache-beam[gcp]
# pip install google-cloud-bigquery

import os
import tempfile
import pandas
import time

from google.cloud import bigquery
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow

import apache_beam as beam

from bbq_basic import run_a_simple_pipeline
from bbq_orders import run_an_order_pipeline
from bbq_posts import run_a_posts_pipeline
from bbq_posts_basic import run_a_posts_basic_pipeline

########################################################################################

assert 'google_sa_key' in os.environ, "Please define a secret called google_sa_key; its value must be the contents of access key in JSON format"
credentials_dict = eval("{" + os.environ['google_sa_key'] +"}")
credentials = service_account.Credentials.from_service_account_info(credentials_dict)

# Beam pipeline
tf = tempfile.NamedTemporaryFile()
with open(tf.name, 'w') as f:
  f.write("{" + os.environ['google_sa_key'] +"}")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = tf.name

########################################################################################

temp_bucket = os.environ.get('google_temp_bucket', None)

wtbq_dict = {"method":"STREAMING_INSERTS"} if temp_bucket is None else {"custom_gcs_temp_location" : temp_bucket}
wtbq_dict["write_disposition"] = beam.io.BigQueryDisposition.WRITE_APPEND if temp_bucket is None else beam.io.BigQueryDisposition.WRITE_TRUNCATE
wtbq_dict["create_disposition"] = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
wtbq_dict["ignore_unknown_columns"] = True

########################################################################################

run_a_posts_basic_pipeline(project_id=credentials.project_id, wtbq_dict=wtbq_dict)

########################################################################################

run_a_posts_pipeline(project_id=credentials.project_id, filename='Posts.xml', wtbq_dict=wtbq_dict)

########################################################################################

run_a_simple_pipeline(project_id=credentials.project_id, wtbq_dict=wtbq_dict)

########################################################################################
run_an_order_pipeline(project_id=credentials.project_id, wtbq_dict=wtbq_dict)

########################################################################################

print("all done!")
