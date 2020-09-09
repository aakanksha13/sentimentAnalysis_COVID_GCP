from google.cloud import storage
from google.cloud import bigquery
import os

def gcsTObq(event, context):
     """Triggered by a change to a Cloud Storage bucket.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     # Construct a BigQuery client object.
     client = bigquery.Client()
     bucketName = event['bucket']
     name = event['name']
     # TODO(developer): Set table_id to the ID of the table to create.
     table_id = os.environ['table_id']

     job_config = bigquery.LoadJobConfig()
     job_config.write_disposition = 'WRITE_APPEND'
     job_config.allow_quoted_newlines = True
     job_config.allow_jagged_rows=True
     job_config.schema=[
          bigquery.SchemaField("status_id", "NUMERIC"),
          bigquery.SchemaField("user_id", "NUMERIC"),
          bigquery.SchemaField("created_at", "TIMESTAMP"),
          bigquery.SchemaField("screen_name", "STRING"),
          bigquery.SchemaField("text", "STRING"),
          bigquery.SchemaField("source", "STRING"),
          bigquery.SchemaField("reply_to_status_id", "STRING"),
          bigquery.SchemaField("reply_to_user_id", "STRING"),
          bigquery.SchemaField("reply_to_screen_name", "STRING"),
          bigquery.SchemaField("is_quote", "STRING"),
          bigquery.SchemaField("is_retweet", "STRING"),
          bigquery.SchemaField("favourites_count", "NUMERIC"),
          bigquery.SchemaField("retweet_count", "NUMERIC"),
          bigquery.SchemaField("country_code", "STRING"),
          bigquery.SchemaField("place_full_name", "STRING"),
          bigquery.SchemaField("place_type", "STRING"),
          bigquery.SchemaField("followers_count", "NUMERIC"),
          bigquery.SchemaField("friends_count", "NUMERIC"),
          bigquery.SchemaField("account_lang", "STRING"),
          bigquery.SchemaField("account_created_at", "TIMESTAMP"),
          bigquery.SchemaField("verified", "STRING"),
          bigquery.SchemaField("lang", "STRING")
     ]
     job_config.skip_leading_rows = 1
     job_config.source_format = bigquery.SourceFormat.CSV

     uri = "gs://" + event['bucket'] + "/" + event['name']

     load_job = client.load_table_from_uri(
          uri,
          table_id,
          job_config=job_config
     )  # Make an API request.

     load_job.result()  # Waits for the job to complete.
