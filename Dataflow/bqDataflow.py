from __future__ import absolute_import
import argparse
import logging
import re
import os
import csv
import apache_beam as beam
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.transforms.util import BatchElements
from googleapiclient import discovery
from google.cloud.dlp import DlpServiceClient
import collections
import json
import time
import google.cloud.dlp
from textblob import TextBlob


def get_nlp_output(text):
    tweet = {}
    tweet['text'] = text
    text = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(#[A-Za-z0-9]+)", " ", str(text)).split()).lower()
    # text = text.apply(lambda tweets: ' '.join([word for word in tweets.split() if word not in stopwords]))
    tweet['sentiment'] = ' '
    tweet['polarity'] = None
    blob = TextBlob(text)
    tweet['polarity'] = blob.sentiment.polarity
    if blob.sentiment.polarity > 0 :
        tweet['sentiment'] = 'positive'
    elif blob.sentiment.polarity < 0 :
        tweet['sentiment'] = 'negative'
    else :
        tweet['sentiment'] = 'neutral'

    return tweet
    # get_nlp_output_response = collections.defaultdict(list)
    # get_nlp_output_response['text'] = output
    # from oauth2client.client import GoogleCredentials
    # from googleapiclient import discovery
    # credentials = GoogleCredentials.get_application_default()
    # nlp_service = discovery.build('language', 'v1beta2', credentials=credentials)
    #
    # # [START NLP analyzeSentiment]
    # get_operation_sentiment = nlp_service.documents().analyzeSentiment(
    #     body={
    #         'document': {
    #             'type': 'PLAIN_TEXT',
    #             'content': get_nlp_output_response['text']
    #         }
    #     })
    # response_sentiment = get_operation_sentiment.execute()
    #
    # get_nlp_output_response['sentimentscore'] = response_sentiment['documentSentiment']['score']
    # get_nlp_output_response['magnitude'] = response_sentiment['documentSentiment']['magnitude']

    # for element in response_sentiment['sentences']:
    #     get_nlp_output_response['sentences'].append({
    #         'sentence': element['text']['content'],
    #         'sentiment': element['sentiment']['score'],
    #         'magnitude': element['sentiment']['magnitude']
    #     })
    # [END NLP analyzeSentiment]

    # [START NLP analyzeEntitySentiment]
    # get_operation_entity = nlp_service.documents().analyzeEntitySentiment(
    #     body={
    #         'document': {
    #             'type': 'PLAIN_TEXT',
    #             'content': get_nlp_output_response['text']
    #         }
    #     })
    # response_entity = get_operation_entity.execute()
    #
    # for element in response_entity['entities']:
    #     get_nlp_output_response['entities'].append({
    #         'name': element['name'],
    #         'type': element['type'],
    #         'sentiment': element['sentiment']['score']
    #     })
    # time.sleep(1)
    # # [END NLP analyzeEntitySentiment]
    # return get_nlp_output_response

PROJECT = "covid-tweet-analysis"
BUCKET = 'covid-daywise-tweets'
DATASET = 'covidtweets'

def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input BigQuery table to read.',
        default='covid-tweet-analysis:covidtweets.tweets_table')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='covidtweets.covid_tweet_sentiments')
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args = [
    '--project={0}'.format(PROJECT),
    '--save_main_session',
    '--staging_location=gs://{0}/staging/'.format(BUCKET),
    '--temp_location=gs://{0}/temp/'.format(BUCKET),
    '--machine_type=n1-highcpu-16',
    '--runner=DataflowRunner',
    '--region=us-central1',
    '--requirements_file=requirements.txt'.format(BUCKET),
    '--template_location=gs://{0}/dataflow_template/dataflow_template'.format(BUCKET),
    '--service_account_email=covid-tweet-analysis@appspot.gserviceaccount.com'
    ]

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.

    pipeline_options = PipelineOptions(pipeline_args)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)


    (
    p | 'QueryTable' >> beam.io.Read(beam.io.BigQuerySource(
        query='SELECT text FROM '\
        '[covid-tweet-analysis:covidtweets.tweets_table] WHERE lang="en"'))
     | beam.Map(lambda elem: elem['text'])
     | 'Sentiment analysis' >>
     beam.Map(lambda s: get_nlp_output(s))
     | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType

             schema='text:STRING, sentiment:STRING, polarity:FLOAT64',

             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
