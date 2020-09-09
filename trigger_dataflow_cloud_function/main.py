from googleapiclient.discovery import build
from google.cloud import storage
import time

def startDataflowProcess(data, context):
    #replace with your projectID
    project = "covid-tweet-analysis"
    job = project + str(time.time_ns())
    #path of the dataflow template on google storage bucket
    template = 'gs://covid-daywise-tweets/dataflow_template/dataflow_template'

    service = build('dataflow', 'v1b3', cache_discovery=False)
    #below API is used when we want to pass the location of the dataflow job
    request = service.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template,
        location='us-central1',
        body={
            'jobName': job,
        },
    )

    response = request.execute()
    return response
