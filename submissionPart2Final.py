from google.cloud import bigquery
import apache_beam as beam
from google.oauth2 import service_account
from datetime import datetime
import datetime as dtTime
import os 
import ast
import pandas as pd
import pytz
import unittest2
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import argparse
from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import calendar
import argparse, sys, os
from argparse import ArgumentParser
################################################

projectName='virgin-media-pano-test'
datasetName='CLOUD_SAMPLES_DATA'
tableName='TRANSACTIONS'
tableIdString=projectName+'.'+datasetName+'.'+tableName
cwd = os.getcwd()
# Save to output file directory
outputFileDir=os.path.join(cwd,'output')
outputFileSaveLoc=outputFileDir+r'\results'
############################################################ Table location in big query
table_spec = bigquery.TableReference(
    projectId=projectName,
    datasetId=datasetName,
    tableId=tableName)
############################################################
parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg', help='description')
args, beam_args = parser.parse_known_args()
############################################################ taken from google documentation 
# Create and set your PipelineOptions.
# For Cloud execution, specify DataflowRunner and set the Cloud Platform
# project, job name, temporary files location, and region.
# For more information about regions, check:
# https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
beam_options = PipelineOptions(
    beam_args,
    runner='DirectRunner',
    project=projectName,
    job_name='unique-job-name',
    temp_location='gs://virgin-media-test',
    region='US')
#################################################################################
def transactionGreaterTwenty(elem):
    return elem['transaction_amount']>20
def transactionDateFilter(elem,timestamp=beam.DoFn.TimestampParam):
    dateFilter=datetime(2010, 1, 1)
    utc = pytz.timezone('UTC')
    dateFilter = dateFilter.astimezone(utc)    
    return (elem['timestamp']).replace(tzinfo=pytz.UTC)>dateFilter
def getDate(elem):
    timestamp1 = calendar.timegm(elem['timestamp'].timetuple())
    dateValue=datetime.date(datetime.utcfromtimestamp(timestamp1))
    elem['Date']=dateValue.strftime("%Y-%m-%d")
    elem.pop('timestamp', None)
    elem.pop('origin',None)
    elem.pop('destination',None)
    return (elem['Date'],elem['transaction_amount'])
def tupleToDict(elem):
    dictReturn={}
    dictReturn[elem[0]]=elem[1]
    return dictReturn
#################################################################################
class transactionTransformationAndSave(beam.PTransform):
    def expand(self, dfInput):
        dfFinal =(
             dfInput
                    | 'Transactions greater than 20' >> beam.Filter(transactionGreaterTwenty)
                    | 'Transactions after 2010-01-01' >> beam.Filter(transactionDateFilter)
                    | 'Get Date of each transaction' >> beam.Map(getDate)
                    | 'Aggregate by date and obtain sum of transactions' >> beam.CombinePerKey(sum)    
                    | 'Transform Tuple to Dictionary' >> beam.Map(tupleToDict)
        )
        return dfFinal
#################################################################################

# Create the Pipeline with the specified options.
with beam.Pipeline(options=beam_options) as pipe:
    inputData = (
    pipe
    | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec)
    )
    transformation =(
        inputData
        | 'Transforming and saving data' >> transactionTransformationAndSave()
    )
    outputFileSave =(
        transformation
        | 'Write to files' >> beam.io.WriteToText(outputFileSaveLoc,file_name_suffix='.json.gz',shard_name_template='')        
        | 'Reading Input data' >> beam.Map(print)    
    )


### Unit Test function
def test_count():
    # Our static input data, which will make up the initial PCollection.
    inputTest=[{'timestamp': dtTime.datetime(2009, 1, 9, 2, 54, 25, tzinfo=dtTime.timezone.utc), 'origin': 'wallet00000e719adfeaa64b5a', 'destination': 'wallet00001866cb7e0f09a890', 'transaction_amount': 1021101.990000000}
    ,{'timestamp': dtTime.datetime(2017, 1, 1, 4, 22, 23, tzinfo=dtTime.timezone.utc), 'origin': 'wallet00000e719adfeaa64b5a', 'destination': 'wallet00001e494c12b3083634', 'transaction_amount': 19.950000000}
    ,{'timestamp': dtTime.datetime(2017, 3, 18, 14, 9, 16, tzinfo=dtTime.timezone.utc), 'origin': 'wallet00001866cb7e0f09a890', 'destination': 'wallet00001e494c12b3083634', 'transaction_amount': 2102.220000000}
    ,{'timestamp': dtTime.datetime(2017, 3, 18, 14, 10, 44, tzinfo=dtTime.timezone.utc), 'origin': 'wallet00001866cb7e0f09a890', 'destination': 'wallet00000e719adfeaa64b5a', 'transaction_amount': 1.000300000}
    ,{'timestamp': dtTime.datetime(2017, 8, 31, 17, 0, 9, tzinfo=dtTime.timezone.utc), 'origin': 'wallet00001e494c12b3083634', 'destination': 'wallet00005f83196ec58e4ffe', 'transaction_amount': 13700000023.080000000}
    ,{'timestamp': dtTime.datetime(2018, 2, 27, 16, 4, 11, tzinfo=dtTime.timezone.utc), 'origin': 'wallet00005f83196ec58e4ffe', 'destination': 'wallet00001866cb7e0f09a890', 'transaction_amount': 129.120000000}]
    # Create a test pipeline.
    with TestPipeline() as p:

      # Create an input PCollection.
      inputData = (p | beam.Create(inputTest))

      # Apply the Count transform under test.
      transformation =(
          inputData
          | 'Transforming and saving data' >> transactionTransformationAndSave()
      )
#     outputFileSave =(
#         transformation
#         | 'Reading Input data' >> beam.Map(print)    
#     )    

      # Assert on the results.
      assert_that(
        transformation,
        equal_to([
                {'2017-03-18': 2102.22},
                {'2017-08-31': 13700000023.08},
                {'2018-02-27': 129.12}]))

              # The pipeline will run and verify the results.

