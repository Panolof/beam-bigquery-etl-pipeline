### Input arguments - file location of key
from google.cloud import bigquery
import apache_beam as beam
from google.oauth2 import service_account
from datetime import datetime
import datetime as dtTime
import os 
import compress_json
import ast
import pandas as pd
import pytz
from google.cloud import bigquery
import argparse, sys, os
from argparse import ArgumentParser
#######################################################
### Taken from https://stackoverflow.com/questions/11540854/file-as-command-line-argument-for-argparse-error-message-if-argument-is-not-va
def extant_file(x):
    """
    'Type' for argparse - checks that file exists but does not open.
    """
    if not os.path.exists(x):
        # Argparse uses the ArgumentTypeError to give a rejection message like:
        # error: argument input: x does not exist
        raise argparse.ArgumentTypeError("{0} does not exist".format(x))
    return x

parser = ArgumentParser(description="Input File Location for Google API")
parser.add_argument("-i", "--input",dest="filename", required=True,type=extant_file, help="input json-file key for the Google API", metavar="FILE")
args = parser.parse_args()

#######################################################
projectName='virgin-media-pano-test'
datasetName='CLOUD_SAMPLES_DATA'
tableName='TRANSACTIONS'
tableIdString=projectName+'.'+datasetName+'.'+tableName
key_path=str(args.filename)


credentials = service_account.Credentials.from_service_account_file(
    key_path, 
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

def getInputDataFrame(clientInput,table_id):
    query_job=clientInput.query("""select * from {}""".format(table_id))
    results = query_job.result()  # Waits for job to complete.
    dfInput = results.to_dataframe(create_bqstorage_client=True)
    # print(dataframe)
    return dfInput

#1.1 - Find all transactions have a `transaction_amount` greater than `20`
#1.2 - Exclude all transactions made before the year `2010`
#1.3 - Sum the total by `date`
def transactionGreaterTwenty2(df):
    try:
        dfOutput=df[df.transaction_amount>20]
    except Exception as esc:
        raise Exception('Error in applying transaction filter for amounts greater than 20.\nThe error raised:\n'+str(esc)) 
    return dfOutput

def transactionDateFilter2(df):
    try:
        dateFilter=datetime(2010, 1, 1)
        df.timestamp=df.timestamp.dt.tz_localize(None)
        dfOutput=df[df.timestamp>dateFilter]
    except Exception as esc:
        raise Exception('Error in applying transaction date filter.\nThe error raised:\n'+str(esc))
    return dfOutput

def transactionAggregation2(df):
    try:
        df['date']=df.timestamp.dt.date
        df=df.drop(columns=['origin','destination','timestamp'])
        dfOutput=df.groupby(['date']).sum()
    except Exception as esc:
        raise Exception('Error in transaction aggregation.\nThe error raised:\n'+str(esc))
    return dfOutput
def fileSave2(dfFinal):
    try:
        cwd = os.getcwd()
        # Save to output file directory
        outputFileDir=os.path.join(cwd,'output')
        fileDirExist = os.path.exists(outputFileDir)    
    except Exception as esc:
        raise Exception('Error checking output file directory.\n Error raised:\n'+str(esc))
    try:
        if(fileDirExist):
            print('File directory exists.')
            print('Saving to file directory.')
            outputFileSaveLoc=outputFileDir+r'\results.json.gz'
            dfFinal2=dfFinal.reset_index()
            out = dfFinal2.to_json(orient='records')[1:-1]
            output=ast.literal_eval(out)
            compress_json.dump(output, outputFileSaveLoc) # for a gzip file
        else:
            print('File directory does not exist, creating file directory.')
            os.makedirs(outputFileDir)
            print('Saving to file directory.')
            outputFileSaveLoc=outputFileDir+r'\results.json.gz'
            dfFinal2=dfFinal.reset_index()
            out = dfFinal2.to_json(orient='records')[1:-1]
            output=ast.literal_eval(out)
            compress_json.dump(output, outputFileSaveLoc) # for a gzip file
    except Exception as esc:
        raise Exception("Error saving file to output file directory.\nError raised:\n"+str(esc))



dfInput=getInputDataFrame(client,tableIdString)
df1=transactionGreaterTwenty2(dfInput)
df2=transactionDateFilter2(df1)
dfFinal=transactionAggregation2(df2)
dfFinal=dfFinal.rename(columns={'transaction_amount':'total_amount'}).reset_index(drop=True)
fileSave2(dfFinal)
