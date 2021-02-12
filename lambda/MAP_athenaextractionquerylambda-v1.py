import boto3
import json
import os
import datetime
import time
from dateutil.relativedelta import relativedelta


def lambda_handler(event, context):
    
    s3outputbucketname = os.environ['s3outputbucketname']
    outputfolder = os.environ['outputfolder'] + '/'
    print("outputfolder: " + outputfolder)
   
    map_migrated_db = os.environ['map_migrated_db']
    map_migrated_table = os.environ['map_migrated_table']
    extraction_query_name = os.environ['extraction_query_name']
    athena_output_location = os.environ['athena_output_location']
    
    #Check if output folder exists. If it doesn't exist then create an output folder. If it exists, then proceed to empty
    create_s3outputbucketfolder(s3outputbucketname, outputfolder)
    
    # Empty the existing MAP query output bucket for linked accounts
    empty_s3outputbucket(s3outputbucketname, outputfolder)
    
    #Retrieve and run Athena Extraction query- extracts relevant MAP reports for the linked member
    run_athenaextractionquery(map_migrated_db,map_migrated_table,extraction_query_name,athena_output_location)
    return 

def create_s3outputbucketfolder(s3outputbucketname, outputfolder):

    client = boto3.client('s3')
    print("inside create_s3outputbucketfolder")
    response = client.list_objects_v2(
            Bucket=s3outputbucketname
    )
    
    print("keycount: " + str(response['KeyCount']) )
    
    if (response['KeyCount'] == 0):
        print("inside create folder")
        response = client.put_object(
            Bucket=s3outputbucketname, 
            Key=outputfolder
        )
        

def empty_s3outputbucket(s3outputbucketname, outputfolder):

    client = boto3.client('s3')
    response = client.list_objects_v2(
            Bucket=s3outputbucketname,
            Prefix=outputfolder
    )
    print("keycount in emptybucketfn: " + str(response['KeyCount']) )
    
    if (response['KeyCount'] > 0):
        s3objects = response['Contents']
        for s3object in s3objects:
            objectkey = s3object['Key']
            print("objectkey is: " + objectkey)
            response = client.delete_object(
                Bucket=s3outputbucketname,
                Key=objectkey
            )
        response_folder = client.put_object(
            Bucket=s3outputbucketname, 
            Key=outputfolder
        ) 
    
# Run MAP Extraction Query for linked accounts
def run_athenaextractionquery(map_migrated_db, map_migrated_table, extraction_query_name, athena_output_location):

    client = boto3.client('athena')
    response = client.list_named_queries()
    named_query_IDs = response['NamedQueryIds']
    
    
    for query_ID in named_query_IDs: 
        print("named_query_id: " + str(query_ID) )
        named_query = client.get_named_query(
            NamedQueryId=query_ID
        )
        query_string = named_query['NamedQuery']['QueryString']
        query_name = named_query['NamedQuery']['Name']
        print("query_name: " + query_name )
        
        if extraction_query_name in query_name:
            drop_query_string = 'DROP TABLE ' + map_migrated_db + '.temp_table'
            print("inside drop_query_string: " + drop_query_string )
            executionID_drop = client.start_query_execution(
                QueryString=drop_query_string,
                ResultConfiguration={
                    'OutputLocation': athena_output_location,
                    'EncryptionConfiguration': {
                        'EncryptionOption': 'SSE_S3',
                    }
                }
            )
            
            response_exec = client.get_query_execution(
            QueryExecutionId=executionID_drop['QueryExecutionId']
            )['QueryExecution']['Status']['State']
            while response_exec in ['QUEUED','RUNNING']:
                time.sleep(30)
                response_exec = client.get_query_execution(
                QueryExecutionId=executionID_drop['QueryExecutionId']
                )['QueryExecution']['Status']['State']
                
            
            print("completed drop_query_string: " + drop_query_string )
            
            executionID_create = client.start_query_execution(
                QueryString=query_string,
                ResultConfiguration={
                    'OutputLocation': athena_output_location,
                    'EncryptionConfiguration': {
                        'EncryptionOption': 'SSE_S3',
                    }
                }
            )
            print("completed create_query_string: " + query_string)
            
            response_exec_2 = client.get_query_execution(
            QueryExecutionId=executionID_create['QueryExecutionId']
            )['QueryExecution']['Status']['State']
            while response_exec_2 in ['QUEUED','RUNNING']:
                time.sleep(30)
                response_exec_2 = client.get_query_execution(
                QueryExecutionId=executionID_create['QueryExecutionId']
                )['QueryExecution']['Status']['State']
                
            break
