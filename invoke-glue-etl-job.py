import json
import boto3

def get_latest_file_name(bucket_name):
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket_name)
    files = sorted(s3_bucket.objects.all(), key=lambda obj: obj.last_modified)
    latest_file = files[-1]
    file_name = latest_file.key
    return file_name

def lambda_handler(event, context):
    bucket = 'application-audit-logs-csv-files'
    file = get_latest_file_name(bucket)
    path = 's3://'+ bucket +'/' + file
    jobname = 'Application_Audit_Logs'
    client = boto3.client('glue')
    response = client.start_job_run(
        JobName=jobname,
        Arguments={
            '--path': path
        }
    )
    print(path)
    return response