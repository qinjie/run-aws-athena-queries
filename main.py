from dateutil.relativedelta import relativedelta
from datetime import date, datetime
import os
import boto3
import botocore
from retrying import retry

REGION = 'us-east-1'

client = boto3.client("sts")
account_id = client.get_caller_identity()["Account"]

# configuration
# S3 Bucket name
s3_bucket = f'aws-athena-query-results-{account_id}-us-east-1'
s3_ouput = f's3://{s3_bucket}/'   # S3 Bucket to store results
database = 'customer_cur_data'  # The database to which the query belongs

# init clients
athena = boto3.client('athena', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)


@retry(stop_max_attempt_number=10,
       wait_exponential_multiplier=300,
       wait_exponential_max=1 * 60 * 1000)
def poll_status(_id):
    result = athena.get_query_execution(QueryExecutionId=_id)
    state = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception


def run_query(query, database, s3_output, local_filename=None):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        })

    QueryExecutionId = response['QueryExecutionId']
    result = poll_status(QueryExecutionId)

    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        print("Query SUCCEEDED: {}".format(QueryExecutionId))

        s3_key = QueryExecutionId + '.csv'
        if not local_filename:
            cur_path = os.path.dirname(os.path.realpath(__file__))
            local_filename = cur_path + "/" + QueryExecutionId + '.csv'

        # download result file
        print(local_filename)
        try:
            s3.download_file(s3_bucket, s3_key, local_filename)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
    else:
        print('Error in execution')
        print(result)


if __name__ == '__main__':

    print(f'Account ID: {account_id}')

    cur_path = os.path.dirname(os.path.realpath(__file__))
    query_file_list = os.listdir(cur_path + '/queries')
    query_file_list.sort()

    for idx, file_name in enumerate(query_file_list):
        input_file_path = f'{cur_path}/queries/{file_name}'
        output_file_path = f'{cur_path}/result/{account_id}_{datetime.now().strftime("%Y-%m-%d")}_{file_name}'

        print(f'Query {idx}: {input_file_path}')
        # try:
        # SQL Query to execute
        with open(input_file_path) as f:
            query = f.read()

        if query:
            # print("Executing query: {}".format(query))
            run_query(query, database, s3_ouput,
                      local_filename=output_file_path)
            print()
            # except Exception as ex:
            #   print(ex)
        else:
            print("Error: Empty query")
