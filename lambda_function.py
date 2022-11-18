import json
# import urllib.parse
import boto3
# import logging
# import pytest
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='botocore.client')

# print('Loading function')

# s3 = boto3.client('s3')
sqs = boto3.resource('sqs', region_name="us-east-1")
queue = sqs.get_queue_by_name(QueueName='cs5260-requests')


def lambda_handler(event):  # , context):
    print("-=Inside lambda_handler=-")
    print("Received event: " + json.dumps(event, indent=2))

    # # Get the object from the event and show its content type
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # try:
    #     response = s3.get_object(Bucket=bucket, Key=key)
    #     print("CONTENT TYPE: " + response['ContentType'])
    #     return response['ContentType']
    # except Exception as e:
    #     print(e)
    #     print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as '
    #           'this function.'.format(key, bucket))
    #     raise e

    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }

    # In particular...
    #   Take a widget requests (as...JSON? TBD)
    if event["body"] is None:
        return formatError({"statusCode": 499, "code": 'UNEXPECTED_VALUE', "message": 'Empty event body'})
    # Otherwise...

    #   Transform it into...(JSON, again? *shrug*)
    # TODO transform? tbd

    #   Add it to an SQS queue
    queue.send_message(MessageBody=f'{event["body"]}')

    return formatResponse("Success?")
    # print("About to return: " + str(event["body"]["firstName"]))
    # return formatResponse(str(event["body"]["firstName"]))


# The following is node.js, I believe
# exports.handler = async function(event, context){
# (logging)
# if (event.body !== 'test'){
#     return formatError({
#         status_code: 499,
#         code: 'UNEXPECTED_VALUE',
#         message: 'Unexpected event body'
#     });
# }
# (more logging)
# return formatResponse("Success");

def formatResponse(body):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "isBase64Encoded": False,
        "multiValueHeaders": {
            "X-Custom-Header": ["my value", "my other value"]
        },
        "body": body
    }


def formatError(error):
    return {
        "statusCode": error["statusCode"],
        "headers": {
            "Content-Type": "text/plain",
            "x-amzn-errorType": error["code"]
        },
        "isBase64Encoded": False,
        "body": error["code"] + ": " + error["message"]
    }


# var serialize = function(object){
#   return JSON.stringify(object, null, 2);
# }
def serialize(obj):
    return json.dumps(obj)


if __name__ == '__main__':
    resp = lambda_handler({"body": {"firstName": "jim"}})
    # resp = lambda_handler({"body": None})
    str1 = str(resp["statusCode"]) + ": " + str(resp["body"])
    print("\nResult:\n" + str(str1))
