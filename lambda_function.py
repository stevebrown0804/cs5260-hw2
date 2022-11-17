import json
import urllib.parse
import boto3
import logging
import pytest

print('Loading function')


# s3 = boto3.client('s3')


def lambda_handler(event):  # , context):
    # print("Inside lambda_handler")
    # print("Received event: " + json.dumps(event, indent=2))
    #
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
    print("Inside lambda_handler")
    print("Received event: " + json.dumps(event, indent=2))
    return formatResponse("Success")


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
            "Content-Type": "appplication/json"
        },
        "isBase64Encoded": False,
        "multiValueHeaders": {
            "X-Custom-Header": ["my value", "my other value"]
        },
        "body": body
    }

# var formatError = function(error){
#     return {
#         "statusCode": error.statusCode,
#         "headers": {
#               "Content-Type": "text/plain",
#               "x-amzn-errorType": error.code
#         },
#         "isBase64Encoded": false,
#         "body": error.code + ": " + error.message,
#     };
# }

# var serialize = function(object){
#   return JSON.stringify(object, null, 2);
# }
