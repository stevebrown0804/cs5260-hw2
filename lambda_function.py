import json
# import urllib.parse
import boto3
# import logging
# import pytest
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='botocore.client')

# Set up the AWS stuff
sqs = boto3.resource('sqs', region_name="us-east-1")
queue = sqs.get_queue_by_name(QueueName='cs5260-requests')


def lambda_handler(event, context):
    # print("-=Inside lambda_handler=-")
    # print("Received event: " + json.dumps(event, indent=2))

    #   Take a widget requests (as...JSON? TBD)
    if event["body"] is None:
        return formatError({"statusCode": 499, "code": 'UNEXPECTED_VALUE', "message": 'Empty event body'})
    # Otherwise...

    #   Transform it into...(JSON, again? *shrug*)
    # TODO transform? tbd

    #   Add it to an SQS queue
    queue.send_message(MessageBody=f'{event["body"]}')

    return formatResponse("Success?")


def formatResponse(body):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "isBase64Encoded": False,
        "multiValueHeaders": {
            "X-Custom-Header": ["value 1", "value 2"]
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


# def serialize(obj):
#     return json.dumps(obj)


# Note to self: Don't copy this into the AWS Lambda interface
if __name__ == '__main__':
    resp = lambda_handler({"body": {"firstName": "jim"}})
    # resp = lambda_handler({"body": None})
    str1 = str(resp["statusCode"]) + ": " + str(resp["body"])
    print("\nResult:\n" + str(str1))
