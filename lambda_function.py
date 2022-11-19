# import json
# import urllib.parse
import boto3
import logging
# import pytest
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='botocore.client')

# Initialize logging
# Reminder: logger.info(), .debug(), .warning() and .error()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, filename='logging_lambda_fn.log', filemode='w',
                    format='%(levelname)s: %(message)s', encoding='utf-8', )

# Set up the AWS stuff
sqs = boto3.resource('sqs', region_name="us-east-1")
queue = sqs.get_queue_by_name(QueueName='cs5260-requests')
logger.info("SQS queue created")


def lambda_handler(event, context):
    # print("-=Inside lambda_handler=-")
    # print("Received event: " + json.dumps(event, indent=2))

    #   Take a widget requests
    if event["body"] is None:
        logger.warning("Error: Empty event body")
        return formatError({"statusCode": 499, "code": 'UNEXPECTED_VALUE', "message": 'Empty event body'})
    # Otherwise...

    #   Transform it (note: not really)
    #   Add it to an SQS queue
    queue.send_message(MessageBody=f'{event["body"]}')
    logger.info("Message queued to SQS queue")

    return formatResponse("Success")


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
    resp = lambda_handler({"body": {"firstName": "jim"}}, {})
    # resp = lambda_handler({"body": None})
    str1 = str(resp["statusCode"]) + ": " + str(resp["body"])
    print("\nResult:\n" + str(str1))
