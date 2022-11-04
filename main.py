import os
import argparse  # https://docs.python.org/3/howto/argparse.html#id1
import sys

import boto3 as boto3
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.delete
# https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/s3/s3_basics/object_wrapper.py
from botocore.exceptions import ClientError
import logging  # https://docs.python.org/3/howto/logging.html#when-to-use-logging
import time
import json

# Reminder: To update the credentials for AWS, get them from the learner lab module (under AWS details->AWS CLI)
# and paste them into C:\Users\steve\.aws\credentials

# Initialize logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, filename='logging.log', filemode='w',
                    format='%(levelname)s: %(message)s', encoding='utf-8', )

# Parse the command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--read-from", help="name of the bucket to read from, eg. usu-cs5260-cocona-requests, bucket2 or "
                                        "cs5260-requests")
parser.add_argument("--write-to", help="target to write to, eg. usu-cs5260-cocona-web or bucket3 or dynamoDB")
args = parser.parse_args()

read_from = None
if args.read_from:
    read_from = args.read_from
    print(f'Reading from: {read_from}')
else:
    print('You must specify a read_from target')
    logger.error('You must specify a read_from target')
    sys.exit()

write_to = None
if args.write_to:
    write_to = args.write_to
    print(f'Writing to: {write_to}')
else:
    print('You must specify a write_to target')
    logger.error('You must specify a write_to target')
    sys.exit()

# Create the AWS resources/clients/etc.
s3 = boto3.resource('s3')
client = boto3.client("s3")
dynamodb = boto3.resource('dynamodb')
bucket2 = s3.Bucket(f'usu-cs5260-cocona-requests')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='cs5260-requests')
if read_from == "bucket2" or "usu-cs5260-cocona-requests":
    read_from = "usu-cs5260-cocona-requests"
elif read_from == "cs5260-requests":
    pass
else:
    logger.error('Unrecognized read-from target: %s', read_from)
    sys.exit()

if write_to == "bucket3" or "usu-cs5260-cocona-web":
    write_to = "usu-cs5260-cocona-web"
    bucket3 = s3.Bucket(f'usu-cs5260-cocona-web')


# The following two functions (list_objects, delete) are adapted from:
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html
#  The ones after that are adapted from those initial adaptations.
def list_objects(bucket):
    try:
        objects = list(bucket.objects.all())
        logger.info("Got %s objects from bucket '%s'", len(objects), bucket.name)
    except ClientError:
        logger.exception("Unable to get objects from bucket '%s'.", bucket.name)
        raise
    else:
        return objects


# def delete(self):
#     try:
#         self.object.delete()
#         self.object.wait_until_not_exists()
#         logger.info("Deleted object '%s' from bucket '%s'.", self.object.key, self.object.bucket_name)
#     except ClientError:
#         logger.exception("Couldn't delete object '%s' from bucket '%s'.", self.object.key, self.object.bucket_name)
#         raise


def insert_into_dynamodb(to_write):
    try:
        dynamo_table = dynamodb.Table("widgets")
        dynamo_table.put_item(Item=to_write)
        logger.info("Put item into dynamoDB: %s", to_write)
    except ClientError:
        logger.exception("Couldn't insert item %s into dynamoDB", to_write)
        raise


# "When a widget needs to be stored in the DynamoTable, place every widget attribute in the
# request its own attribute in the DynamoDB object. In other words, in addition to the widget_id,
# owner, label, and description, all the properties listed in the otherAttributes properties need to
# be stored as attributes in the DynamoDB object and not as single map or list."
def process_data_for_dynamoDB(ze_data):
    ze_data["id"] = ze_data["widgetId"]
    if ze_data['otherAttributes']:
        for attr in ze_data['otherAttributes']:
            ze_data[attr['name']] = attr['value']
        del ze_data['otherAttributes']
    return ze_data


def run():
    try:
        # check bucket2 for the presence of a widget
        #  if it's there, read it, delete it and add it to the write-to target
        while True:
            objs = list_objects(bucket2)
            if len(objs) > 0:
                # A file!  Let's take a look...
                the_object = s3.Object(bucket2, objs[0]).key.key
                logger.info('Looking at object: %s', the_object)
                bucket2.download_file(the_object, the_object)
                s3.Object(bucket2, objs[0]).key.delete()

                # ...and write it to the write-to target
                with open(the_object, 'r') as a_file:
                    data = a_file.read()
                the_data = json.loads(data)

                # We'll (eventually) accommodate create, update and delete requests
                if the_data["type"] == "create":
                    if write_to == "usu-cs5260-cocona-web":
                        owner = the_data["owner"].replace(" ", "-").lower()
                        widget_id = the_data["widgetId"]
                        client.upload_file(the_object, write_to, "widgets/" + owner + "/" + widget_id)
                    elif write_to == "dynamoDB":
                        the_data = process_data_for_dynamoDB(the_data)
                        insert_into_dynamodb(the_data)
                    else:
                        logger.error("Unrecognized write-to target")
                        print("Unrecognized write-to target")
                        quit()

                    # ...then delete the local copy
                    # print("Deleting file: %s", the_object)
                    if os.path.exists(the_object):
                        os.remove(the_object)
                        logger.info("The file: %s has been deleted", the_object)
                    else:
                        logger.info("The file: %s does not exist", the_object)
                #
                #   LATER: update and delete widgets
                #
                elif the_data["type"] == "update":
                    pass
                elif the_data["type"] == "delete":
                    pass
                else:
                    logger.warning("Unrecognized 'type' field: %s", the_data["type"])
            else:
                # if len(objs) >= 0, wait 100ms and try again
                time.sleep(0.1)
                # print("Sleeping...")
                # sys.exit()
    except KeyboardInterrupt:
        # print("You Ctrl-c'd!")
        sys.exit()


# Logging examples:
# logger.debug('This message should go to the log file')
# logger.info('So should this')
# logger.warning('And this, too')
# logger.error('And non-ASCII stuff, too, like Øresund and Malmö')

if __name__ == '__main__':
    run()
