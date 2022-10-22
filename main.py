import os
import argparse  # https://docs.python.org/3/howto/argparse.html#id1
import boto3 as boto3
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.delete
# https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/s3/s3_basics/object_wrapper.py
from botocore.exceptions import ClientError
import logging  # https://docs.python.org/3/howto/logging.html#when-to-use-logging
import time
import json
import unittest  # https://docs.python.org/3/library/unittest.html

# Reminder: To update the credentials for AWS, get them from the learner lab module (under AWS details->AWS CLI)
# and paste them into C:\Users\steve\.aws\credentials

# When a Widget needs to be stored in Bucket 3, you should serialize it into a JSON string and store that string
# data. Its key should be based on the following pattern:
# widgets/{owner}/{widget id}
# where {owner} is derived from the widget’s owner and {widget id} is derived from the widget’s
# id. The {owner} part of the key should be computed from the Owner property by 1) replacing
# spaces with dashes and converting the whole string to lower case.
#
# When a widget needs to be stored in the DynamoTable, place every widget attribute in the
# request its own attribute in the DynamoDB object. In other words, in addition to the widget_id,
# owner, label, and description, all the properties listed in the otherAttributes properties need to
# be stored as attributes in the DynamoDB object and not as single map or list.

# Parse the command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--read-from", help="name of the bucket to read from, eg. usu-cs5260-cocona-requests or bucket2")
parser.add_argument("--write-to", help="target to write to, eg. usu-cs5260-cocona-web or bucket3 or dynamoDB")
args = parser.parse_args()
if args.read_from:
    read_from = args.read_from
    print("Reading from: " + read_from)
write_to = None
if args.write_to:
    write_to = args.write_to
    print("Writing to: " + write_to)

# Initialize logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, filename='logging.log', filemode='w',
                    format='%(levelname)s: %(message)s', encoding='utf-8', )


# The following functions adapted from:
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html
def list_objects(bucket):
    try:
        objects = list(bucket.objects.all())
        logger.info("Got %s objects from bucket '%s'", len(objects), bucket.name)
    except ClientError:
        logger.exception("Unable to get objects from bucket '%s'.", bucket.name)
        raise
    else:
        return objects


def delete(self):
    try:
        self.object.delete()
        self.object.wait_until_not_exists()
        logger.info(
            "Deleted object '%s' from bucket '%s'.", self.object.key, self.object.bucket_name)
    except ClientError:
        logger.exception(
            "Couldn't delete object '%s' from bucket '%s'.", self.object.key, self.object.bucket_name)
        raise


def insert_into_dynamodb(to_write, key_name):
    try:
        dynamo_table = dynamodb.Table("widgets")
        dynamo_table.put_item(Item=to_write)
        logger.info("Put item %s into dynamoDB", to_write)
    except ClientError:
        logger.exception("Couldn't insert item %s into dynamoDB", to_write)
        raise


# Create the bucket(s)
s3 = boto3.resource('s3')
client = boto3.client("s3")
dynamodb = boto3.resource('dynamodb')
bucket2 = s3.Bucket(f'usu-cs5260-cocona-requests')
if write_to == "bucket3" or "usu-cs5260-cocona-web":
    bucket3 = s3.Bucket(f'usu-cs5260-cocona-web')

# check bucket2 for the presence of a widget #
#  if it's there, read it, delete it and add it to <wherever>
done = False  # Note: Never actually set to true!
while not done:
    objs = list_objects(bucket2)
    if len(objs) > 0:
        the_object = s3.Object(bucket2, objs[0]).key.key
        logger.info('Looking at object: %s', the_object)
        # bucket2.download_file(the_object, 'the_object')
        bucket2.download_file(the_object, the_object)
        s3.Object(bucket2, objs[0]).key.delete()
        # ...and write it to the write-to target
        #   LATER: create, update and delete widgets
        #           store the widget(s) in bucket 3 or in the dynamoDB 'widgets' table
        #
        if write_to == "usu-cs5260-cocona-web":
            client.upload_file(the_object, write_to, "widgets/" + the_object)
        elif write_to == "dynamoDB":
            # print("TODO: Insert the file into dynamoDB")                                               -- IN PROGRESS
            with open(the_object, 'r') as a_file:
                data = a_file.read()
            the_data = json.loads(data)
            print("JSON data: ")
            print(the_data)
            insert_into_dynamodb(the_data, the_object)
        else:
            logger.error("Unrecognized write-to target")
            print("Unrecognized write-to target")
            quit()
        # ...then delete the local copy
        if os.path.exists(the_object):
            os.remove(the_object)
            logger.info("The file: %s has been deleted", the_object)
        else:
            logger.info("The file: %s does not exist", the_object)
    else:
        # if it's not, wait 100ms and try again
        time.sleep(0.1)

# Logging examples:
# logger.debug('This message should go to the log file')
# logger.info('So should this')
# logger.warning('And this, too')
# logger.error('And non-ASCII stuff, too, like Øresund and Malmö')
