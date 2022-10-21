import argparse
import boto3 as boto3
from botocore.exceptions import ClientError
import logging
import time
import json
import unittest

# Reminder: To update the credentials for AWS, get them from the learner lab module (under AWS details->AWS CLI)
# and paste them into C:\Users\steve\.aws\credentials

# Parse the command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--read-from", help="name of the bucket to read from, eg. usu-cs5260-cocona-requests or bucket2")
parser.add_argument("--write-to", help="target to write to, eg. usu-cs5260-cocona-web or bucket3 or widgets_table")
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


# Create the bucket(s)
s3 = boto3.resource('s3')
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
        # ...and write it to the write-to target TODO
        #   create, update and delete widgets
        #   store the widget(s) in bucket 3 or in the dynamoDB 'widgets' table
        #
    else:
        # if it's not, wait 100ms and try again
        time.sleep(0.1)

# Logging examples:
# logger.debug('This message should go to the log file')
# logger.info('So should this')
# logger.warning('And this, too')
# logger.error('And non-ASCII stuff, too, like Øresund and Malmö')
