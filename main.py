import argparse
import boto3 as boto3
from botocore.exceptions import ClientError
import logging
import json
import unittest

# Reminder: To update the credentials for AWS, get them from the learner lab module (under AWS details->AWS CLI)
# and paste them into C:\Users\steve\.aws\credentials

# Parse the command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--read-from", help="name of the bucket to read from, eg. usu-cs5260-cocona-requests")
parser.add_argument("--write-to", help="target to write to, eg. usu-cs5260-cocona-web or widgets_table")
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
logging.basicConfig(level=logging.INFO, filename='example.log', filemode='w',
                    format='%(levelname)s: %(message)s', encoding='utf-8', )


# The following function adapted from:
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


# Create the bucket(s)
s3 = boto3.resource('s3')
bucket2 = s3.Bucket(f'usu-cs5260-cocona-requests')
if write_to == "bucket3" or "usu-cs5260-cocona-web":
    bucket3 = s3.Bucket(f'usu-cs5260-cocona-web')

# WIDGETS #
# create, update and delete widgets
# store the widget(s) in bucket 3 or in the dynamoDB 'widgets' table

# check bucket2 for the presence of a widget ####
objs = list_objects(bucket2)
#  if it's there, read it, delete it and add it to <wherever>
if len(objs) > 0:
    # the_object = objs[0]
    the_object = s3.Object(bucket2, objs[0])
    logger.info('Looking at object: %s', the_object)
    bucket2.download_file(the_object, 'the_object')
    the_object.delete()
    # ...and write it to the write-to target TODO


# s3 = boto3.client('s3')
# s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')

# if it's not, wait 100ms and try again


# Doin' some logging
logger.debug('This message should go to the log file')
logger.info('So should this')
logger.warning('And this, too')
logger.error('And non-ASCII stuff, too, like Øresund and Malmö')
