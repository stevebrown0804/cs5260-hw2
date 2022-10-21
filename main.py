import argparse
import boto3 as boto3
from botocore.exceptions import ClientError
import logging
import json
import unittest

# Reminder: To update the credentials for AWS, get them from the learner lab module (under AWS details->AWS CLI)
# and paste them into C:\Users\steve\.aws\credentials

# ARGPARSE
# TODO: storage method, resources
parser = argparse.ArgumentParser()
# add arguments
parser.add_argument("--read-from", help="name of the bucket to read from, eg. usu-cs5260-cocona-requests")
parser.add_argument("--write-to", help="target to write to, eg. bucket3 or widgets_table")
args = parser.parse_args()
# respond to arguments
if args.read_from:
    read_from = args.read_from
    print("Reading from :" + read_from)
if args.write_to:
    write_to = args.write_to
    print("Writing to: " + write_to)

# Initialize logging
logger = logging.getLogger(__name__)
# logging.basicConfig(filename='example.log', filemode='w', encoding='utf-8', level=logging.DEBUG)
logging.basicConfig(level=logging.INFO, filename='example.log', filemode='w',
                    format='%(levelname)s: %(message)s', encoding='utf-8', )


# S3
# list the s3 buckets (just to do something with boto3, for now) ####
# s3 = boto3.resource('s3')
# for bucket in s3.buckets.all():
#     print(bucket.name)
# TODO: read bucket 2, write to bucket 3, etc.
# s3 = boto3.client('s3')
# s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')

# The following function came from:
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html
def list_objects(bucket, prefix=None):
    """
    Lists the objects in a bucket, optionally filtered by a prefix.

    :param bucket: The bucket to query. This is a Boto3 Bucket resource.
    :param prefix: When specified, only objects that start with this prefix are listed.
    :return: The list of objects.
    """
    try:
        if not prefix:
            objects = list(bucket.objects.all())
        else:
            objects = list(bucket.objects.filter(Prefix=prefix))
        logger.info("Got objects %s from bucket '%s'",
                    [o.key for o in objects], bucket.name)
    except ClientError:
        logger.exception("Couldn't get objects for bucket '%s'.", bucket.name)
        raise
    else:
        return objects


# Create the bucket(s)
s3_resource = boto3.resource('s3')
bucket2 = s3_resource.Bucket(f'usu-cs5260-cocona-requests')

# WIDGETS #
# create, update and delete widgets
# store the widget(s) in bucket 3 or in the dynamoDB 'widgets' table

# check bucket2 for the presence of a widget ####
#  if it's there, read it, delete it and add it to <wherever>

# if it's not, wait 100ms and try again


# Doin' some logging
logger.debug('This message should go to the log file')
logger.info('So should this')
logger.warning('And this, too')
logger.error('And non-ASCII stuff, too, like Øresund and Malmö')
