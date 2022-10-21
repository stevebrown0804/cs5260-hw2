import argparse
import boto3 as boto3
import logging
import json
import unittest



#### cmd-line arguments ####
print("Argparse version: " + argparse.__version__)
parser = argparse.ArgumentParser()
# parser.add_argument("echo", help="echo the string you use here")
args = parser.parse_args()
# print(args.echo)

#### list the s3 buckets (just to do something with boto3, for now) ####
s3 = boto3.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)

#### WIDGETS #####
# create, update and delete widgets
# store the widget(s) in bucket 3 or in the dynamoDB table

# check bucket2 for the presence of a widget ####
#  if it's there, read it, delete it and add it to <wherever>

# if it's not, wait 100ms and try again




#### log the transaction
logging.basicConfig(filename='example.log', filemode='w', encoding='utf-8', level=logging.DEBUG)
logging.debug('This message should go to the log file')
logging.info('So should this')
logging.warning('And this, too')
logging.error('And non-ASCII stuff, too, like Øresund and Malmö')
