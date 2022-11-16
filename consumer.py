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
    logger.info(f'Reading from: {read_from}')
else:
    print('You must specify a read_from target')
    logger.error('You must specify a read_from target')
    # sys.exit()

write_to = None
if args.write_to:
    write_to = args.write_to
    logger.info(f'Writing to: {write_to}')
else:
    print('You must specify a write_to target')
    logger.error('You must specify a write_to target')
    # sys.exit()

# Create the AWS resources/clients/etc.
s3 = boto3.resource('s3')
client = boto3.client("s3")
dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
bucket2 = s3.Bucket(f'usu-cs5260-cocona-requests')
sqs = boto3.resource('sqs', region_name="us-east-1")
queue = sqs.get_queue_by_name(QueueName='cs5260-requests')

if read_from == "bucket2" or read_from == "usu-cs5260-cocona-requests":
    read_from = "usu-cs5260-cocona-requests"
elif read_from == "cs5260-requests":
    pass
else:
    logger.error(f'Unrecognized read-from target: {read_from}')
    # sys.exit()

if write_to == "bucket3" or write_to == "usu-cs5260-cocona-web":
    write_to = "usu-cs5260-cocona-web"
    bucket3 = s3.Bucket(f'usu-cs5260-cocona-web')
elif write_to == 'dynamoDB':
    pass
else:
    print(f'Unrecognized write-to target: {write_to}')
    logger.error(f'Unrecognized write-to target: {write_to}')
    # sys.exit()


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
        logger.exception(f"Couldn't insert item {to_write} into dynamoDB")
        raise


def delete_from_dynamodb(data_to_delete):
    try:
        dynamo_table = dynamodb.Table("widgets")
        to_delete_dict = {'id': data_to_delete['widgetId']}
        dynamo_table.delete_item(Key=to_delete_dict)
        logger.info(f"Deleted item from dynamoDB: {data_to_delete}")
        return True
    except ClientError:
        print(f"Couldn't delete item {data_to_delete} from dynamoDB")
        logger.exception(f"Couldn't delete item {data_to_delete} from dynamoDB")
        #raise
        return False


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
    del ze_data['type']
    return ze_data


def sleep_for_a_bit(milliseconds, output):
    seconds = milliseconds / 1000
    time.sleep(seconds)
    if output:
        print("Sleeping...")
    return True


def run():
    try:
        # check bucket2 for the presence of a widget
        #  if it's there, read it, delete it and add it to the write-to target
        no_messages_or_files = True
        while True:
            if read_from == 'usu-cs5260-cocona-requests':
                objs = list_objects(bucket2)
                if len(objs) > 0:
                    no_messages_or_files = False
                    # A file!  Let's take a look...
                    the_object = s3.Object(bucket2, objs[0]).key.key
                    logger.info('Looking at object: %s', the_object)
                    bucket2.download_file(the_object, the_object)
                    s3.Object(bucket2, objs[0]).key.delete()

                    # ...and write it to the write-to target
                    with open(the_object, 'r') as a_file:
                        data = a_file.read()
                    the_data = json.loads(data)
            elif read_from == 'cs5260-requests':
                # check for messages
                messages = queue.receive_messages()
                if len(messages) > 0:
                    no_messages_or_files = False
                    the_message = messages[0]
                    the_data = json.loads(the_message.body)
                    the_object = the_message.message_id
                    f = open(the_object, "w")
                    f.write(json.dumps(the_data))
                    f.close()
                    # print(f'the_data: {the_data}')

            if not no_messages_or_files:
                # We'll accommodate create, update and delete requests
                if the_data["type"] == "create":
                    if write_to == "usu-cs5260-cocona-web":
                        owner = the_data["owner"].replace(" ", "-").lower()
                        widget_id = the_data["widgetId"]
                        client.upload_file(the_object, write_to, "widgets/" + owner + "/" + widget_id)
                        print(f"Object: {the_object} has been created in usu-cs5260-cocona-web")
                        logger.info(f"Object: {the_object} has been created in usu-cs5260-cocona-web")
                    elif write_to == "dynamoDB":
                        # print(f'Writing to dynamodb')
                        the_data = process_data_for_dynamoDB(the_data)
                        insert_into_dynamodb(the_data)
                        print(f"Object: {the_object} has been created in dynamoDB")
                        logger.info(f"Object: {the_object} has been created in dynamoDB")
                    else:
                        logger.error("Unrecognized write-to target")
                        print("Unrecognized write-to target")
                        sys.exit()
                elif the_data["type"] == "update":
                    # print(f'Doing an update...')
                    if write_to == "usu-cs5260-cocona-web":
                        # print(f'...to bucket3!')
                        # the same code as with creating
                        owner = the_data["owner"].replace(" ", "-").lower()
                        widget_id = the_data["widgetId"]
                        client.upload_file(the_object, write_to, "widgets/" + owner + "/" + widget_id)
                        print(f"Updated {the_object} on usu-cs5260-cocona-web")
                        logger.info(f"Updated {the_object} on usu-cs5260-cocona-web")
                    elif write_to == "dynamoDB":
                        # print(f'...to dynamoDB!')
                        # widgets_table = dynamodb.Table('widgets')
                        # pkey = {'id': the_data['widgetId']}
                        # to_update_dict = process_data_for_dynamoDB(the_data)
                        # del to_update_dict["id"]
                        # str = "SET "
                        # for x in to_update_dict:
                        #     print (f'{x}: {to_update_dict[x]}')
                        #     str += f'{x}: {to_update_dict[x]},'
                        #
                        # widgets_table.update_item(Key=pkey, AttributeUpdates=str)
                        # STOPGAP
                        the_data = process_data_for_dynamoDB(the_data)
                        insert_into_dynamodb(the_data)
                        print(f"Object: {the_object} has been updated in dynamoDB")
                        logger.info(f"Object: {the_object} has been updated in dynamoDB")
                    else:
                        logger.error("Unrecognized write-to target")
                        print("Unrecognized write-to target")
                        sys.exit()
                elif the_data["type"] == "delete":
                    if write_to == "usu-cs5260-cocona-web":
                        s3.Object("usu-cs5260-cocona-web", the_object).delete()
                        # logger.info(f'Object: {the_object} deleted from usu-cs5260-cocona-web')
                        # print(f'Object: {the_object} deleted from usu-cs5260-cocona-web')
                    elif write_to == "dynamoDB":
                        delete_from_dynamodb(the_data)
                        logger.info(f'Deleted item from dynamoDB: {the_object}')
                    else:
                        logger.error("Unrecognized write-to target")
                        print("Unrecognized write-to target")
                        sys.exit()
                else:
                    logger.warning(f'Unrecognized "type" field: {the_data["type"]}')

                # Delete the local copy
                # print(f"Deleting file: {the_object}")
                if os.path.exists(the_object):
                    os.remove(the_object)
                    logger.info("The file: %s has been deleted", the_object)
                else:
                    logger.info("The file: %s does not exist", the_object)

                if read_from == 'cs5260-requests':
                    # delete the message from the queue
                    # print(f'Deleting message: {the_message}')
                    the_message.delete()

            if no_messages_or_files:
                sleep_for_a_bit(100, False)  # True)
            else:
                no_messages_or_files = True
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
