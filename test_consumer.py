import pytest
import boto3
import json

from consumer import list_objects, process_data_for_dynamoDB, sleep_for_a_bit, delete_from_dynamodb


# class Tests(unittest.TestCase):
class Tests:
    # list_objects(bucket)
    def test_list_objects_should_list_objects(self):
        s3 = boto3.resource('s3')
        the_bucket = s3.Bucket(f'usu-cs5260-cocona-requests')
        objects = list_objects(the_bucket)
        # objects should be a list of 0 or more objects
        assert len(objects) >= 0  # , "object list had negative length? O_o"

    # delete(self)
    # def test_delete_should_delete_object_from_bucket(self):
    #     s3 = boto3.resource('s3')
    #     the_bucket = s3.Bucket(f'usu-cs5260-cocona-requests')
    #     # ...to do: look for a way to upload data as a file, to a bucket
    #     # assert does_not_exist(the_file)
    #     pass

    # insert_into_dynamodb(to_write)
    def test_insert_into_dynamodb_should_insert_into_dynamodb(self):
        # insert into dynamodb
        # ...then confirm that the item is there
        pass

    # process_data_for_dynamoDB(ze_data)
    def test_data_should_be_processed(self):
        with open('test-request', 'r') as a_file:
            data = a_file.read()
        the_data = json.loads(data)
        # call process_data_for_dynamodb
        process_data_for_dynamoDB(the_data)
        # ...and check that there's no otherAttributes
        assert 'otherAttributes' not in the_data

    def test_sleeping_should_always_return_true(self):
        ret = sleep_for_a_bit(100, False)
        assert ret == True

    def test_delete_from_dynamodb_should_return_true_when_successful(self):
        with open('test-request', 'r') as a_file:
            data = a_file.read()
        the_data = json.loads(data)
        assert delete_from_dynamodb(the_data) == True

# if __name__ == '__main__':
#     unittest.main()
