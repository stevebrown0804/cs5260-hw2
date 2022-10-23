import pytest
# import unittest  # https://docs.python.org/3/library/unittest.html
import boto3

from main import list_objects


# class Tests(unittest.TestCase):
class Tests:
    # list_objects(bucket)
    def test_list_objects_should_list_objects(self):
        s3 = boto3.resource('s3')
        the_bucket = s3.Bucket(f'usu-cs5260-cocona-requests')
        objects = list_objects(the_bucket)
        # objects = []
        # objects should be a list of 0 or more objects
        # self.assertGreaterEqual(len(objects), 0, msg="asserting objects.len >= 0, I think")
        assert len(objects) >= 0  # , "object list had negative length? O_o"

    # delete(self)
    def test_delete_should_delete_object_from_bucket(self):
        s3 = boto3.resource('s3')
        the_bucket = s3.Bucket(f'usu-cs5260-cocona-requests')
        # ...to do: look for a way to upload data as a file, to a bucket
        # assert does_not_exist(the_file)

    # insert_into_dynamodb(to_write)
    def test_insert_into_dynamodb_should_insert_into_dynamodb(self):
        pass

    # process_data_for_dynamoDB(ze_data)
    def test_data_should_be_processed(self):
        pass

# if __name__ == '__main__':
#     unittest.main()
