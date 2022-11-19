import pytest
import boto3
import json

from lambda_function import formatResponse, formatError, lambda_handler


class Tests:
    def test_formatResponse_with_body_should_return_200(self):
        response = formatResponse("testValue")
        assert response["body"] == "testValue"

    def test_formatError_should_return_error_code_pass_to_it(self):
        resp = formatError({"statusCode": 499, "code": 'UNEXPECTED_VALUE', "message": 'Test message'})
        assert resp["statusCode"] == 499

    def test_lambda_handler_should_return_success_on_nonempty_body(self):
        resp = lambda_handler({"body": {"firstName": "jim"}}, {})
        assert resp["statusCode"] == 200 and resp["body"] == "Success"

    def test_lambda_handler_should_return_error_code_499_on_empty_body(self):
        resp = lambda_handler({"body": None}, {})
        assert resp["statusCode"] == 499
