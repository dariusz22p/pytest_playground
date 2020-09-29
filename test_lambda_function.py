import pytest
from unittest.mock import patch
import json
import os
import requests_mock
import src.lambda_function
import time
from src.exceptions import *


os.environ["API_ID"] = "test_api_key"
os.environ["GRAPHQL_ENDPOINT"] = "https://example.com"


class MockS3Client:
    """Very basic mock of the Boto3 S3 client, only mocks the functionality required for this particular lambda"""

    def get_object(self, Bucket, Key):
        # TODO: make file path relative to this file
        this_dir = os.path.dirname(os.path.realpath(__file__))
        if not Bucket == "test_bucket":
            raise Exception("The correct bucket value is not passed to get object")
        try:
            sample_file = open(os.path.join(this_dir, Key), mode="rb")
        except:
            raise Exception(f"No sample file exists for key: {Key}")
        return {"Body": sample_file}


class MockAppsyncClient:
    """Very basic mock of the Boto3 appsync client, only mocks the functionality required for this particular lambda"""

    def list_api_keys(self, apiId):
        if not apiId == "test_api_key":
            raise Exception("did not receive Appsync API key")
        return {
            "apiKeys": [
                {
                    "description": "key_1",
                    "expires": int(time.time()) + 12000,
                    "id": "wrong key",
                },
                {
                    "description": "key_2",
                    "expires": int(time.time()) + 24000,
                    "id": "correct key with longest validity",
                },
            ],
            "nextToken": "string",
        }


def mock_boto3_client(service):
    """basic mock of boto3.client function returns one of the mocked clients above"""
    if service == "s3":
        return MockS3Client()
    if service == "appsync":
        return MockAppsyncClient()
    raise Exception(f"The service {service} is not mocked")


def mock_bad_boto3(service):
    raise Exception


@patch("boto3.client", mock_boto3_client)
def test_happy_path():
    with requests_mock.Mocker() as rmock:
        # setting up a mock response for requests to the GRAPHQL_ENDPOINT
        rmock.post(
            os.getenv("GRAPHQL_ENDPOINT"),
            json={"data": {"addContributorFromSampleFile": True}},
            status_code=200,
        )

        # a test message the lambda receives
        message = {
            "location": "s3://test_bucket/sample-file.json",
            "survey": "test_survey",
            "period": "test_period",
        }
        # using assert to pass above message to Lambda in src folder
        # checking that the lambda runs without errors, and returns the message it recieved as an input
        assert src.lambda_function.load_sample(message, {}) == message

        # checking that the correct parameters are sent to the graphql endpoint for the first request
        mocked_request = rmock.request_history[0]
        # check the endpoint for the request is the GRAPHQL_ENDPOINT environment variable
        assert mocked_request.url == os.getenv("GRAPHQL_ENDPOINT") + "/"
        # check the api key matches the correct API key from the mocked Boto3 s3 client
        assert (
            mocked_request.headers["X-Api-Key"] == "correct key with longest validity"
        )
        # check the variables of the graphql request match the values in sample-file.json
        variables = json.loads(mocked_request.json()["variables"])
        assert (
            variables["survey_id"] == "test_survey"
            and variables["period"] == "test_period"
            and variables["details"] == json.dumps({"ruref": "test_ruref"})
        )


def test_empty_input():
    with pytest.raises(InvalidMessageException):
        src.lambda_function.load_sample({}, {})
    with pytest.raises(InvalidMessageException):
        src.lambda_function.load_sample("", {})


def test_invalid_location():
    with pytest.raises(InvalidURIError, match=r"bucket location"):
        message = {
            "location": "not_a_uri",
            "survey": "test_survey",
            "period": "test_period",
        }
        src.lambda_function.load_sample(message, {})

    with pytest.raises(InvalidURIError, match=r"bucket location"):
        message = {
            "location": "s3://notauri",
            "survey": "test_survey",
            "period": "test_period",
        }
        src.lambda_function.load_sample(message, {})


@patch("boto3.client", mock_bad_boto3)
def test_handling_of_failed_boto3_connection():
    with pytest.raises(
        Boto3ConnectionError, match=r"Failed to connect to boto3 s3 client"
    ):
        message = {
            "location": "s3://test_bucket/sample-file.json",
            "survey": "test_survey",
            "period": "test_period",
        }
        src.lambda_function.load_sample(message, {})


@patch("boto3.client", mock_boto3_client)
def test_handling_bad_s3_bucket():
    with pytest.raises(Boto3ConnectionError, match=r"Could not get sample file"):
        message = {
            "location": "s3://test_bucket/no-sample-file.json",
            "survey": "test_survey",
            "period": "test_period",
        }
        src.lambda_function.load_sample(message, {})


@patch("boto3.client", mock_boto3_client)
def test_handling_sample_file_message_missmatch():
    with pytest.raises(SampleFileError, match=r"survey_id mismatch"):
        message = {
            "location": "s3://test_bucket/sample-file.json",
            "survey": "wrong_survey",
            "period": "test_period",
        }
        src.lambda_function.load_sample(message, {})
    with pytest.raises(SampleFileError, match=r"period mismatch"):
        message = {
            "location": "s3://test_bucket/sample-file.json",
            "survey": "test_survey",
            "period": "wrong_period",
        }
        src.lambda_function.load_sample(message, {})


@patch("boto3.client", mock_boto3_client)
def test_handling_bad_sample_files():
    with pytest.raises(SampleFileError):
        message = {
            "location": "s3://test_bucket/sample-file-missing-period.json",
            "survey": "test_survey",
            "period": "test_period",
        }
        src.lambda_function.load_sample(message, {})


@patch("boto3.client", mock_boto3_client)
def test_handling_sample_file_missing_atributes():
    with pytest.raises(SampleFileError):
        message = {
            "location": "s3://test_bucket/sample-file-missing-attributes.json",
            "survey": "wrong_survey",
            "period": "test_period",
        }
        src.lambda_function.load_sample(message, {})


# TODO:
# - finish handling errors in the sample file, and related tests
# - handle errors in the appsync response, and test
