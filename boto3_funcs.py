import logging
import boto3
from botocore.exceptions import ClientError
import os
from botocore.config import Config


def init_client(ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN):
    """initalize s3 client

    :param ACCESS_KEY
    :param SECRET_KEY 
    :param SESSION_TOKEN 
    :return: Client if intialization was sucessful, else None
    """
    my_config = Config(
    region_name = 'us-west-2',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
    )   
    
    try:
        client = boto3.client('s3',
                            config=my_config,
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY,
                            aws_session_token=SESSION_TOKEN)
        return client
    except: 
        return None

def upload_file(client,
                file_name,
                bucket,
                object_name=None):
    """Upload a file to an S3 bucket
    :param s3 client
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True