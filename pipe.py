import pandas as pd
import boto3 
import requests
import pkg_resources
from exportcomments import ExportComments, ExportCommentsException
import time
import sys
import os
import logging 

def fetching_url_from_next()->object:
    pass

#https://docs.digitalocean.com/products/spaces/how-to/use-aws-sdks/
def init_S3_connection(BUCKET_NAME:str)-> object:

    session = boto3.session.Session()
    client = session.client('s3',
                            region_name='nyc3',
                            endpoint_url='https://nyc3.digitaloceanspaces.com',
                            aws_access_key_id=os.getenv('SPACES_KEY'),
                            aws_secret_access_key=os.getenv('SPACES_SECRET'))
    return client


#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
#https://docs.digitalocean.com/reference/api/spaces-api/#upload-an-object-put
#https://github.com/exportcomments/exportcomments-python
def export_comment_POST_GET_and_dump_into_bucket(POST_URL:str,
                                            BUCKET_NAME:str,
                                            client,
                                            KEY:str) -> None:
    ex = ExportComments('<YOUR API TOKEN HERE>')
    while True:
        response = ex.exports.check(guid=guid)
        status = response.body['data'][0]['status']

        if status == 'done':
            break
        elif status == 'error':
            print("Error generating your file.")
            sys.exit()

        time.sleep(20)

    download_url = response.body['data'][0]['downloadUrl']
    headers = {
        'Authorization': "Your API Token",
        'Content-Type': 'application/json',
        'User-Agent': 'python-sdk-{}'.format(pkg_resources.get_distribution('exportcomments').version),
    }

    response = requests.get("https://exportcomments.com/" + download_url, headers=headers)




    client.put_object(Bucket=BUCKET_NAME,
              Key='encrypt-key',
              Body=b'foobar',
              SSECustomerKey=KEY,
              SSECustomerAlgorithm='AES256')
    
    # if response.status_code == 200:
    #     with open("result.xlsx", "wb") as output:
    #         output.write(response.content)
    #     print(f"[SUCCESSFUL DOWNLOAD] File Downloaded: {download_url}")
    # else:
    #     print(f"[FAILED TO DOWNLOAD] Status Code: {response.status_code}")


def fetch_csv_from_s3(BUCKET_NAME:str,
                      client,
                      KEY:str)-> object:
    
    response = client.get_object(Bucket=BUCKET_NAME,
                         Key='encrypt-key',
                         SSECustomerKey=KEY,
                         SSECustomerAlgorithm='AES256')
    

    return response['Body'].read()


def metadata_removal(csv_file)->object:
    #cleaning here
    cleaned_csv = csv_file
    return cleaned_csv



def upload_clean_data_to_S3(cleaned_csv,
                            BUCKET_NAME:str,
                            client,
                            KEY:str)->None:
    
    client.put_object(Bucket=BUCKET_NAME,
            Key='encrypt-key',
            Body=cleaned_csv,
            SSECustomerKey=KEY,
            SSECustomerAlgorithm='AES256')



