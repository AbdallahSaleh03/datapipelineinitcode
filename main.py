#!/bin/bash/env python
from pipe import fetching_url_from_next,init_S3_connection,export_comment_POST_GET_and_dump_into_bucket,fetch_csv_from_s3,metadata_removal,upload_clean_data_to_S3

def main():
    # event trigger
    fetch_data = ["list","of","urls"]
    BUCKET_NAME = "Name"
    KEY = "some_key"


    queue = fetching_url_from_next()

    client = init_S3_connection()
    
    export_comment_POST_GET_and_dump_into_bucket(POST_URL=queue,
                                             BUCKET_NAME=BUCKET_NAME,
                                             client=client,
                                             KEY=KEY)
    
    raw_csv = fetch_csv_from_s3(BUCKET_NAME=BUCKET_NAME,
                      client=client,
                      KEY=KEY)
    
    clean_csv = metadata_removal(raw_csv)

    upload_clean_data_to_S3(cleaned_csv=clean_csv,
                            BUCKET_NAME=BUCKET_NAME,
                            client=client,
                            KEY=KEY)
    
if __name__ == "__main__":
    main()