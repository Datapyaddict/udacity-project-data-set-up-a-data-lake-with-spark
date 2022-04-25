import os
import time
import json
import boto3
import configparser

def main():
    """
    The purpose of this script is to create s3 bucket and uplaod to it
    the following files:
        - etl.py
        - dl.cfg
    """
    
    print("read in the config file")

    config = configparser.ConfigParser()
    config_file_path = 'dl.cfg'
    config.read_file(open(config_file_path))
    
    
    KEY                    = config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET                 = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
    REGION                 = config.get("CLUSTER","AWS_REGION")
    
    
    OUTPUT_BUCKET_NAME     = config.get("S3_BUCKET","OUTPUT_BUCKET_NAME")
    
    print("Creating iam, s3 and emr clients") 
    try:        
        s3 = boto3.client('s3',aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET,
                             region_name=REGION)
    except Exception as e:
        print(e)
        
    print("Creating s3 bucket") 
    try:
        s3.create_bucket(ACL='private',Bucket=OUTPUT_BUCKET_NAME)
    except Exception as e:
        print(e)

    print("upload etl.py script to s3")
    try:
        s3.upload_file(Filename='etl.py',\
                    Bucket=OUTPUT_BUCKET_NAME,\
                    Key='etl.py')
    except Exception as e:
        print(e)
        
    print("upload dl.cfg to s3")
    try:
        s3.upload_file(Filename='dl.cfg',\
                    Bucket=OUTPUT_BUCKET_NAME,\
                    Key='dl.cfg')
    except Exception as e:
        print(e)
    
    
if __name__ == "__main__":
    main()