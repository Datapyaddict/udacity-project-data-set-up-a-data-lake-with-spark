import os
import time
import json
import boto3
import configparser

def main():
    """
    The purpose of this script is to :
        - empty the s3 bucket,
        - delete it.
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
        
    print("Delete s3 bucket steps:") 
    try:
        while True:
            objects = s3.list_objects(Bucket=OUTPUT_BUCKET_NAME)
            print(json.dumps(objects, indent=4, sort_keys=True, default=str))
            
            content = objects.get('Contents', [])
            if len(content) == 0:
                break
            for obj in content:
                print('delete object ' , obj)
                s3.delete_object(Bucket=OUTPUT_BUCKET_NAME, Key=obj['Key'])
        print("Delete s3 bucket") 
        s3.delete_bucket(Bucket=OUTPUT_BUCKET_NAME)
    except Exception as e:
        print(e)
    print("s3 bucket deleted")     
    
if __name__ == "__main__":
    main()