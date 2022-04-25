import os
import time
import json
import boto3
import configparser



def main():
    """
    The purpose of this script is to create an AWS EMR cluster that
    runs the etl script and terminates automatically once the 
    etl process is done.
    The pre-requisites:
        - have the s3 bucket already created with
        the etl.py and dl.cfg files uploaded into it.
        - use aws credentials with admin rights,
        - use a key pair attached to the profile
    Steps of the function:
        - read in the config file,
        - create iam and emr clients,
        - create IAM role with relevat policies,
        - create emr cluster which will run the following steps:
            - set up debugging,
            - copy etl.py and dl.cfg files from s3 to emr machine.
            - run etl.py
            - emr auto- terminates.
        - wait until the emr is auto-terminated,
        - detach IAM role policies,
        - delete IAM role
        
    """
    
    print("read in the config file")

    config = configparser.ConfigParser()
    config_file_path = 'dl.cfg'
    config.read_file(open(config_file_path))
    
    
    KEY                    = config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET                 = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
    CLUSTER_IDENTIFIER     = config.get("CLUSTER","EMR_CLUSTER_IDENTIFIER")
    NUM_NODES              = config.get("CLUSTER","EMR_NUM_WORKER_NODES")
    INSTANCE_TYPE          = config.get("CLUSTER","EMR_INSTANCE_TYPE")
    REGION                 = config.get("CLUSTER","AWS_REGION")
    RELEASE_LABEL          = config.get("CLUSTER","EMR_RELEASE_LABEL")
    
    IAM_ROLE_NAME          = config.get("CLUSTER","AWS_IAM_ROLE_NAME")
    
    OUTPUT_BUCKET_NAME     = config.get("S3_BUCKET","OUTPUT_BUCKET_NAME")
    OUTPUT_DATA            = config.get("S3_BUCKET","OUTPUT_DATA")
    
    print("Creating iam and emr clients") 
    try:
        iam = boto3.client('iam',aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET,
                             region_name=REGION
                          )
    
        emr = boto3.client('emr',aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET,
                               region_name=REGION                   
                               )    
    except Exception as e:
        print(e)
        
    print("Creating a new IAM Role") 
    try:
        emrRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = 
            "Allows EMR clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 
                                 'elasticmapreduce.amazonaws.com'}}],
                 'Version': '2012-10-17'})
            )    
    except Exception as e:
        print(e)
    
    print(json.dumps(emrRole, indent=4, sort_keys=True, default=str))
    
    
    print("Attaching S3 Full Access Policy to IAM role")
    try:
        iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')
    except Exception as e:
        print(e)
        
    print("Attaching Default EMR Policy to IAM role")
    try:
        iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
        PolicyArn=
        'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
                              ) 
    except Exception as e:
        print(e)
        
        
    print("Create emr cluster")   
    try:
        response = emr.run_job_flow(
            Name= CLUSTER_IDENTIFIER,
            LogUri=os.path.join(OUTPUT_DATA, 'logs/'),
            ReleaseLabel=RELEASE_LABEL,
            Applications=[
                {
                    'Name': 'Spark'
                },
            ],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': INSTANCE_TYPE,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Core nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': INSTANCE_TYPE,
                        'InstanceCount': int(NUM_NODES),
                    }
                ],\
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False
            },
            Steps=[
                {
                    'Name': 'Debugging-setup',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['state-pusher-script']
                    }
                },
                {
                    'Name': 'copy-script-files-from-s3-to-emr',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws','s3','cp','s3://'+ OUTPUT_BUCKET_NAME,
                                 '/home/hadoop/','--recursive']
                    }
                },
                {
                    'Name': 'run-spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit','/home/hadoop/etl.py']
                    }
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole=IAM_ROLE_NAME
        )
        cluster_id = response['JobFlowId']
    except Exception as e:
        print(e)
        
    print(json.dumps(response, indent=4, sort_keys=True, default=str))
    
    print("Wait until cluster is terminated before deleting IAM role") 
    while True:
        cluster_status = emr.describe_cluster(ClusterId=cluster_id)\
            ['Cluster']['Status']['State']
    #     cluster_status = cluster_props["ClusterStatus"]
        if 'TERMINATED' in cluster_status:
            print("cluster terminated") 
            break
        time.sleep(1)    
    
    print("detach IAM role policy S3 full access") 
    try:
        iam.detach_role_policy(RoleName=IAM_ROLE_NAME, 
                    PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')
    except Exception as e:
        print(e)        
    
    print("detach IAM role policy for emr") 
    try:
        iam.detach_role_policy(RoleName=IAM_ROLE_NAME, 
        PolicyArn=
        'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole')
    except Exception as e:
        print(e)     
        
    print("delete IAM role") 
    try:
        iam.delete_role(RoleName=IAM_ROLE_NAME)
    except Exception as e:
        print(e)     


if __name__ == "__main__":
    main()