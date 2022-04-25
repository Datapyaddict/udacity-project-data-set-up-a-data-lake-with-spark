# <u>Data Engineer nanodegree / Udacity project : set up a datalake with spark</u>

## Table of Contents
1. [Project info](#project-info)
2. [Repository files info](#repository-files-info)
3. [Prerequisite to scripts run](#pre-requisite)
4. [Database modelling](#database-modelling)
5. [How to run the scripts](#how-to-run-the-scripts)

***

### Project info

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into another S3 bucket as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


***
### Repository files info

* `data/` folder contains some JSON files that can be used to test the etl pipeline before it is deployed on AWS EMR cluster.
* `etl.py` is the script that loads the JSON files from s3 `s3a://udacity-dend/`, transforms them into dimensional tables and saves the tables as parquet files in another s3 bucket.
* `dl.cfg` contains the configuration information.
* `create_s3_bucket_and_upload_scripts.py` is the script that creates the s3 bucket that will host the parquet files. Once created, the script uploads to the bucket the etl.py script and the configuration file dl.cfg.
* `create_emr_and_run_etl.py` is the script that creates an AWS EMR cluster in order to run the etl script. It terminates automatically once the etl process is terminated.
* `delete_s3_bucket.py` is the script that empties the s3 bucket and deletes it.
* `Analytics_queries.ipynb` is the notebook running a few analytic queries on the parquet files saved in s3 bucket.


***
### Prerequisite to scripts run

* Create an AWS user with `AdministratorAccess` right and use the credentials in the config file.
* Fill the config file dl.cfg. Information below are suggested.
    > [AWS]
    
    > aws_access_key_id = 
    
    > aws_secret_access_key = 


    > [CLUSTER]
    
    > emr_release_label = emr-5.35.0

    > emr_num_worker_nodes = 3

    > emr_instance_type = m5.xlarge

    > emr_cluster_identifier = sparkify-emr-cluster

    > aws_region = us-east-1

    > aws_iam_role_name = emr_role

    > [S3_BUCKET]

    > input_data = s3a://udacity-dend/

    > output_bucket_name = udacity-sparkify

    > output_data = s3://udacity-sparkify/

    > output_data_s3a = s3a://udacity-sparkify/


***
## Database modelling

The database model consists of :
* one fact table : `songplay`,
* dimension tables : `users`, `songs`, `artists` , `time`. 

The data model is a simple star schema.

Mapping rules from JSON files to the tables:

* __songplay__:

| column | source file.field  |
|:--------------|:-------------|
| start_time | *log_file.ts* |
| user_id | *log_file.userid* |
| level | *log_file.level* |
| song_id | *song_file.songid*|
| artist_id | *song_file.artist_id*|
| session_id | *log_file.sessionId*|
| location | *log_file.location*|
| user_agent | *log_file.userAgent*|

* __users__:

| column | source file.field  |
|:--------------|:-------------|
| user_id | *log_file.userid* |
| first_name | *log_file.firstName* |
| last_name | *log_file.lastName* |
| gender | *log_file.gender*|
| level | *log_file.level*|

* __songs__:

| column | source file.field  |
|:--------------|:-------------|
| song_id | *song_file.songid* |
| title | *song_file.title* |
| artist_id | *song_file.artist_id* |
| year | *song_file.year*|
| duration | *song_file.duration*|

* __artists__:

| column | source file.field  |
|:--------------|:-------------|
| artist_id | *song_file.artist_id* |
| name | *song_file.artist_name* |
| location | *song_file.artist_location* |
| latitude | *song_file.artist_latitude*|
| longitude | *song_file.artist_longitude*|

* __time__:

| column | source file.field  |
|:--------------|:-------------|
| start_time | *log_file.ts* |
| hour | *log_file.ts* |
| day | *log_file.ts* |
| week | *log_file.ts* |
| month | *log_file.ts* |
| year | *log_file.ts* |
| weekday| *log_file.ts* |

**<u>Important</u>** : `song_id` and and `artist_id` columns in `songplay` table are derived from the `song_file` JSON file 
***

## How to run the scripts


Open a terminal on local machine and run the following scripts.
> 1. run `create_s3_bucket_and_upload_scripts.py` in order to create the output s3 bucket and upload the etl.py script and configuration file dl.cfg.
> 2. run `create_emr_and_run_etl.py` in order to create the EMR cluster. The master node will then run the etl pipeline and create the dimensional tables in s3. Afterwards, it self-terminates.
> 3. run the jupyter notebook `Analytics queries.ipynb` to check the tables stored as parquet files in s3. 
> 4. run `delete_s3_bucket.py` in order to empty the output s3 bucket and delete it. .
