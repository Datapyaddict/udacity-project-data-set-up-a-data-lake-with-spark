import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofweek, hour, weekofyear, dayofmonth,monotonically_increasing_id


config = configparser.ConfigParser()
config.read('/home/hadoop/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    The function creates a spark session and return the session object.
    """
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.2.2")\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider',\
            'com.amazonaws.auth.DefaultAWSCredentialsProviderChain') \
    .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """"
    The function processes the json song files
    """
    # get filepath to song data file
    
    print('get song json files')
    
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
       
    # read song data file
    
    print('read in song json files')
    
    songs_df = spark.read.json(song_data)

    # extract columns to create songs table
    
    print('create songs table')
        
    song_columns = ["song_id","title", "artist_id", "year", "duration"]
    
    songs_table = songs_df.select(song_columns).\
    withColumn('year_partition', col('year')).\
    withColumn('artist_id_partition', col('year')).\
    dropDuplicates()
       
    # write songs table to parquet files partitioned by year and artist

    print('write songs table to s3')
    
    songs_table.write.mode("overwrite").\
        partitionBy("year_partition", "artist_id_partition").\
        parquet(output_data + "songs")

    # extract columns to create artists table

    print('create artists table')
    
    artists_columns = ["artist_id", "artist_name as name",\
                       "artist_location as location", "artist_latitude as latitude",\
                       "artist_longitude as longitude"]
    artists_table = songs_df.selectExpr(artists_columns).dropDuplicates()
    
    # write artists table to parquet files
    
    print('write artists table to s3')
    
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """"
    The function processes the json log files
    """
    # get filepath to log data file
    
    print('get log json files') 
    
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    
    print('read in log json files') 
    
    logs_df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    logs_df = logs_df.where(logs_df.page == "NextSong")

    # extract columns for users table    
    
    print('create users table')
    
    users_columns = ["userId as user_id", "firstName as first_name",\
                     "lastName as last_name", "gender", "level"]
    users_table = logs_df.selectExpr(users_columns).dropDuplicates()
    
    # write users table to parquet files
    
    print('write users table to s3')
    
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original timestamp column    
    # extract columns to create time table
    
    print('create time table')
    
    logs_df = logs_df.withColumn('start_time', (logs_df.ts/1000).cast('timestamp'))
    logs_df = logs_df.withColumn('hour', hour(logs_df.start_time)).\
                withColumn('day', dayofmonth(logs_df.start_time)).\
                withColumn('week', weekofyear(logs_df.start_time)).\
                withColumn('month', month(logs_df.start_time)).\
                withColumn('year', year(logs_df.start_time)).\
                withColumn('weekday', dayofweek(logs_df.start_time))
    
    time_table = logs_df.select(['start_time', 'hour','day','week','month','year',\
                       'weekday']).\
                withColumn('month_partition', col('month')).\
                withColumn('year_partition', col('year')).\
                dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    
    print('write time table to s3')
    
    time_table.write.mode('overwrite')\
            .partitionBy("year_partition", "month_partition")\
            .parquet(output_data + 'time')

    # read in song data to use for songplays table
    
    print('read in song data to use for songplays table')
    
    song_df = spark.read.parquet(output_data + "songs/*/*/*.parquet")
    song_df = song_df.\
            withColumnRenamed('year', 'song_year')

    # extract columns from joined song and log datasets to create songplays table 
    
    print('create songplays table')

    logs_songs_df = logs_df.join(song_df, logs_df.song == song_df.title)
    
    songplays_table = logs_songs_df.withColumn("songplay_id", monotonically_increasing_id())
    
    songplays_table = songplays_table.select(col('songplay_id'),\
        col('start_time'),\
        col('userId').alias('user_id'),\
        col('level'),\
        col('song_id'),\
        col('artist_id'),\
        col('sessionId').alias('session_id'),\
        col('location'),\
        col('userAgent').alias('user_agent'),\
        col('year'),\
        col('month')).\
    withColumnRenamed('year', 'year_partition').\
    withColumnRenamed('month', 'month_partition').\
    dropDuplicates()


    # write songplays table to parquet files partitioned by year and month
    
    print('write songplays table to s3')
    
    songplays_table.write.mode("overwrite").partitionBy("year_partition", "month_partition").parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    
    input_data = config['S3_BUCKET']['INPUT_DATA']  
    output_data = config['S3_BUCKET']['OUTPUT_DATA_S3A']

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print('script completed')


if __name__ == "__main__":
    main()
