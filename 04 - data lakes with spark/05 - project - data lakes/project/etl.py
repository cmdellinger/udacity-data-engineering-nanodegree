""" ETL pipline
    
Ingests JSON files in 'input_data/log_data' and 'input_data/song_data' trees
into Spark DataFrames. The DataFrames are transformed into other DataFrames
that contain the columns corresponding to the schema in `README.md`. The
processed data is then saved to parquet file trees in 'output_data/'
"""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import to_date, to_timestamp

# load AWS credentials from config file into enviromental variables
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def ts_msg(message):
    """ prints string with a timestamp """
    timestamp = "{} ".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(timestamp + message)
    
def create_spark_session():
    """ creates Spark session with AWS hadoop package """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """ Loads song data into songs and artists tables and saves them as parquet.
        
    The function ingests data into a Spark DataFrame from the song JSON files
    located at 'input_data/song_data/'. The applicable data columns are parsed
    and transformed based on the `songs` and `artists` table schema. The tables
    are then save as parquet file/directory tree in the `output_data` directory.
    
    Args:
        spark (SparkSession): SparkSession object of Spark cluster/instance
        intput_data (str): root filepath containing `song_data` directory
        output_data (str): root filepath of directory to save parquet file trees
    Returns:
        `None`: actions performed, but no return value
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    ts_msg("selecting songs_table")
    songs_table = df.select("song_id", \
                            "title", \
                            "artist_id", \
                            "year", \
                            "duration") \
                    .dropDuplicates()
            
    # write songs table to parquet files partitioned by year and artist
    ts_msg("writing songs_table")
    songs_table.write.parquet(os.path.join(output_data, "songs_table"), \
                              mode='overwrite', \
                              partitionBy=["year","artist_id"])

    # extract columns to create artists table
    ts_msg("selecting artists_table")
    artists_table = df.select('artist_id', \
                              col('artist_name').alias('name'), \
                              col('artist_location').alias('location'), \
                              col('artist_latitude').alias('latitude'), \
                              col('artist_longitude').alias('longitude')) \
                      .dropDuplicates()
    
    # write artists table to parquet files
    ts_msg("writing artists_table")
    artists_table.write.parquet(os.path.join(output_data, "artists_table"), \
                                mode='overwrite')
    
def process_log_data(spark, input_data, output_data):
    """ Loads log data to users, time, & songplays tables and saves as parquet.
        
    The function ingests data into a Spark DataFrame from the log JSON files
    located at 'input_data/log_data/'. The applicable data columns are parsed
    and transformed based on the `users`, `time`, and `songplays` table schema.
    The tables are then save as parquet file/directory tree in the `output_data`
    directory.
    
    Args:
        spark (SparkSession): SparkSession object of Spark cluster/instance
        intput_data (str): root filepath containing `song_data` directory
        output_data (str): root filepath of directory to save parquet file trees
    Returns:
        `None`: actions performed, but no return value
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table
    ts_msg("selecting users_table")
    users_table = df.select(col('userId').alias('user_id'), \
                            col('firstName').alias('first_name'), \
                            col('lastName').alias('last_name'), \
                            'gender', \
                            'level') \
                    .dropDuplicates()
            
    # write users table to parquet files
    ts_msg("writing users_table")
    users_table.write.parquet(os.path.join(output_data, "users_table"), \
                               mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', to_timestamp(df.ts/1000))

    # create datetime column from original timestamp column
    df = df.withColumn('datetime', to_date(df.timestamp))
    
    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    ts_msg("selecting time_table")
    time_table = df.select(col('timestamp').alias('start_time'), \
                           hour(col('datetime')).alias('hour'), \
                           dayofmonth(col('datetime')).alias('day'), \
                           weekofyear(col('datetime')).alias('week'), \
                           month(col('datetime')).alias('month'), \
                           year(col('datetime')).alias('year'), \
                           date_format(col('datetime'), 'E').alias('weekday')) \
                    .dropDuplicates()
            
    # write time table to parquet files partitioned by year and month
    ts_msg("writing time_table")
    time_table.write.parquet(os.path.join(output_data, "time_table"), \
                             mode='overwrite', \
                             partitionBy=["year","month"])

    # read in song data to use for songplays table
    ts_msg("loading songs_table")
    song_df = spark.read.parquet(os.path.join(output_data, 'songs_table'))

    # extract columns from joined song and log datasets to create songplays table
    ts_msg("selecting songplays_table")
    songplays_table = df.join(song_df, df.song == song_df.title) \
                        .select(col('timestamp').alias('start_time'), \
                                col('userId').alias('user_id'), \
                                'level', \
                                'song_id', \
                                'artist_id', \
                                col('sessionId').alias('session_id'), \
                                'location', \
                                col('userAgent').alias('user_agent'), \
                                year('timestamp').alias('year'), \
                                month('timestamp').alias('month')) \
                        .dropDuplicates() \
                        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    ts_msg("writing songplays_table")
    songplays_table.write.parquet(os.path.join(output_data, "songplays_table"), \
                                  mode='overwrite', \
                                  partitionBy=["year","month"])

def main():
    spark = create_spark_session()
    
    # input and output directories or S3 paths
    # note: don't use trailing '/' 
    input_data = "s3a://sampleS3bucket/input_data"
    output_data = "s3a:/sampleS3bucket/output_data"
    
    print("===== processing song_data =====")
    process_song_data(spark, input_data, output_data)
    print("===== processing log_data =====")
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
