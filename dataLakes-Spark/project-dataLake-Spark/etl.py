import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

#Schema of the song data to load in spark
schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", StringType(), True),
        StructField("artist_longitude", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", FloatType(), True),
        StructField("year", IntegerType(), True),
    ])

def create_spark_session():
    """Returns Spark session of EMR cluster

    If no session exists - a new Spark session is built, else existing Spark session is returned.

    :return:
        SparkSession: The entry point to Spark cluster.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Extract, Transforms and Load song data available in s3, to Two Tables: songs_table, artists_table. The tables
    are stored as parquet files in s3.

    Args:
        SparkSession: The entry point to  Spark cluster.
        input_data: Path to the songs_data
        output_data: Path to output, location to store songs and artist data outputs.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data, schema)

    # extract columns to create songs table
    songs_cols =  ["song_id","title","artist_id","year", "duration"]
    songs_table = df[songs_cols].na.drop(subset=["song_id"]).dropDuplicates(subset=["song_id"])

    # write songs table to parquet files partitioned by year and artist
    output_songs = output_data+"songs"
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_songs)

    # extract columns to create artists table
    artists_cols = ["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]

    #Rank artists 
    window = Window.partitionBy("artist_id").orderBy("year")
    ranked_artists = df.withColumn("row_num", row_number().over(window))
    ##extract
    artists_table = ranked_artists.filter(ranked_artists.row_num == 1).select(artists_cols)
    
    # write artists table to parquet files
    output_artists = output_data+"artists"
    artists_table.write.mode("overwrite").parquet(output_artists)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    #artists_table =
    users_cols = [col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), "gender", "level"]
    latest_visits = df.na.drop(subset=["userId"]).groupBy(df.userId).max("ts").select(col("max(ts)").alias("max_ts")).collect()

    max_ts_values = [row.max_ts for row in latest_visits]
    users_table = df.filter(df.ts.isin(max_ts_values)).select(users_cols)

    # write users table to parquet files
    ##artists_table
    output_users = output_data+"users"
    users_table.write.mode("overwrite").parquet(output_users)


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000)
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn('start_time', get_datetime('timestamp'))
    
    # extract columns to create time table
    time_table = df.select("start_time", 
                           hour("start_time").alias("hour"),
                           dayofmonth("start_time").alias("day"),
                           weekofyear("start_time").alias("week"),
                           month("start_time").alias("month"),
                           year("start_time").alias("year"),
                           date_format("start_time", 'u').alias("weekday").cast(IntegerType())
                           )
    
    # write time table to parquet files partitioned by year and month
    output_time = output_data+"time"
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_time)

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data, schema)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_cols = ["start_time",
                      col("userId").alias("user_id"),
                      "level",
                      "song_id",
                      "artist_id",
                      col("sessionId").alias("session_id"),
                      "location",
                      col("userAgent").alias("user_agent"),
                      year(col("start_time")).alias("year"),
                      month(col("start_time")).alias("month")
                      ]
    songplays_table = df.join(song_df,
                                ((df.song == song_df.title) & (df.artist == song_df.artist_name)& (df.length == song_df.duration)),
                                how='left').select(songplays_cols)

    # write songplays table to parquet files partitioned by year and month
    output_songplays = output_data + "songplays"
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_songplays)


def main():
    # Get session
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    # Stop spark session
    spark.stop()

if __name__ == "__main__":
    main()
