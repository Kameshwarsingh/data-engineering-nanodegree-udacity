## 1 Project Summary
This initiative aims to enable Sparkify to analyze data pertaining to streaming of songs and user activity on their new music streaming app. Currently, user-activity and song-metadata data resides in AWS S3 in JSON format. Sparkify has grown their user base and song database, and want to scale data analysis processes and move data onto AWS cloud.


## 2 Primary Objective
Sparkify analytics team is particularly interested in get insight of  - “what songs their users are listening to?”

To accomplish above objective
       1. Build ETL (Extract Transform and Load) pipe line to
              - Extract data from S3 processes them using Spark.
              - Load processed data back into S3 as a set of dimensional tables
       2.Test end to end ETL pipe line


## 3 Data modeling
### 3.1 Data set
#### a. Songs data
           - Each file is in JSON format and contains metadata about a song and the artist of that song.
           - Data is located on S3 at s3://udacity-dend/
           - Sample Data in JSON format
             { "num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud",    "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0 }


#### b. Logs Data
            - Each file is in JSON format and contains user activity logs from a music streaming app based on specified configurations.

            - Data is located on S3 at s3://udacity-dend/

            - Sample Data in JSON format
               {"artist":"Mr Oizo","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":3,"lastName":"Summers","length":144.03873,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Flat 55","status":200,"ts":1541106352796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}

### 3.2 Approach
Spark-based datalake solution  allows a wide range of analytics on the row data. 

#### a. Dimension tables
Song dataset can be divided into two groups. One group of fields concern the artist and other concern with Song.

       Artist: "artist_id","artist_name","artist_location","artist_latitude","artist_longitude"

       Song: "song_id","title","artist_id","year", "duration"

       Time table : "start_time", hour("start_time").alias("hour"), dayofmonth("start_time").alias("day"), weekofyear("start_time").alias("week"),month("start_time").alias("month"), year("start_time").alias("year"), date_format("start_time", 'u').alias("weekday").cast(IntegerType())
                        

#### b. Fact table
Song Plays record will be indexed with a new field called songplay_id which will serve as a primary key for fact table.

       Song Plays : "start_time", col("userId").alias("user_id"),"level", "song_id", "artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"), year(col("start_time")).alias("year"), month(col("start_time")).alias("month") 



## 4 Descriptions of artifacts
#### a. etl.py : The script connects to Spark AWS EMR cluster, loads and extracts data using Spark, and writes back output in S3 as a set of dimensional tables.
#### b. dl.cfg : Configurations file with AWS credentials.


## 5 How do I setup and execute program?
#### Follow below steps in order
#### a. Logon to AWS EMR Master public DNS. Submit Spark job 
        spark-submit --master yarn  etl.py
