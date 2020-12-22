## 1 Project Summary
This initiative aims to enable Sparkify to analyze data pertaining to streaming of songs and user activity on their new music streaming app. Currently, user-activity and song-metadata data resides in AWS S3 in JSON format. Sparkify has grown their user base and song database, and want to scale data analysis processes and move data onto AWS cloud.

<hr>

## 2 Primary Objective
Sparkify has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

### 2.1 Why Airflow?
       a) Create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.
       b) Data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.
Keeping requirements in context and thinking forward, Airflow is good option to accomplish Sparkify requirments.

<hr>

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
Airflow is used to build the ETL Pipeline. Primarily two DAGs are desined to accomplish the Goal.

       - udac_example_create_table_dag : This DAG creates table on RedShift. Schduler for this DAG is set as None, this means this DAG requires external trigger to execute.
       - udac_example_dag : This DAG performs ETL task/jobs. It can be scheduled to run repeatedly. 


#### a. Dimension tables
Song dataset can be divided into two groups. One group of fields concern the artist and other concern with Song.

       Artist: "artist_id","artist_name","artist_location","artist_latitude","artist_longitude"
       Song: "song_id","title","artist_id","year", "duration"

       Time table : "start_time", hour("start_time").alias("hour"), dayofmonth("start_time").alias("day"), weekofyear("start_time").alias("week"),month("start_time").alias("month"), year("start_time").alias("year"), date_format("start_time", 'u').alias("weekday").cast(IntegerType())
                        

#### b. Fact table
Song Plays record will be indexed with a new field called songplay_id which will serve as a primary key for fact table.

       Song Plays : "start_time", col("userId").alias("user_id"),"level", "song_id", "artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"), year(col("start_time")).alias("year"), month(col("start_time")).alias("month") 

#### b. Staging tables
Staging tables are used to stage data from S3 to database table. Staging tables help in transforming data before loading to dimension and fact tables (OLAP)

##### "Staging Events" - this tables stages data from logs_data (events).

       Staging Events: artist,auth,firstname,gender,iteminsession,lastname,length numeric,	"level",	location,"method",page,registration,sessionid,song,status,ts,useragent,userid

#### "Staging Songs" - this tables stages data from songs_data (dimensions).

       Staging Songs: num_songs,artist_id,artist_name,artist_latitude,artist_longitude,	artist_location,song_id,title,duration,"year".


<hr>

## 4 Descriptions of artifacts

       └───airflow
       ├───dags
       │   └───
       └───plugins
              ├───helpers
              │   └───
              ├───operators
              │   └───
        
The <b>airflow</b> folder is base folder. <b>airflow</b> contains <b>dag</b> and <b>plugins</b>   folder.  <b>dag</b> folder contains collection of DAGs and <b>plugins</b> folder contains operators and helper/utility clasess, SQLs.


<hr>

## 5 Design overview - DAGS and Operators

### 5.1 Operators
There are four different operators that will stage, transform, and checks on data quality.  Operators and task will execute SQL statements against the Redshift database.

### 5.2 Stage Operator
The stage operator loads JSON or CSV formatted files from S3 to Amazon Redshift.

### 5.3 Fact and Dimension Operators
The Fact/dimension operator takes SQL statement as input and executes it on target database Redshift). Dimension tables can be truncated before Insert. Fact tables are massive so it is advisable to allow only append, and no truncate..

### 5.4 Data Quality Operator
Data Quality Operator is meant for data quality checks. This operator can be extended to add any bussiness or data quality check operation to improve quality and confidence in data.


### 5.5 Graph View of DAG
![Alt text](DAG_GraphView.png?raw=true "DAG Graph View")


### 5.6 Tree View of DAG
![Alt text](DAG_TreeView.png?raw=true "DAG Tree View")



<hr>

## 6 How do I setup and execute program?
#### Follow below steps in order
#### a. Logon to AWS and setup RedShift cluster.
#### b. Configure (airflow.cfg) Airflow to point to <b>airflow</b> folder. 
       dags_folder = /airflow/dags
#### c. Configure AWS credentials(<b>aws_credentials</b>) and RedShift(<b>redshift</b>) conection in AirFlow.     
#### d. Execute dag <b>udac_example_createTable_dag</b> to create tables on RedShift. 
#### e. Execute dag <b>udac_example_dag</b> to execute ETL pipeline (Stage/Load, Transform, and Load to OLAP cubes).
       i) udac_example_create_table_dag : This DAG creates table on RedShift. Schduler for this DAG is set as None, this means this DAG requires external trigger to execute.
       ii) udac_example_dag : This DAG performs ETL task/jobs. It can be scheduled to run repeatedly.
