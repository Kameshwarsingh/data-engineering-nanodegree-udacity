## 1 Project Summary
This initiative aims to enable Sparkify to analyze data pertaining to streaming of songs and user activity on their new music streaming app. Currently, user-activity and song-metadata data resides in AWS S3 in JSON format. Sparkify has grown their user base and song database, and want to scale data analysis processes and move data onto AWS cloud.


## 2 Primary Objective
Sparkify analytics team is particularly interested in get insight of  - “what songs their users are listening to?”

To accomplish above objective
       1. Build ETL (Extract Transform and Load) pipe line to
              - Extract and Load data from S3 to staging tables on Redshift
              - Transforms data into a set of dimensional tables for analytics
       2. Create Logical data model (LDM) for RedShift
       3. Test end to end ETL pipe line



## 3 Data modeling
### 3.1 Data set
#### a. Songs data
           - Each file is in JSON format and contains metadata about a song and the artist of that song.
           - Data is located on S3 at s3://udacity-dend/song_data
           - Sample Data in JSON format
             { "num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud",    "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0 }


#### b. Logs Data
            - Each file is in JSON format and contains user activity logs from a music streaming app based on specified configurations.
            - Data is located on S3 at s3://udacity-dend/log_data
            - Sample Data in JSON format
               {"artist":"Mr Oizo","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":3,"lastName":"Summers","length":144.03873,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Flat 55","status":200,"ts":1541106352796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}





### 3.2 Logical data model (Star Schema)
#### a. Stagging table follows structure and data-type of raw data files.

![Alt text](ldm/stagingTables.PNG?raw=true "Staging tables")

#### b. Fact table contain foreign keys referencing primary keys in surrounding dimension tables. It stores user activity metrics.

![Alt text](ldm/factTable.PNG?raw=true "Fact table")

#### c. Dimension table contains meta-data, data pertaining to Songs, Artis and User.

![Alt text](ldm/dimensionTables.PNG?raw=true "Dimension tables")

#### d. Distribution Keys and Sort Keys
       - Distribtuion Keys:  
              - Fact table: Since song dimension has more rows than other dimension, we make song_id as distirbution column for Fact table (songplay).
              - Dimension table: Dimension tables are small, so we use "diststyle all" for dimension tables
       - Sort Keys:
              - Primary Key is used as sort key.




## 4 Descriptions of artifacts
#### a. create_tables.py : The script successfully connects to database, drops any tables if they exist, and creates the tables.
#### b. sql_queries.py : This script contains common tasks such as query, commands to create tables, insert data and select data.
#### c. etl.py : The script connects to database, loads data in staging tables and insert data in dimension & fact tables.
#### d. test_queries.ipynb : This script runs test queries to confirm  implementation.






## 5 How do I setup and execute program?
#### Follow below steps in order
#### a. Create Database and tables
              python create_tables.py
#### b. Execute ETL pipeline to load data
              python etl.py
