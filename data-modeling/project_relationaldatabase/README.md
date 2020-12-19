## Project Summary
This initiative aims to enable Sparkify to analyze data pertaining to streaming of songs and user activity on their new music streaming app. Currently, user-activity and song-metadata data resides in a directory in JSON format. However, Sparkify don't have  effective way to gain insight on their streaming data.


## Primary Objective
Sparkify analytics team is particularly interested in understanding - “what songs users are listening to?”

To accomplish above objective

       1. Create data model (analytics oriented) for PostgresSQL (RDBMS)
       2. Create ETL (Extract Transform and Load) pipe line to load user-activity and song-metadata from JSON file to Database.



## Data modeling
### Data set
#### a. Songs data
              Each file is in JSON format and contains metadata about a song and the artist of that song.

#### b. Logs Data
              Each file is in JSON format and contains user activity logs from a music streaming app based on specified configurations.

### Logical data model (Star Schema)
#### a. Fact table contain foreign keys referencing primary keys in surrounding dimension tables. It stores user activity metrics.
#### b. Dimension table contains meta-data, data pertaining to Songs, Artis and User.

![Alt text](LDM.png?raw=true "Title")



## Descriptions of artifacts
#### a. create_tables.py : The script successfully connects to the Sparkify database, drops any tables if they exist, and creates the tables.
#### b. sql_queries.py : This script contains common tasks such as query, commands to create tables, insert data and select data.
#### c. etl.py :The script connects to the Sparkify database, extracts and processes the log_data and song_data , and loads data into the 
#### d. test.ipynb: This script contains basic test cases for quick verification.





## How do I setup and execute program?
#### Follow below steps in order
#### a. Create Database and tables
              python create_tables.py
#### b. Execute ETL pipeline to load data
              python etl.py
#### c. Execute test case to verify (usning jupyter notebook)
               test.ipynb 
