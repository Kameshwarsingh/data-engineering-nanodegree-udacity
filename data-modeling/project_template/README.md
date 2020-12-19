## Project Summary
This initiative aims to enable Sparkify to analyze data pertaining to streaming of songs and user activity on their new music streaming app. Currently, user-activity data resides in a directory in CSV format. However, Sparkify don't have  effective way to gain insight on their user-activity data.


## Primary Objective
Sparkify analytics team is particularly interested in understanding below sample queries.

Sample queries:

        1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
        2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
        3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


To accomplish above objective

       1. Create data model (analytics oriented) for NoSQL DB (Cassandra), use above Sample queries to create data model (LDM)
       2. Create ETL (Extract Transform and Load) pipe line to load user-activity from CSV file to Database.




## Field mapping and Logical Data model (LDM)
![Alt text](NoSQL_DataModel.PNG?raw=true "Logical Data model (LDM)")
