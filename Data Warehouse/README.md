# Udacity Data Warehouse

## Project Summary

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked to build an ETL pipeline that extracts their data from S#, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Database Schema

| Table | Description |
| --- | --- |
| songplays | Fact Table showing records in event data associated with song plays i.e. records with page NextSong. |
| users | Dimension table for users in the app. |
| songs | Dimension table for song sin music database |
| artists | Dimension table for artists in music database |
| time | Dimension table for timestamps of records in songplays broken down into specific units |
