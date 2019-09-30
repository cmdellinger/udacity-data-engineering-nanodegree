# Udacity Data Engineering Nanodegree

This repository contains lectures slides, exercises, and projects from Udacity's Data Engineering Nanodegree program. The projects by module are as follows:

## Data modeling

### [Data Modeling with Postgres](https://github.com/cmdellinger/udacity-data-engineering-nanodegree/tree/master/02%20-%20Data%20Modeling/03%20-%20project%20-%20data%20modeling%20with%20postgres/project)

The purpose of this project is to create an ETL pipline that transfers data from two types of JSON log files stored in directory trees into a Postres database using Python and SQL.

### [Data Modeling with Apache Cassandra](https://github.com/cmdellinger/udacity-data-engineering-nanodegree/tree/master/02%20-%20Data%20Modeling/05%20-%20project%20-%20data%20modeling%20with%20apache%20cassandra/project)

The purpose of this project is to create an ETL pipline that transfers data from CSV files stored in a directory into a NoSQL database (Apache Cassandra) using Python.

## Cloud Data Warehouses

### [Data Warehousing with AWS](https://github.com/cmdellinger/udacity-data-engineering-nanodegree/tree/master/03%20-%20cloud%20data%20warehouses/04%20-%20project%20-%20data%20warehouse/project%20files)

The purpose of this project is to create an ETL pipline that transfers data from two types of JSON log files stored in S3 and load them into an Amazon Redshift database for staging and production tables.

## Data Lakes with Spark

### [Data Lakes with AWS](https://github.com/cmdellinger/udacity-data-engineering-nanodegree/tree/master/04%20-%20data%20lakes%20with%20spark/05%20-%20project%20-%20data%20lakes/project)

The purpose of this project is to create an ETL pipline that transfers data from two types of JSON log files stored in S3, transform the data using Spark on an Amazon EMR cluster, and store the new tables back to S3 as parquet files.

## Data Pipelines with Airflow

### [Data Pipelines with Airflow](https://github.com/cmdellinger/udacity-data-engineering-nanodegree/tree/master/05%20-%20data%20pipelines%20with%20airflow/04%20-%20project/project/home)

Created a pipeline in Apache Airflow that automates the ETL of JSON files on S3 to a star schema in a Redshift database, writing custom Airflow operators to run ingest, transformation, and data quality checks.

Note: Repository contains the airflow folder from the Docker install of Airflow

## Capstone

### [Captstone](https://github.com/cmdellinger/udacity-data-engineering-nanodegree/tree/master/06%20-%20capstone%20project/project)

Programatically download variable amounts of compressed CSV files from the EPA website containing hourly data. Transform the data into daily summary statistics by state, upload to S3, then `COPY` into Redshift and make a `JOIN`-ed table for maximums and means by day.
