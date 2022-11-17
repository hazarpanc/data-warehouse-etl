# Project Overview: Data Lake with Spark
The goal of the project is to build an ETL pipeline that extracts data from S3, stages in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights.

#### Background
A music streaming startup, Sparkify, wants to move their processes and data to the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Files

Files in this project and their details are provided below.

```
.
├── dhw.cfg           # Configuration file containing credentials for connecting to AWS Redshift and S3
├── sql_queries.py    # Contains SQL queries executed by create_tables.py and etl.py
├── create_tables.py  # Creates Redshift tables; drops any pre-existing tables
├── etl.py            # Extracts data from S3 and loads into Redshift
└── README.md

```

## Instructions to Run

Create tables on Redshift by running the following command:
```sh
$ python create_tables.py
```

Run the ETL process with the following command:
```sh
$ python etl.py
```