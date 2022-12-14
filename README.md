# Data Warehouse ETL Project with AWS Redshift

## Purpose
A music streaming startup, Sparkify, has their data in Amazon S3. Goal of this project is to build an ETL pipeline that extract raw data from S3, stage in Redshift, and transform data into a set of dimensional tables.<br>
The final tables will be used by Sparkify's analytics team to gain insight into the songs that their users are listening to.

## Files

Files in this project and their details are provided below.

```
.
├── dhw.cfg           # Configuration file containing credentials for connecting to AWS Redshift and S3
├── sql_queries.py    # Contains SQL queries executed by create_tables.py and etl.py
├── create_tables.py  # Creates Redshift tables; drops any pre-existing tables
├── etl.py            # Executes the queries to extract data from the S3 bucket and load data into Redshift.
└── README.md

```

## How to Run

Have an up and running AWS Redshift Cluster with an associated IAM role with the following permissions: AmazonS3ReadOnlyAccess, AmazonRedshiftFullAccess. Enter the cluster information in dwh.cfg.

Create tables on Redshift by running the following command:
```sh
$ python create_tables.py
```

Run the ETL process with the following command:
```sh
$ python etl.py
```