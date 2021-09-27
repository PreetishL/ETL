###Section 1

Installed and configured both SQL (Postgres) and NoSQL (mongoDB)

ETL PIPELINE - 
Created a ETL Pipeline using both SQL and NoSQL where the pipeline extracts data from the given CSV file (DATA.csv), Transforms it where it checks for duplicate entries into the database for email and ipaddress if the email and ipaddress exists in the given database and then it loads into the database. 

etl_main.py executes both the jobs for SQL and NoSQL where it executes by calling the class and functions from both the files etl_pg.py and etl_mongo.py 


