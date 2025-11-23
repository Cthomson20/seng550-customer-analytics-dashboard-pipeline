# seng550-customer-analytics-dashboard-pipeline
This is a repository that includes our dataset and our ETL pipeline used to manipulate and ingest data in PostgreSQL.
Install the following Dependencies:
- pySpark
- pyArrow
- pandas

Create a ".env" file that includes the following fields (note to inquire creds from admin):
- DB_HOST=?
- DB_PORT=?
- DB_NAME=?
- DB_USER=?
- DB_PASSWORD=?

Attempt to run the file, if the ETL does not complete, follow the next steps:

1. open Local Disk (C:)
2. Create a folder called hadoop
3. Inside hadoop, create a folder called bin
4. download a hadoop "winutils" file
5. put "winutils" inside bin
6. Restart compiler/command line
7. rerun pipeline

