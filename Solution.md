# PROJECT SOLUTION

## Processing

The type of data pipeline will be ETL, since the source data comes from a non-normalized source which is the NikeScrAPI.

### ETL

- Extraction:
    This uses the NikeScrapi to scrape data from a Nike API endpoint, and saves it into a pandas data frame.
    Use pandas to save into a csv with a pipe separator since CSV reading in later steps may be affected due to 
    Short-Description having several commas inside its text. 

- Transform:
    This uses a SparkSubmitOperator to get the csv, remove some columns and then save into a local postgres DB.

- Load:
    This uses a SparkSubmitOperator to get the data from a local postgres DB and loads it into a Snowflake Warehouse.

## Warehouse

For a data Warehouse we are using Snowflake, and we already have a database, a schema and SQL worksheets to handle data for analysis.

1. First step is to create the tables, this is done by running the statements in the table_creation.sql file.

2. Second step is to insert the data from the raw table into our schema tables, this is done by running the statements in the table_insertion.sql file.

3. Lastly, we query the elements we want to get for data visualization, with a view to make it easier to use in a BI instrument.