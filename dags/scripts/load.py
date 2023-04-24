
from pyspark.sql import SparkSession
import sys as sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--target-table', help='Target Remote Table')
parser.add_argument('--snowflake-url', help='Snowflake URL')
parser.add_argument('--snowflake-user', help='Snowflake user')
parser.add_argument('--snowflake-password', help='Snowflake password')
parser.add_argument('--snowflake-database', help='Snowflake database')
parser.add_argument('--snowflake-schema', help='Snowflake schema')
parser.add_argument('--snowflake-warehouse', help='Snowflake schema')

args = parser.parse_args()

conn_params = {
    "host": "postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("LoadApp") \
    .master("local[*]")\
    .getOrCreate()

print('Spark Configured with',spark.sparkContext.getConf().getAll())

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}") \
    .option("dbtable", args.target_table) \
    .option("user", conn_params["user"]) \
    .option("password", conn_params["password"]) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(df.printSchema())

sfOptions = {
    "sfURL" : args.snowflake_url,
    "sfUser" : args.snowflake_user,
    "sfPassword" : args.snowflake_password,
    "sfDatabase": args.snowflake_database,
    "sfSchema": args.snowflake_schema,
    "sfWarehouse" : args.snowflake_warehouse,
    "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    "dbtable": args.target_table
}

SNOWFLAKE_SOURCE_NAME = "snowflake"

df.write \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .mode('overwrite') \
    .save()