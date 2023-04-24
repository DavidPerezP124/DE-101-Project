from pyspark.sql import SparkSession
import sys as sys

path = sys.argv[1]
target_table = sys.argv[2]

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
    .appName("ETLApp") \
    .master("local[*]")\
    .getOrCreate()

print('Spark Configured with',spark.sparkContext.getConf().getAll())

df = spark.read.csv(path, header=True, inferSchema=True, sep="|")
print(f'file has {df.count()} rows')

# Drop dummy columns from CSV file.
df = df.drop("_c0","Unnamed: 0",'short_description')
print('The current schema for the data frame')
print('The first items', df.head(8))
df.printSchema()

df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}") \
    .option("dbtable", target_table) \
    .option("user", conn_params["user"]) \
    .option("password", conn_params["password"]) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
