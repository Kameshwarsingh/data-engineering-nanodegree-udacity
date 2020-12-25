from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import udf, trim, lower
from pyspark.sql.types import StringType


output_data = "s3://kamesh-capstone/"
s3 = "s3://kamesh-capstone/"

def create_spark_session():
    """Returns Spark session of EMR cluster
    If no session exists - a new Spark session is built, else existing Spark session is returned.
    :return:
        SparkSession: The entry point to Spark cluster.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

DEMOGRAPHICS_DATA_PATH = "data/raw/demographics/us-cities-demographics.csv"
input_log_data_file = os.path.join(s3, DEMOGRAPHICS_DATA_PATH)
udf_capitalize_lower = udf(lambda x:str(x).lower().capitalize(),StringType())
spark = create_spark_session()
df_demo = spark.read.format("csv").option("delimiter", ";").option("header", "true").option("encoding", "UTF-8").load(input_log_data_file)
df_demo = df_demo.withColumnRenamed("State Code","state_code").withColumnRenamed("Median Age","median_age").withColumnRenamed("City","city").withColumnRenamed("Total Population","population")
df_demo = df_demo.select("city","state_code","median_age","population")
df_state = spark.read.parquet(s3+"data/processed/codes/us_state")
df_demo = df_demo.join(df_state,["state_code"])
df_demo = df_demo.withColumn("city",lower(trim(df_demo.city)))
df_demo.write.mode("overwrite").parquet(s3 + 'data/processed/city/')