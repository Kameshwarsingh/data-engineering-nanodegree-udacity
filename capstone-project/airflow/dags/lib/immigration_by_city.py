from pyspark.sql.functions import udf, trim, lower
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number


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


spark = create_spark_session()
df_immigration_airport=spark.read.parquet(s3+"data/processed/airports/")
df_immigration_airport = df_immigration_airport.withColumn("city",lower(df_immigration_airport.city))
df_demo = spark.read.parquet(s3 + 'data/processed/city/')
df_demo_airport = df_immigration_airport.join(df_demo,["city","state_code","state_name"])

df_immigration = spark.read.parquet(s3+"data/processed/immigration/").filter("i94dt=='{0}'".format(year_month))
df_immigration = df_immigration.withColumnRenamed("port_of_entry","airport_code")
df_demo_airport = df_demo_airport.drop("state_code","state_name")
df_immigration_demo = df_immigration.join(df_demo_airport,["airport_code"]).\
selectExpr("cicid","arrival_date","departure_date","airport_code","name","city","state_code","state_name","population","median_age","i94dt")

df_immigrant = spark.read.parquet(s3+"data/processed/immigrant/").filter("i94dt=='{0}'".format(year_month)).drop("i94dt")
df_immigrant_demographics = df_immigrant.join(df_immigration_demo,["cicid"]).\
selectExpr("cicid","age","birth_country","residence_country","gender","visatype","visa",\
           "i94dt","arrival_date","departure_date","airport_code","name","city","state_code",\
           "state_name","population","median_age")

df_immigrant_demographics.write.partitionBy("i94dt").mode("append").parquet(s3 + 'data/processed/immigration_demographics/')





