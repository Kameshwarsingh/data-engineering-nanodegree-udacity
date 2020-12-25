import logging
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number

output_data = "s3://kamesh-capstone/"
s3 = "s3://kamesh-capstone/"


IMMIGRATION="data/processed/immigration/"
IMMIGRANT="data/processed/immigrant/"
IMMIGRANT_CITY="data/processed/immigration_demographics/"

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

spark.sparkContext.setLogLevel("WARN")



def check(path, table):
    spark = create_spark_session()
    df = spark.read.parquet(path).filter("i94dt = '{}'".format(year_month))
    if len(df.columns) > 0 and df.count() > 0:
        self.log.info(e)
    else:
        raise ValueError("Data quality checks failed")



check(s3+IMMIGRATION,"immigration")
check(s3+IMMIGRANT,"immigrant")
#check(s3+IMMIGRANT_CITY,"immigration_demographics")