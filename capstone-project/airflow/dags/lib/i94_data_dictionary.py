from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number
import pyspark.sql.functions as F

output_data = "s3://kamesh-capstone/"
s3 = "s3://kamesh-capstone/"


I94_CODES_DATA_PATH = "data/raw/codes/"
AIRPORT_FILE="i94prtl.txt"
COUNTRY_FILE="i94cntyl.txt"
STATE_FILE="i94addrl.txt"
MODELFILE = "i94model.txt"
VISAFILE = "i94visa.txt"

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
    
# process and clean data from i94 label description file
def process_state_codes():
    input_data_file = os.path.join(s3, I94_CODES_DATA_PATH+STATE_FILE)
    spark = create_spark_session()
    df_state = spark.read.format("csv").option("delimiter", "=").option("header", "False").load(input_data_file)
    df_state = df_state.withColumnRenamed("_c0", "state_code").withColumnRenamed("_c1", "state_name")
    df_state = df_state.withColumn("state_code", F.regexp_replace(df_state.state_code, "[^A-Z]", ""))
    df_state = df_state.withColumn("state_name", F.regexp_replace(df_state.state_name, "'", ""))
    df_state = df_state.withColumn("state_name", F.ltrim(F.rtrim(df_state.state_name)))
    df_state.write.mode("overwrite").parquet(s3 + 'data/processed/codes/us_state')
    return df_state


def process_country_codes():
    input_data_file = os.path.join(s3, I94_CODES_DATA_PATH + COUNTRY_FILE)
    spark = create_spark_session()
    df_country = spark.read.format("csv").option("delimiter", "=").option("header", "False").load(input_data_file)
    df_country = df_country.withColumnRenamed("_c0", "country_code").withColumnRenamed("_c1", "country_name")
    df_country = df_country.withColumn("country_name", F.regexp_replace(df_country.country_name, "'", ""))
    df_country = df_country.withColumn("country_name", F.ltrim(F.rtrim(df_country.country_name)))
    df_country = df_country.withColumn("country_code", F.ltrim(F.rtrim(df_country.country_code)))
    df_country = df_country.withColumn("country_name",
                                       F.regexp_replace(df_country.country_name, "^INVALID.*|Collapsed.*|No\ Country.*",
                                                        "INVALID"))
    df_country.write.mode("overwrite").parquet(s3 + 'data/processed/codes/country')
    return df_country


def process_airport_codes():

    #transform airport codes
    input_data_file = os.path.join(s3, I94_CODES_DATA_PATH + AIRPORT_FILE)
    spark = create_spark_session()
    df_airport = spark.read.format("csv").option("delimiter", "=").option("header", "False").load(input_data_file)
    df_airport = df_airport.withColumn("_c0", F.regexp_replace(df_airport._c0, "'", "")).withColumn("_c1",
                                                                                                  F.regexp_replace(
                                                                                                      df_airport._c1,
                                                                                                      "'", ""))
    split_col = F.split(df_airport._c1, ",")
    df_airport = df_airport.withColumn("city", split_col.getItem(0))
    df_airport = df_airport.withColumn("state_code", split_col.getItem(1))
    df_airport = df_airport.withColumnRenamed("_c0", "port_code")
    df_airport = df_airport.drop("_c1")
    df_airport = df_airport.withColumn("port_code",
                                       F.regexp_replace(df_airport.port_code, "[^A-Z]", "")).withColumn("city",
                                                                                                           F.ltrim(
                                                                                                               F.rtrim(
                                                                                                                   df_airport.city))).withColumn(
        "state_code", F.regexp_replace(df_airport.state_code, "[^A-Z]", ""))
    df_state = process_state_codes()
    df_airport = df_airport.join(df_state, "state_code")
    df_airport.write.mode("overwrite").parquet(s3 + 'data/processed/codes/us_ports')

def process_i94Model_codes():
    input_data_file = os.path.join(s3, I94_CODES_DATA_PATH + MODELFILE)
    spark = create_spark_session()
    df_i94mode = spark.read.format("csv").option("delimiter", "=").option("header", "False").load(input_data_file)

    df_i94mode = df_i94mode.withColumn("_c0", F.regexp_replace(df_i94mode._c0, "'", "")). \
        withColumn("_c1", F.regexp_replace(df_i94mode._c1, "[^A-Za-z]", ""))
    df_i94mode = df_i94mode.withColumnRenamed("_c0", "i94mode").withColumnRenamed("_c1", "mode")
    df_i94mode.write.mode("overwrite").parquet(s3+"data/processed/codes/i94mode")


def process_i94Visa_codes():
    input_data_file = os.path.join(s3, I94_CODES_DATA_PATH + VISAFILE)
    spark = create_spark_session()
    df_i94visa = spark.read.format("csv").option("delimiter", "=").option("header", "False").load(input_data_file)

    df_i94visa = df_i94visa.withColumn("_c0", F.regexp_replace(df_i94visa._c0, "'", "")). \
        withColumn("_c1", F.regexp_replace(df_i94visa._c1, "[^A-Za-z0-9]", ""))
    df_i94visa = df_i94visa.withColumnRenamed("_c0", "i94visa").withColumnRenamed("_c1", "visa")
    df_i94visa.write.mode("overwrite").parquet(s3+"data/processed/codes/i94visa")


process_airport_codes()
process_country_codes()
process_i94Model_codes()
process_i94Visa_codes()



