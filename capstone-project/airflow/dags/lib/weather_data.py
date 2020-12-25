from pyspark.sql.functions import year, month, dayofmonth, udf
from pyspark.sql.types import StringType
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number

output_data = "s3://kamesh-capstone/"
s3 = "s3://kamesh-capstone/"


WEATHER_DATA_PATH = "data/raw/globaltemperatures/GlobalLandTemperaturesByCity.csv"


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
input_log_data_file = os.path.join(s3, WEATHER_DATA_PATH)
udf_capitalize_lower = udf(lambda x:str(x).lower().capitalize(),StringType())
df_weather = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(input_log_data_file)
df_weather_us = df_weather.filter("Country == 'United States'")
df_weather_us = df_weather_us.select(df_weather_us.dt.alias("date"),
                year("dt").alias("Year"),
                month("dt").alias("Month"),
                dayofmonth("dt").alias("DayOfMonth"),
                df_weather_us.AverageTemperature,
                df_weather_us.AverageTemperatureUncertainty,
                udf_capitalize_lower("City").alias("city"),
                df_weather_us.Latitude,
                df_weather_us.Longitude)
df_weather_us.repartition("Month").write.partitionBy("Month").mode("overwrite").parquet(s3 + 'data/processed/weather/')
