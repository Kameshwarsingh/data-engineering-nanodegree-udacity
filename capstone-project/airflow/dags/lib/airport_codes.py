from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import udf, regexp_replace,lower
from pyspark.sql.types import DoubleType, StringType

output_data = "s3://kamesh-capstone/"
s3 = "s3://kamesh-capstone/"

# Read in the data here
# Read in the data here
def parseUDFlat(coordinates):
    l=coordinates.strip().split(",",1)
    return float(l[0])

udf_parse_lat = udf(lambda x: parseUDFlat(x), DoubleType())

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
    
def parseUDFlog(coordinates):
    l=coordinates.strip().split(",",1)
    return float(l[1])

udf_parse_log = udf(lambda x: parseUDFlog(x), DoubleType())

def parse_state(iso_region):
    state = None
    if(iso_region is not None):
        l = iso_region
        l = iso_region.strip().split("-",1)
        state = l[1]
    return state
udf_parse_state = udf(lambda x:parse_state(x),StringType())
udf_capitalize_lower = udf(lambda x:str(x).lower().capitalize(),StringType())


AIRPORT_DATA_PATH = "data/raw/airportcode/airport-codes_csv.csv"
input_log_data_file = os.path.join(s3, AIRPORT_DATA_PATH)
spark = create_spark_session()
df_airport = spark.read.format("csv").option("header", "true").load(input_log_data_file)
df_airport = df_airport.filter("iso_country='US' and type!='closed' and iata_code!='null'")
df_airport = df_airport.withColumn("latitude",udf_parse_lat("coordinates")).withColumn("longitude",udf_parse_log("coordinates"))
df_airport = df_airport.withColumn("state",udf_parse_state("iso_region"))
df_airport = df_airport.withColumnRenamed("municipality","city").withColumnRenamed("iata_code","airport_code")
columns = ["ident","type","name","gps_code","airport_code","local_code","latitude","longitude"]
df_airport = df_airport.select(*columns)
df_us_ports = spark.read.parquet(s3+"data/processed/codes/us_ports").withColumnRenamed("port_code","airport_code")
df_immigration_airport = df_airport.join(df_us_ports,["airport_code"])
df_immigration_airport = df_immigration_airport.withColumn("city",lower(regexp_replace(df_immigration_airport.city,"\t","")))
df_immigration_airport.write.mode("overwrite").parquet(output_data + 'data/processed/airports/')
