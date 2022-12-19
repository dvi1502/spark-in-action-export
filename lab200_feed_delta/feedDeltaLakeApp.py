"""
 Ingestion the 'Grand Debate' files to Delta Lake.

 @author rambabu.posa
"""
import logging
import os

import pyspark
from delta import *
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField, StringType,
                               TimestampType, DoubleType, BooleanType)


def main(spark):

    absolute_file_path = "../data/france_grand_debat/20190302 EVENTS.json"

    # Create the schema
    schema = StructType([StructField('authorId', StringType(), False),
                         StructField('authorType', StringType(), True),
                         StructField('authorZipCode', StringType(), True),
                         StructField('body', StringType(), True),
                         StructField('createdAt', TimestampType(), False),
                         StructField('enabled', BooleanType(), True),
                         StructField('endAt', TimestampType(), True),
                         StructField('fullAddress', StringType(), True),
                         StructField('id', StringType(), False),
                         StructField('lat', DoubleType(), True),
                         StructField('link', StringType(), True),
                         StructField('lng', DoubleType(), True),
                         StructField('startAt', TimestampType(), False),
                         StructField('title', StringType(), True),
                         StructField('updatedAt', TimestampType(), True),
                         StructField('url', StringType(), True)])

    # Reads a JSON file, called 20190302 EVENTS.json,
    # stores it in a dataframe
    df = spark.read\
        .format("json") \
        .schema(schema) \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .load(absolute_file_path)

    df = df.withColumn("authorZipCode", F.col("authorZipCode").cast("int")) \
        .withColumn("authorZipCode", F.when(F.col("authorZipCode")< F.lit(1000), None).otherwise(F.col("authorZipCode"))) \
        .withColumn("authorZipCode", F.when(F.col("authorZipCode") >= F.lit(99999), None).otherwise(F.col("authorZipCode"))) \
        .withColumn("authorDept", F.expr("int(authorZipCode / 1000)"))

    df.show(25)
    df.printSchema()

    logging.warning(f"{df.count()} rows updated.")

    df.write\
        .format("delta") \
        .mode("overwrite") \
        .save("/tmp/delta_grand_debat_events")


if __name__ == "__main__":
    #
    # ******************************************************************************************************************
    # To set up a Python project (for example, for unit testing), you can install Delta Lake using
    # pip install delta-spark==2.2.0 and then configure the SparkSession with the
    # configure_spark_with_delta_pip() utility function in Delta Lake.
    # https://docs.delta.io/latest/quick-start.html#create-a-table
    # ******************************************************************************************************************
    #
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()




