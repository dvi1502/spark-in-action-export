"""
  Export data into Delta Lake

  @author rambabu.posa
"""
import os
import logging
from pyspark.sql import (SparkSession, functions as F)
import pyspark
from delta import *


def main(spark):
    absolute_file_path = "./data/france_population_dept/population_dept.csv"

    # Reads a CSV file, called population_dept.csv,
    # stores it in a dataframe
    df = spark.read.format("csv") \
        .option("inferSchema", True) \
        .option("header", True) \
        .load(absolute_file_path)

    df = df.withColumn("Code département",
              F.when(F.col("Code département") == F.lit("2A"), "20")
              .otherwise(F.col("Code département"))) \
        .withColumn("Code département",
              F.when(F.col("Code département") == F.lit("2B"), "20")
               .otherwise(F.col("Code département"))) \
        .withColumn("Code département",
              F.col("Code département").cast("int")) \
        .withColumn("Population municipale",
              F.regexp_replace(F.col("Population municipale"), ",", "")) \
        .withColumn("Population municipale",
              F.col("Population municipale").cast("int")) \
        .withColumn("Population totale",
              F.regexp_replace(F.col("Population totale"), ",", "")) \
        .withColumn("Population totale",
              F.col("Population totale").cast("int")) \
        .drop("_c9")

    df.show(25)
    df.printSchema()

    df.write.format("delta") \
        .mode("overwrite") \
        .option("encoding", "utf-8") \
        .save("/tmp/delta_france_population")

    logging.warning("{} rows updated.".format(df.count()))

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

