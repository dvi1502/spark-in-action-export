"""
 Author/Organizer analytics.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
import pyspark
from delta import *

def main(spark):
    df = spark.read.format("delta") \
        .load("/tmp/delta_grand_debat_events")

    df = df.groupBy(F.col("authorType")) \
        .count() \
        .orderBy(F.col("authorType").asc_nulls_last())

    df.show(25, 0, False)
    df.printSchema()

if __name__ == "__main__":

    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
