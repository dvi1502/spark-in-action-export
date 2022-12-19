"""
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               StringType,IntegerType)

def createDataframe(spark):
    # Create the schema
    schema = StructType([StructField('fname', StringType(), False),
                         StructField('lname', StringType(), False),
                         StructField('id', IntegerType(), False),
                         StructField('score', IntegerType(), False)])
    # data to create a dataframe
    data = [
        ("Matei", "Zaharia", 34, 456),
        ("Jean-Georges", "Perrin", 23, 3),
        ("Jacek", "Laskowski", 12, 758),
        ("Holden", "Karau", 31, 369)
    ]
    return spark.createDataFrame(data, schema)




warehouse_location = os.path.abspath('/tmp/spark-warehouse')

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession \
        .builder \
        .appName("SparkByExamples.com") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    df = createDataframe(spark)
    df.write.mode('overwrite').saveAsTable("ch17_lab920")

    sampleDF = spark.sql("select * from ch17_lab920")
    sampleDF.show(truncate=False)
    sampleDF.printSchema()


    # main(spark)
    spark.stop()


