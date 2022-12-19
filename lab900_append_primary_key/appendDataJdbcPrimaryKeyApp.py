"""
  Appends content of a dataframe to a PostgreSQL database.
  Check for additional information in the README.md file in the same repository.

  @author rambabu.posa
"""
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

def main(spark):
    df = createDataframe(spark)
    df.show(truncate=False)

    # Write in a table called ch17_lab900_pkey
    df.write.mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.23.0.1:15432/spark_labs") \
        .option("dbtable", "ch17_lab900_pkey") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "postgres") \
        .option("password", "1234") \
        .save()

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.23.0.1:15432/spark_labs") \
        .option("dbtable", "ch17_lab900_pkey") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "postgres") \
        .option("password", "1234") \
        .load()
    df.show()
    df.printSchema()

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession\
        .builder \
        .appName("Addition") \
        .master("local[*]")\
        .getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()


