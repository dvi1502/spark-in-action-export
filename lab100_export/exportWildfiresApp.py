"""
 Export data.

 @author rambabu.posa
"""
import logging
from pyspark.sql import (SparkSession, functions as F)
from download import download

def downloadWildfiresDatafiles():
        logging.info("-> downloadWildfiresDatafiles()")
        # Download the MODIS data file
        fromFile = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/" + modis_file
        toFile = tmp_storage + "/" + modis_file
        if not download(fromFile, toFile):
                return False
        # Download the VIIRS data file
        fromFile = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/" + viirs_file
        toFile = tmp_storage + "/" + viirs_file
        if not download(fromFile, toFile):
                return False
        return True

def load_viirs(spark, filelist_viirs):
        viirsDf = spark.read \
                .format("csv") \
                .option("header", True) \
                .option("inferSchema", True) \
                .load(filelist_viirs)\
                .withColumn("acq_time_min", F.expr("acq_time % 100")) \
                .withColumn("acq_time_hr", F.expr("int(acq_time / 100)")) \
                .withColumn("acq_time2", F.unix_timestamp(F.col("acq_date"))) \
                .withColumn("acq_time3", F.expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")) \
                .withColumn("acq_datetime", F.from_unixtime(F.col("acq_time3"))) \
                .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3") \
                .withColumnRenamed("confidence", "confidence_level") \
                .withColumn("brightness", F.lit(None)) \
                .withColumn("bright_t31", F.lit(None))

        viirsDf.show()
        viirsDf.printSchema()
        return viirsDf

def load_modis(spark, filelist_modis):
        # Format the MODIS dataset
        low = 40
        high = 100

        modisDf = spark.read \
                .format("csv") \
                .option("header", True) \
                .option("inferSchema", True) \
                .load(filelist_modis)\
                .withColumn("acq_time_min", F.expr("acq_time % 100")) \
                .withColumn("acq_time_hr", F.expr("int(acq_time / 100)")) \
                .withColumn("acq_time2", F.unix_timestamp(F.col("acq_date"))) \
                .withColumn("acq_time3", F.expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")) \
                .withColumn("acq_datetime", F.from_unixtime(F.col("acq_time3"))) \
                .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3") \
                .withColumn("confidence_level", F.when(F.col("confidence") <= F.lit(low), "low")
                            .when((F.col("confidence") > F.lit(low)) & (F.col("confidence") < F.lit(high)), "nominal")
                            .when(F.isnull(F.col("confidence")), "high")
                            .otherwise(F.col("confidence"))) \
                .drop("confidence") \
                .withColumn("bright_ti4", F.lit(None)) \
                .withColumn("bright_ti5", F.lit(None))

        modisDf.show()
        modisDf.printSchema()
        return modisDf


        # viirsDf2 = viirsDf \
        #         .withColumn("acq_time_min", F.expr("acq_time % 100")) \
        #         .withColumn("acq_time_hr", F.expr("int(acq_time / 100)")) \
        #         .withColumn("acq_time2", F.unix_timestamp(F.col("acq_date"))) \
        #         .withColumn("acq_time3", F.expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")) \
        #         .withColumn("acq_datetime", F.from_unixtime(F.col("acq_time3"))) \
        #         .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3") \
        #         .withColumnRenamed("confidence", "confidence_level") \
        #         .withColumn("brightness", F.lit(None)) \
        #         .withColumn("bright_t31", F.lit(None))
        #
        # viirsDf2.show()
        # viirsDf2.printSchema()

        # # This piece of code shows the repartition by confidence level, so you
        # # can compare when you convert the confidence as a % to a level for the
        # # MODIS dataset.
        # df = viirsDf2.groupBy("confidence_level").count()
        # count = viirsDf2.count()
        # df = df.withColumn("%", F.round(F.expr("100 / {} * count".format(count)), 2))
        # df.show()
        #
        # # Format the MODIS dataset
        # low = 40
        # high = 100
        #
        # modisDf = spark.read.format("csv") \
        #         .option("header", True) \
        #         .option("inferSchema", True) \
        #         .load("/tmp/{}".format(modis_file)) \
        #         .withColumn("acq_time_min", F.expr("acq_time % 100")) \
        #         .withColumn("acq_time_hr", F.expr("int(acq_time / 100)")) \
        #         .withColumn("acq_time2", F.unix_timestamp(F.col("acq_date"))) \
        #         .withColumn("acq_time3", F.expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")) \
        #         .withColumn("acq_datetime", F.from_unixtime(F.col("acq_time3"))) \
        #         .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3") \
        #         .withColumn("confidence_level", F.when(F.col("confidence") <= F.lit(low), "low")
        #                     .when((F.col("confidence") > F.lit(low)) & (F.col("confidence") < F.lit(high)), "nominal")
        #                     .when(F.isnull(F.col("confidence")), "high")
        #                     .otherwise(F.col("confidence"))) \
        #         .drop("confidence") \
        #         .withColumn("bright_ti4", F.lit(None)) \
        #         .withColumn("bright_ti5", F.lit(None))
        #
        # modisDf.show()
        # modisDf.printSchema()
        #
        # # This piece of code shows the repartition by confidence level, so you
        # # can compare when you convert the confidence as a % to a level for the
        # # MODIS dataset.
        # df = modisDf.groupBy("confidence_level").count()
        # count = modisDf.count()
        # df = df.withColumn("%", F.round(F.expr("100 / {} * count".format(count)), 2))
        # df.show()
        # df.printSchema()

def wrireDF(wildfireDf,dest_dir):

        logging.info("# of partitions: {}".format(wildfireDf.rdd.getNumPartitions()))

        wildfireDf.write\
                .format("parquet") \
                .mode("overwrite") \
                .save("{0}/fires_parquet".format(dest_dir))

        outputDf = wildfireDf\
                .filter("confidence_level = 'high'") \
                .repartition(1)

        outputDf.write.format("csv") \
                .option("header", True) \
                .mode("overwrite") \
                .save("{0}/high_confidence_fires_csv".format(dest_dir))




url_viirs = "https://firms.modaps.eosdis.nasa.gov/data/country/zips/viirs-snpp_2021_all_countries.zip"
url_modis = "https://firms.modaps.eosdis.nasa.gov/data/country/zips/modis_2021_all_countries.zip"

# url_viirs = "http://172.22.0.1:8000/viirs_redirect.html"
# url_modis = "http://172.22.0.1:8000/modis_redirect.html"


if __name__ == "__main__":

        spark = SparkSession\
                .builder\
                .appName("Wildfire data pipeline") \
                .master("local[*]")\
                .getOrCreate()

        spark.sparkContext.setLogLevel("warn")

        filelist_viirs = download(url_viirs,"/tmp/nasa/viirs")
        viirsDf = load_viirs(spark, filelist_viirs)
        # This piece of code shows the repartition by confidence level, so you
        # can compare when you convert the confidence as a % to a level for the
        # MODIS dataset.
        df = viirsDf.groupBy("confidence_level").count()
        count = viirsDf.count()
        df = df.withColumn("%", F.round(F.expr("100 / {} * count".format(count)), 2))
        df.show()
        df.printSchema()


        filelist_modis = download(url_modis,"/tmp/nasa/modis")
        modisDf = load_modis(spark, filelist_modis)
        # This piece of code shows the repartition by confidence level, so you
        # can compare when you convert the confidence as a % to a level for the
        # MODIS dataset.
        df = modisDf.groupBy("confidence_level").count()
        count = modisDf.count()
        df = df.withColumn("%", F.round(F.expr("100 / {} * count".format(count)), 2))
        df.show()
        df.printSchema()

        wildfireDf = viirsDf.unionByName(modisDf)
        wildfireDf.show()
        wildfireDf.printSchema()

        wrireDF(wildfireDf,"/tmp/nasa")

        spark.stop()
