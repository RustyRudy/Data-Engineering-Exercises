from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType
import pyspark
from pyspark.sql.types import *
from pyspark.sql import functions
from pyspark.sql.window import Window
from pyspark.sql.functions import*

def classProject6CovidDataAnalysis(spark:SparkSession):
    df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
        .csv("/data/spark/test/covidCases.txt")
    #df.printSchema()
    #df.show()
    print("-------------------------------------Section1-----------------------------------------------------------")
    print("Rename the column infection_case to infection_source")
    renamedDf = df.withColumnRenamed("infection_case", "infection_source")
    #renamedDf.show()
    print("Select only following columns 'Province','city',infection_source,'confirmed'")
    #renamedDf.select("Province","city", "infection_source", "confirmed").show()
    print("Change the datatype of confirmed column to integer")
    changedDatatypeDf = renamedDf.withColumn("confirmed",col("confirmed").cast("integer"))
    #changedDatatypeDf.printSchema()
    print("Return the TotalConfirmed and MaxFromOneConfirmedCase for each province,city pair")
    aggDf = changedDatatypeDf.groupBy("province", "city") \
        .agg(sum("confirmed").alias("TotalConfirmed"),\
             max("confirmed").alias("MaxFromOneConfirmedCase"))
    #aggDf.show()
    print("Sort the output in asc order on the basis of confirmed")
    sortedAggDf = aggDf.sort(aggDf.TotalConfirmed.asc(), aggDf.MaxFromOneConfirmedCase.asc())
    #sortedAggDf.show()
    sortedDf = changedDatatypeDf.sort(changedDatatypeDf.confirmed.asc())
    #sortedDf.show()
    print("-------------------------------------Section2-----------------------------------------------------------")
    print("Return the top 2 provinces on the basis of confirmed cases")
    #descSortAggDf = aggDf.sort(aggDf.TotalConfirmed.desc(), aggDf.MaxFromOneConfirmedCase.desc())
    #descSortAggDf.show()
    windowSpec = Window.orderBy(col("MaxFromOneConfirmedCase").desc())
    #sortedAggDf.withColumn("Highest Infected Province", dense_rank().over(windowSpec)).show(2)
    print("-------------------------------------Section3-----------------------------------------------------------")
    print("Return the details only for ‘Daegu’ as province name where confirmed cases are more than 10")
    #sortedDf.filter((sortedDf.province == "Daegu") & (sortedDf.confirmed > 10)).show()
    print("Select the columns other than latitude, longitude and case_id")
    drop_list = ['latitude', 'longitude', 'case_id']
    selectiveDf = sortedDf.select([column for column in sortedDf.columns if column not in drop_list])
    selectiveDf.show()
