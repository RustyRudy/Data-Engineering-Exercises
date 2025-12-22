from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, array_contains
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType, LongType
import pyspark
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import*

def assignment2spark(spark: SparkSession):
    print("--------------------------------------Qs1-----------------------------------------------")
    dfColumns = StructType([
        StructField("dept_name", StringType(), True),
        StructField("dept_id", IntegerType(), True),
        StructField("salary", LongType(), True)
    ])

    df = spark.read.csv("/data/test/sparkAssignment2/department.txt", schema=dfColumns)
    #df.printSchema()
    #df.show()
    #df.write.format("hive").mode("overwrite").saveAsTable("sparkassignment2.department")
    #df2 = spark.sql("select * from sparkassignment2.department")
    #df2.show()
    #doubleSalaryDf = df2.withColumn("doubleSalary", col("salary") * 2)
    #doubleSalaryDf.show()
    #doubleSalaryDf.write.mode("overwrite").parquet("/data/test/sparkAssignment2/department.parquet")
    #df3 = spark.read.parquet("/data/test/sparkAssignment2/department.parquet")
    #df3.show()

    print("--------------------------------------Qs2-----------------------------------------------")
    JDf = spark.read.json("/data/test/sparkAssignment2/student.json")
    #JDf.select("name.firstname", "gender").filter((array_contains(col("languages"), "Java")) & (col("state") != "OH")).show()

    print("--------------------------------------Qs3-----------------------------------------------")
    # EDf = spark.read.json("/data/test/sparkAssignment2/employee.json")
    # #EDf.show()
    # distinctDf = EDf.distinct()
    # #distinctDf.show()
    # spark.conf.set("hive.exec.dynamic.partition", "true")
    # spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    # distinctDf.write.mode("overwrite").partitionBy("department").orc("/data/test/sparkAssignment2/employee.orc")
    # orcDf = spark.read.orc("/data/test/sparkAssignment2/employee.orc")
    # orcDf.printSchema()
    # orcDf.show()
    # meanDf = orcDf.groupBy("department").agg(avg("salary").alias("mean salary")).orderBy(desc("mean salary"))
    # meanDf.show()

    print("--------------------------------------Qs4-----------------------------------------------")
    # empDf = spark.read.json("/data/test/sparkAssignment2/employee4.json")
    # depDf = spark.read.json("/data/test/sparkAssignment2/department4.json")
    # empDf.show()
    # depDf.show()
    # joinedDf =  empDf.join(depDf, empDf.emp_dept_id == depDf.dept_id, "inner")
    # joinedDf.show()
    # resultDf = joinedDf.groupBy("dept_name") \
    #             .agg(max("salary").alias("max_salary"), count("emp_id").alias("number_of_employees"))
    # resultDf.show()
    # spark.conf.set("hive.exec.dynamic.partition", "true")
    # spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    # resultDf.write.mode("overwrite").partitionBy("dept_name").format("parquet")\
    #     .saveAsTable("sparkassignment2.department")
    # hiveDf = spark.sql("select * from sparkassignment2.department")
    # #hiveDf.printSchema()
    # spark.sql("describe formatted sparkassignment2.department").show()
    # hiveDf.show()

    print("--------------------------------------Qs5-----------------------------------------------")
    sampleZipDf = spark.read.options(header = 'True', inferSchema = 'True', delimiter = ',') \
        .csv("/data/test/sparkAssignment2/simple-zipcodes.txt")
    print("Original sample")
    sampleZipDf.show()
    print("50% of the OG sample")
    sampleZipDf.limit(int(sampleZipDf.count()/2)).show()
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sampleZipDf.write.option("maxRecordsPerFile", 3).mode("overwrite").partitionBy("State", "City") \
        .saveAsTable("sparkassignment2.sample_zip_table")
    spark.sql("describe formatted sparkassignment2.sample_zip_table").show()
    hiveDf = spark.sql("select * from sparkassignment2.sample_zip_table")
    hiveDf.show()
    hiveDf.filter((col("state")!="AL") & (col("city")!="SPRINGVILLE")).show()




