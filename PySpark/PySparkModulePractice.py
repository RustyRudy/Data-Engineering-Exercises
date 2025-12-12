from pyspark.sql import SparkSession
#gotta get that contri

def sqlQueriesDF(spark: SparkSession):
    parDF = spark.read.parquet("file:///tmp/output/people.parquet")
    parDF.createOrReplaceTempView("ParquetTable")
    parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

    parkSQL.show()
