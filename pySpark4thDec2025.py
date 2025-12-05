if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    rdd = spark.sparkContext.textFile("file:///home/takeo/data/sparkTest.txt")
    print(rdd.collect())