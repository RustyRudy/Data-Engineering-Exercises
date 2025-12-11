if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import col, lit
    from pyspark.sql.types import StructType, StructField
    from pyspark.sql.types import StringType, IntegerType, ArrayType, DoubleType
    from pyspark.sql.functions import sum, avg, max, min, mean, count

    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()

    data = [("Ford Torino", 140, 3449, "US"),
            ("Chevrolet Monte Carlo", 150, 3761, "US"),
            ("BMW 2002", 113, 2234, "Europe")]

    schema = StructType([StructField('carr', StringType(), True),
                        StructField('horsepower', IntegerType(), True),
                        StructField('weight', IntegerType(), True),
                        StructField('origin', StringType(), True)])

    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)

    # df.withColumnRenamed("carr", "car").show(truncate=False)
    # df.withColumn("AvgWeight", lit(200)).show()
    # df.withColumn("kilowatt_power", 1000*col("horsepower")).show()

    df.withColumnRenamed("carr", "car") \
        .withColumn("AvgWeight", lit(200)) \
        .withColumn("kilowatt_power", 1000 * col("horsepower")).show(truncate=False)


    custDataToClean = [("Smith",23,5.3),
                       ("Rashmi",27,5.8),
                       ("Smith",23,5.3),
                       ("Payal",27,5.8),
                       ("Megha",27,5.4)]

    ToCleanSchema = StructType([StructField('Name', StringType(), True),
                         StructField('Age', IntegerType(), True),
                         StructField('Height', DoubleType(), True)])

    ToCleandf = spark.createDataFrame(data=custDataToClean, schema=ToCleanSchema)

    dropDupliDf = ToCleandf.dropDuplicates()
    print("Distinct count: " + str(dropDupliDf.count()))
    dropDupliDf.show(truncate=False)

    cityTemp = [ ("New York" , 10.0),
                 ("New York" , 12.0),
                 ("Los Angeles" , 20.0),
                 ("Los Angeles" , 22.0),
                 ("San Francisco" , 15.0),
                 ("San Francisco" , 18.0)]

    tempAnalysisSchema = StructType([StructField('city', StringType(), True),
                                     StructField('temperature', DoubleType(), True)])

    tempDf = spark.createDataFrame(data=cityTemp, schema=tempAnalysisSchema)

    tempDf.groupBy("city")\
        .agg(avg("temperature").alias("avg_temperature"),
            sum("temperature").alias("total_temperature"),
            count("city").alias("num_measurements")) \
        .where(col("total_temperature") > 30) \
        .sort(tempDf.city.asc()).show(truncate=False)
