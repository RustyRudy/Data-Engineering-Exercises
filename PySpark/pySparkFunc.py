if __name__ == '__main__':
    #From Takeo PySpark-functions-1-D
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import col, lit
    from pyspark.sql.types import StructType, StructField
    from pyspark.sql.types import StringType, IntegerType, ArrayType
    import pyspark
    from pyspark.sql.types import *

    import PySparkModulePractice
    import PySparkClasProj5
    import pySparkHadoopHive


    spark: SparkSession = SparkSession.builder.master("local[1]")\
        .appName("bootcamp.com").enableHiveSupport().getOrCreate()

    # data = [("James","Smith","USA","CA"), ("Michael","Rose","USA","NY"), ("Robert","Williams","USA","CA"), ("Maria","Jones","USA","FL") ]
    # columns = ["firstname","lastname","country","state"]
    # df = spark.createDataFrame(data = data, schema = columns)
    #
    # ##the commented out lines still work but i just wanted the view
    # ##the function i was currently working on
    #
    # # df.show(truncate=False)
    #
    # ## Select Single & Multiple Columns From PySpark
    # # df.select("firstname", "lastname").show()
    # #
    # # df.select(df.firstname, df.lastname).show()
    # #
    # # df.select(df["firstname"], df["lastname"]).show()
    #
    # # df.select(col("firstname"), col("lastname")).show()
    # ## Select all
    # # df.select("*").show()
    #
    # # Select Columns by Index
    #
    # ## Selects first 3 columns and top 3 rows
    # # df.select(df.columns[:3]).show(3)
    # ## Selects columns 2 to 4 and top 3 rows
    # # df.select(df.columns[2:4]).show(3)
    #
    # # #Select Nested Struct Columns from PySpark
    #
    # # data = [(("James", None, "Smith"), "OH", "M"), (("Anna", "Rose", ""), "NY", "F"),
    # #         (("Julia", "", "Williams"), "OH", "F"), (("Maria", "Anne", "Jones"), "NY", "M"),
    # #         (("Jen", "Mary", "Brown"), "NY", "M"), (("Mike", "Mary", "Williams"), "OH", "M")]
    # #
    # # schema = StructType([StructField('name', StructType(
    # #     [StructField('firstname', StringType(), True), StructField('middlename', StringType(), True),
    # #      StructField('lastname', StringType(), True)])), StructField('state', StringType(), True),
    # #                      StructField('gender', StringType(), True)])
    # # df2 = spark.createDataFrame(data=data, schema=schema)
    # # df2.printSchema()
    # # df2.show(truncate=False)  # shows all columns
    # #
    # # df2.select("name").show(truncate=False)
    # #
    # # df2.select("name.firstname", "name.lastname").show(truncate=False)
    #
    # # withColumn()
    # # data = [('James', '', 'Smith', '1991-04-01', 'M', 3000), ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
    # #         ('Robert', '', 'Williams', '1978-09-05', 'M', 4000), ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
    # #         ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]
    # # columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    # # df = spark.createDataFrame(data=data, schema=columns)
    # #
    # # # Change DataType using PySpark withColumn()
    # # ddf = df.withColumn("salary", col("salary").cast("Double"))
    # #
    # # # Update The Value of an Existing Column
    # # udf = df.withColumn("salary", col("salary") * 100)
    # #
    # # # Create a Column from an Existing
    # # ncol = df.withColumn("CopiedColumn", col("salary") * -1)
    # # ncol.show()
    # #
    # # # Add a New Column using withColumn() with constant value
    # # df.withColumn("Country", lit("USA")).show()
    # #
    # # # Rename Column Name
    # # df.withColumnRenamed("gender", "sex").show(truncate=False)
    #
    # ##filter()
    #
    # # data = [(("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    # #         (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    # #         (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    # #         (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    # #         (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    # #         (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")]
    # #
    # # schema = StructType([StructField('name', StructType(
    # #     [StructField('firstname', StringType(), True), StructField('middlename', StringType(), True),
    # #      StructField('lastname', StringType(), True)])), StructField('languages', ArrayType(StringType()), True),
    # #                      StructField('state', StringType(), True), StructField('gender', StringType(), True)])
    # #
    # # df = spark.createDataFrame(data=data, schema=schema)
    # # df.printSchema()
    # # df.show(truncate=False)
    #
    # # ## DataFrame filter() with Column Condition
    # # df.filter(df.state == "OH").show(truncate=False)
    # # ## not equals condition
    # # df.filter(df.state != "OH").show(truncate=False)
    # # #or
    # # df.filter(~(df.state == "OH")).show(truncate=False)
    # # #using col
    # # df.filter(col("state") == "OH").show(truncate=False)
    #
    # # #DataFrame filter() with SQL Expression
    # # # Using SQL Expression
    # # df.filter("gender == 'M'").show()
    # # # For not equal
    # # df.filter("gender != 'M'").show()
    # # df.filter("gender <> 'M'").show()
    # #
    # # #PySpark Filter with Multiple Conditions
    # # df.filter((df.state == "OH") & (df.gender == "M")).show(truncate=False)
    #
    # # #Filter Based on List Values
    # # li = ["OH", "CA", "DE"]
    # # df.filter(df.state.isin(li)).show()
    # # # Filter NOT IS IN List values
    # # # These show all records with NY (NY is not part of the list)
    # # df.filter(~df.state.isin(li)).show()
    # # df.filter(df.state.isin(li) == False).show()
    #
    # # data = ["Project", "Gutenberg’s", "Alice’s", "Adventures", "in", "Wonderland", "Project", "Gutenberg’s",
    # #         "Adventures", "in", "Wonderland", "Project", "Gutenberg’s"]
    # #
    # # rdd = spark.sparkContext.parallelize(data)
    # #
    # # rdd2 = rdd.map(lambda x: (x, 1))
    # # for element in rdd2.collect():
    # #     print(element)
    # #
    # # data = [('James', 'Smith', 'M', 30), ('Anna', 'Rose', 'F', 41), ('Robert', 'Williams', 'M', 62), ]
    # # columns = ["firstname", "lastname", "gender", "salary"]
    # # df = spark.createDataFrame(data=data, schema=columns)
    # # df.show()
    # #
    # # # Refering columns by index.
    # # rdd2=df.rdd.map(lambda x: (x[0]+","+x[1],x[2],x[3]*2))
    # # df2 = rdd2.toDF(["name", "gender", "new_salary"])
    # # df2.show()
    # #
    # # # Referring Column Names
    # # rdd2=df.rdd.map(lambda x: (x["firstname"]+","+x["lastname"],x["gender"],x["salary"]*2))
    # # rdd2.collect()
    # #
    # # # Referring Column Names
    # # rdd2=df.rdd.map(lambda x: (x.firstname+","+x.lastname,x.gender,x.salary*2) )
    # # rdd2.collect()
    # #
    # # # By Calling function
    # # def func1(x):
    # #     firstName=x.firstname
    # #     lastName=x.lastname
    # #     name=firstName+","+lastName
    # #     gender=x.gender.lower()
    # #     salary=x.salary*2
    # #     return (name,gender,salary)
    # # rdd2 = df.rdd.map(lambda x: func1(x))
    # # rdd2.collect()
    # #
    # # # Foreach example
    # # def f(x): print(x)
    # # df.foreach(f)
    # #
    # # # Another example
    # # df.foreach(lambda x: print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2)) )
    #
    # #PySpark File formats
    # df = spark.read.csv("file:///home/takeo/pycharmprojects/PythonProject/zipcodes.csv")
    # #df.printSchema()
    # #df.show()
    #
    # #Using Header Record For Column Names
    # df2 = spark.read.option("header", True).csv("file:///home/takeo/pycharmprojects/PythonProject/zipcodes.csv")
    # #df2.printSchema()
    #
    # #Options While Reading CSV File
    # df3 = spark.read.options(delimiter=',').csv("file:///home/takeo/pycharmprojects/PythonProject/zipcodes.csv")
    # #df3.printSchema()
    #
    # #InferSchema
    # df4 = spark.read.options(inferSchema='True', delimiter=',') \
    #     .csv("file:///home/takeo/pycharmprojects/PythonProject/zipcodes.csv")
    # #df3.printSchema()
    # #another way
    # df4 = spark.read.option("inferSchema", True) \
    #     .option("delimiter", ",") \
    #     .csv("file:///home/takeo/pycharmprojects/PythonProject/zipcodes.csv")
    # #df4.printSchema()
    #
    # #header
    # df3 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    #     .csv("file:///home/takeo/pycharmprojects/PythonProject/zipcodes.csv")
    # #df3.printSchema()
    #
    # #Reading CSV files with a user-specified custom schema
    # schema = StructType() \
    #     .add("RecordNumber", IntegerType(), True) \
    #     .add("Zipcode", IntegerType(), True) \
    #     .add("ZipCodeType", StringType(), True) \
    #     .add("City", StringType(), True) \
    #     .add("State", StringType(), True) \
    #     .add("LocationType", StringType(), True) \
    #     .add("Lat", DoubleType(), True) \
    #     .add("Long", DoubleType(), True) \
    #     .add("Xaxis", IntegerType(), True) \
    #     .add("Yaxis", DoubleType(), True) \
    #     .add("Zaxis", DoubleType(), True) \
    #     .add("WorldRegion", StringType(), True) \
    #     .add("Country", StringType(), True) \
    #     .add("LocationText", StringType(),True) \
    #     .add("Location",StringType(),True) \
    #     .add("Decommisioned", BooleanType(), True) \
    #     .add("TaxReturnsFiled", StringType(), True) \
    #     .add("EstimatedPopulation", IntegerType(),True) \
    #     .add("TotalWages", IntegerType(), True) \
    #     .add("Notes", StringType(), True)
    # df_with_schema = spark.read.format("csv") \
    #     .option("header", True) \
    #     .schema(schema) \
    #     .load("file:///home/takeo/pycharmprojects/PythonProject/zipcodes.csv")
    # #df_with_schema.printSchema()
    #
    # #Saving Modes (Havent created the related dir so the following code wont work)
    # #df_with_schema.write.mode('overwrite').csv("file:///tmp/spark_output/zipcodes")
    # #//you can also use this
    # #df_with_schema.write.format("csv").mode('overwrite').save("file:///tmp/spark_output/zipcodes")

    # # Pyspark Write DataFrame to Parquet file format
    # data = [("James ", "", "Smith", "36636", "M", 3000),
    #         ("Michael ", "Rose", "", "40288", "M", 4000),
    #         ("Robert ", "", "Williams", "42114", "M", 4000),
    #         ("Maria ", "Anne", "Jones", "39192", "F", 4000),
    #         ("Jen", "Mary", "Brown", "", "F", -1)]
    # columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    # df = spark.createDataFrame(data, columns)
    # #df.write.mode('overwrite').parquet("file:///tmp/output/people.parquet")
    #
    # #Read Parquet file into DataFrame
    # parDF = spark.read.parquet("file:///tmp/output/people.parquet")
    # #parDF.show()
    #
    # #SQL queries DataFrame
    # parDF.createOrReplaceTempView("ParquetTable")
    # parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
    # #parkSQL.show()
    #
    # #ORC
    # #parDF.write.orc("file:///tmp/orc/data.orc")
    # df = spark.read.orc("file:///tmp/orc/data.orc")
    # #df.printSchema()
    # #df.show()
    #
    # #Run sql on Orc File
    # df.createOrReplaceTempView("ORCTable")
    # orcSQL = spark.sql("select firstname,dob from ORCTable where salary >= 4000 ")
    # #orcSQL.show()
    #
    # #JSON
    # # Read JSON file into dataframe
    # #parDF.write.json("file:///tmp/json/data.json")
    # df = spark.read.json("file:///tmp/json/data.json")
    # # df.printSchema()
    # # df.show()
    #
    #Practicing Modularity
    # PySparkModulePractice.sqlQueriesDF(spark)
    #PySparkClasProj5.classProj5(spark)
    pySparkHadoopHive.pySparkHadoopHive(spark)
