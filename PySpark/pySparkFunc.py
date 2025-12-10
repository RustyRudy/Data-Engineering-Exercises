if __name__ == '__main__':
    #From Takeo PySpark-functions-1-D
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import col, lit
    from pyspark.sql.types import StructType, StructField
    from pyspark.sql.types import StringType, IntegerType, ArrayType

    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()

    data = [("James","Smith","USA","CA"), ("Michael","Rose","USA","NY"), ("Robert","Williams","USA","CA"), ("Maria","Jones","USA","FL") ]
    columns = ["firstname","lastname","country","state"]
    df = spark.createDataFrame(data = data, schema = columns)

    ##the commented out lines still work but i just wanted the view
    ##the function i was currently working on

    # df.show(truncate=False)

    ## Select Single & Multiple Columns From PySpark
    # df.select("firstname", "lastname").show()
    #
    # df.select(df.firstname, df.lastname).show()
    #
    # df.select(df["firstname"], df["lastname"]).show()

    # df.select(col("firstname"), col("lastname")).show()
    ## Select all
    # df.select("*").show()

    # Select Columns by Index

    ## Selects first 3 columns and top 3 rows
    # df.select(df.columns[:3]).show(3)
    ## Selects columns 2 to 4 and top 3 rows
    # df.select(df.columns[2:4]).show(3)

    # #Select Nested Struct Columns from PySpark

    # data = [(("James", None, "Smith"), "OH", "M"), (("Anna", "Rose", ""), "NY", "F"),
    #         (("Julia", "", "Williams"), "OH", "F"), (("Maria", "Anne", "Jones"), "NY", "M"),
    #         (("Jen", "Mary", "Brown"), "NY", "M"), (("Mike", "Mary", "Williams"), "OH", "M")]
    #
    # schema = StructType([StructField('name', StructType(
    #     [StructField('firstname', StringType(), True), StructField('middlename', StringType(), True),
    #      StructField('lastname', StringType(), True)])), StructField('state', StringType(), True),
    #                      StructField('gender', StringType(), True)])
    # df2 = spark.createDataFrame(data=data, schema=schema)
    # df2.printSchema()
    # df2.show(truncate=False)  # shows all columns
    #
    # df2.select("name").show(truncate=False)
    #
    # df2.select("name.firstname", "name.lastname").show(truncate=False)

    # withColumn()
    # data = [('James', '', 'Smith', '1991-04-01', 'M', 3000), ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
    #         ('Robert', '', 'Williams', '1978-09-05', 'M', 4000), ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
    #         ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]
    # columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    # df = spark.createDataFrame(data=data, schema=columns)
    #
    # # Change DataType using PySpark withColumn()
    # ddf = df.withColumn("salary", col("salary").cast("Double"))
    #
    # # Update The Value of an Existing Column
    # udf = df.withColumn("salary", col("salary") * 100)
    #
    # # Create a Column from an Existing
    # ncol = df.withColumn("CopiedColumn", col("salary") * -1)
    # ncol.show()
    #
    # # Add a New Column using withColumn() with constant value
    # df.withColumn("Country", lit("USA")).show()
    #
    # # Rename Column Name
    # df.withColumnRenamed("gender", "sex").show(truncate=False)

    ##filter()

    data = [(("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
            (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
            (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
            (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
            (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
            (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")]

    schema = StructType([StructField('name', StructType(
        [StructField('firstname', StringType(), True), StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)])), StructField('languages', ArrayType(StringType()), True),
                         StructField('state', StringType(), True), StructField('gender', StringType(), True)])

    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)

    # ## DataFrame filter() with Column Condition
    # df.filter(df.state == "OH").show(truncate=False)
    # ## not equals condition
    # df.filter(df.state != "OH").show(truncate=False)
    # #or
    # df.filter(~(df.state == "OH")).show(truncate=False)
    # #using col
    # df.filter(col("state") == "OH").show(truncate=False)

    # #DataFrame filter() with SQL Expression
    # # Using SQL Expression
    # df.filter("gender == 'M'").show()
    # # For not equal
    # df.filter("gender != 'M'").show()
    # df.filter("gender <> 'M'").show()
    #
    # #PySpark Filter with Multiple Conditions
    # df.filter((df.state == "OH") & (df.gender == "M")).show(truncate=False)

    #Filter Based on List Values
    li = ["OH", "CA", "DE"]
    df.filter(df.state.isin(li)).show()
    # Filter NOT IS IN List values
    # These show all records with NY (NY is not part of the list)
    df.filter(~df.state.isin(li)).show()
    df.filter(df.state.isin(li) == False).show()


