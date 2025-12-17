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
    import pySparkClassProject6

    spark: SparkSession = SparkSession.builder.master("local[1]")\
        .appName("bootcamp.com").enableHiveSupport().getOrCreate()

    #Practicing Modularity
    # PySparkModulePractice.sqlQueriesDF(spark)
    #PySparkClasProj5.classProj5(spark)
    #pySparkHadoopHive.pySparkHadoopHive(spark)
    pySparkClassProject6.classProject6CovidDataAnalysis(spark)
