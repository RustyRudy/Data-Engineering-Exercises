from pyspark.sql import SparkSession
from pyspark.sql.types import *


def classProj5(spark: SparkSession):
    df = spark.read.json("file:///home/takeo/pycharmprojects/PythonProject/books.json")
    print("Rows in the books.json file:", df.count())
    print("Distinct Rows in the books.json file:", df.distinct().count())
    df_dropDuplicates = df.dropDuplicates()
    #gotta get that contri
    print(" Rows in the books.json file after dropping duplis:", df_dropDuplicates.count())
