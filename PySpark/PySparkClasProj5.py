from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit


def classProj5(spark: SparkSession):
    print("-----------------------------------------------------------------------------------------")
    df = spark.read.json("file:///home/takeo/pycharmprojects/PythonProject/books.json")
    #print("Rows in the books.json file:", df.count())
    #print("Distinct Rows in the books.json file:", df.distinct().count())
    df_dropDuplicates = df.dropDuplicates()
    print(" Rows in the books.json file after dropping duplis:", df_dropDuplicates.count())
    print("-----------ODD HOURS--------------")
    #df.select("title", when(df.title != 'ODD HOURS', 1).otherwise(0).alias("newHours")).show(10)
    print("-----------Title has 'THE'--------------")
    #df.select("author","title", when(df.title.like("%THE%"), "TRUE")\
    #          .otherwise("FALSE").alias("universal")).show(10)
    print("-----------Select substring of author from 1 to 3 and alias as newTitle1--------------")
    #df.select(df.author.substr(1, 3).alias("newTitle1")).show(5)
    print("-----------Select substring of author from 3 to 6 and alias as newTitle2--------------")
    #df.select(df.author.substr(3, 6).alias("newTitle2")).show(5)
    print("-----------Show and Count all entries in title, author, rank, price columns--------------")
    # df.groupby("title").count().show()
    # df.groupby("author").count().show()
    # df.groupby("rank").count().show()
    # df.groupby("price").count().show()
    # df.select("title", "author", "rank", "price").show()
    print("-----------Show rows with for specified authors: John Sandford, Emily Griffin--------------")
    #df.filter((df.author == "John Sandford") | (df.author == "Emily Griffin")).select("title", "author").show(10)
    #df.filter((df.author == "John Sandford") | (df.author == "Emily Griffin")).show(10)
    print("-----------Select author, title when title startsWith THE title ends With IN--------------")
    #df.filter((df.title.like("THE%")) & (df.title.like("%IN"))).select("title", "author").show(30)
    #or using startsWith, endsWith
    #df.filter((df.title.startswith("THE")) & (df.title.endswith("IN"))).select("title", "author").show(30)
    print("-----------Update column 'amazon_product_url' with 'URL'--------------")
    #df.withColumnRenamed("amazon_product_url","url").show(2)
    print("-----------Drop columns publisher and published_date--------------")
    #df.drop("publisher", "published_date").show(5)
    print("-----------Group by author, count the books of the authors in the groups--------------")
    #df.groupby("author").count().show()
    print("-----------Filtering entries of title Only keeps records having value 'THE HOST--------------")
    df.filter((df.title == "THE HOST")).select("title", "author", "amazon_product_url").show(20)

