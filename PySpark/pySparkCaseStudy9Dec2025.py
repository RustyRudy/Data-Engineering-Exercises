if __name__ == '__main__':

    #Step 1: Setting Up PySpark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("MySparkApp").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    #Step 2:Loading the Data into an RDD
    data = ["1,101,5001,Laptop,Electronics,1000.0,1", "2,102,5002,Headphones,Electronics,50.0,2",
            "3,101,5003,Book,Books,20.0,3", "4,103,5004,Laptop,Electronics,1000.0,1",
            "5,102,5005,Chair,Furniture,150.0,1"]

    transactions_rdd = sc.parallelize(data)

    # #Step 3: Transformations on Flat RDDs
    # #a. `map()` Transformation
    # transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))
    # print(transactions_tuple_rdd.collect())
    # #b. `filter()` Transformation
    # high_quantity_rdd = transactions_tuple_rdd.filter(lambda x: int(x[6]) > 1)
    # print(high_quantity_rdd.collect())
    # #c. `flatMap()` Transformation
    # products_flat_rdd = transactions_tuple_rdd.flatMap(lambda x: [x[3]])
    # print(products_flat_rdd.collect())

    # #Step 4: Working with Pair RDDs
    # transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))
    # #a. Creating a Pair RDD
    # pair_rdd = transactions_tuple_rdd.map(lambda x: (x[1], (x[3], float(x[5]) * int(x[6]))))
    # print(pair_rdd.collect())
    # #b. `reduceByKey()` Transformation
    # customer_spending_rdd = pair_rdd.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda x, y: x + y)
    # print(customer_spending_rdd.collect())
    # #c. `groupByKey()` Transformation
    # customer_products_rdd = pair_rdd.map(lambda x: (x[0], x[1][0])).groupByKey().mapValues(list)
    # print(customer_products_rdd.collect())

    # #Step 5: Join Operations on Pair RDDs
    # transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))
    # pair_rdd = transactions_tuple_rdd.map(lambda x: (x[1], (x[3], float(x[5]) * int(x[6]))))
    # 
    # product_category_data = [('Laptop', 'Electronics'), ('Headphones', 'Electronics'), ('Book', 'Books'),
    #                          ('Chair', 'Furniture')]
    # product_category_rdd = sc.parallelize(product_category_data)
    # customer_product_category_rdd = pair_rdd.map(lambda x: (x[1][0], (x[0], x[1][1]))).join(product_category_rdd)
    # print(customer_product_category_rdd.collect())
    #
    # #Step 6: Actions to Collect Results
    # #a. Collect Total Spending Per Customer
    # customer_spending_rdd = pair_rdd.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda x, y: x + y)
    # total_spending = customer_spending_rdd.collect()
    # #b. Collect Products Purchased Per Customer
    # customer_products_rdd = pair_rdd.map(lambda x: (x[0], x[1][0])).groupByKey().mapValues(list)
    # products_per_customer = customer_products_rdd.collect()
    # #c. Collect Product-Category Join Results
    # product_category_info = customer_product_category_rdd.collect()
    #
    # #Step 7: Save the Results
    # customer_spending_rdd.saveAsTextFile("file:///home/takeo/pySparkCollectedOutput/customer_spending")
    # customer_products_rdd.saveAsTextFile("file:///home/takeo/pySparkCollectedOutput/customer_products")
    # customer_product_category_rdd.saveAsTextFile("file:///home/takeo/pySparkCollectedOutput/customer_product_category")

    #Summary of Transformations Used:
    #- `map`: Convert CSV strings to tuples.
    # - `filter`: Filter transactions based on a condition.
    # - `flatMap`: Extract all product names into a flat list.
    # - Pair RDD creation: Converting flat RDDs into key-value pairs.
    # - `reduceByKey`: Calculate total spending per customer.
    # - `groupByKey`: Collect a list of all products purchased by each customer.
    # - `join`: Combine product data with category information.
