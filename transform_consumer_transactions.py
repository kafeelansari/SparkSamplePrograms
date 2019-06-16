from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,SparkSession
from sys import argv

if __name__=="__main__":
    sc = SparkContext(conf=SparkConf().setMaster("local[2]"))
    spark = SparkSession.builder.enableHiveSupport
    sql_cont = SQLContext(sparkContext=sc)
    csv_data = sql_cont.read\
        .csv(argv[1], header='true', escape='\"', multiLine=True)
        #.format("com.databricks.spark.csv")\
        # .options(header='true',inferschema='true')\
        # .options(quote='"')\
        # .options(escape='"')\
        # .options(wholeFile='true')\
        # .load('file:///home/sylar/Desktop/SKH_dataset/kafka/Consumer_Complaints.csv')

    # csv_data.printSchema()
    csv_data.registerTempTable("csvdata")
    distinct_product = sql_cont.sql("select distinct product from csvdata")
    #distinct_product.show()
    distinct_product.write..saveAsTable("product_2")


