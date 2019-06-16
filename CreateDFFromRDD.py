from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql import Row
from sys import argv



"""Below is the spark session created to access spark functionalities """
spark = SparkSession.builder.master("local[1]").appName("TestSQL").getOrCreate()
# df1= spark.read.csv(argv[1])
#print(df1.first())

"""Example of temp view being shared between two sparksessions"""
# df1.createOrReplaceTempView("people")
# df1.createGlobalTempView("people")
# sqldf = spark.sql("select * from people")
# sqldf.show()
# spark2 = SparkSession.builder.master("local[1]").appName("TestSQL2").getOrCreate()
# sqldf2 = spark2.sql("select * from global_temp.people")
# sqldf2.show()
# spark.stop()
# spark2.stop()

""" 
Example of creating DF from RDD. There are two ways to create DF from RDD 
1. Infering the schema 
2. Applying schema on top of RDD

"""
print("This is the 1st method of creating an RDD")
#conf = SparkConf().setMaster("local[1]").setAppName("RDD to DF")
#sc = SparkContext(conf=conf)
sc = spark.sparkContext
lines = sc.textFile(argv[1])
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0],age=int(p[1])))
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT NAME FROM people where age >=18 and age <=28")


teenNames = teenagers.rdd.map(lambda x : "Name: "+x.name)
for name in teenNames:
    print(name)


spark.stop()