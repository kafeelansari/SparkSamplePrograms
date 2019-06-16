"""This python file is to create and understand the concepts of spark RDD and pyspark API"""

from pyspark import SparkConf,SparkContext
from sys import argv



conf = SparkConf().setMaster("local[1]").setAppName("TestApp").set("spark.executor.memory", "16G")
sc = SparkContext(conf=conf)
"""textFile() is the function which returns one record per line in each file"""
rdd = sc.textFile(argv[1])
"""wholeTextFiles returns the <filename,content_of_file>"""
rdd2= sc.wholeTextFiles(argv[1])
print(rdd.first())
print(rdd2.first())
sc.stop()
sc2 = SparkContext(conf=conf)
rdd_sc2 = sc2.textFile("file:///home/sylar/nohup.out")
print(rdd_sc2.first())
sc2.stop()

