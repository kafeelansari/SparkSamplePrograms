from spark_context import *

sc = spark_context("local[2]","RDD Operations").get_sc()

rdd1 = sc.textFile("/home/sylar/Desktop/SKH_dataset/spark/travel.txt")