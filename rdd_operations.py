from spark_context import *
from sys import argv



sc = spark_context("local[2]","RDD Operations").get_sc()
rdd1 = sc.textFile(argv[1])