from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("Ways to create RDD").setMaster("local[2]")
sc = SparkContext(conf=conf)

"""Way 1 : Using parallelize"""
iter_list = [1,2,3,4,5,6,7]
iter_set = set([1,2,3,4,5])
rdd = sc.parallelize(iter_set)
print(type(rdd))