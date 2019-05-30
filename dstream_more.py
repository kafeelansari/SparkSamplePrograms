from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc,10)
ssc.checkpoint("file:///home/sylar/Desktop/SKH_dataset/spark/checkpoint_dir")
initial_rdd = sc.parallelize([(u'hello',1),(u'world',1)])


def update_function(newvalues,last_sum):
    return sum(newvalues) + (last_sum or 0)


lines = ssc.socketTextStream('localhost',9999)
words = lines.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word,1))



# running_wordcount = lines.flatMap(lambda line: line.split(" "))\
#     .map(lambda word: (word,1))\
#     .updateStateByKey(update_function,initialRDD=initial_rdd)


running_wordcount= lines.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word,1))\
    .reduceByKeyAndWindow(lambda x, y: x+y, lambda x, y: x-y, 30, 10)

running_wordcount.pprint()
ssc.start()
ssc.awaitTermination()


"""Below are the functions of a rdd and output of their value and type"""

# rdd = sc.textFile("file:///home/sylar/Desktop/SKH_dataset/spark/sample_rdd_explore.txt")
# rdd2 = sc.parallelize(range(1,10))
# print("sc.defaultMinPartitions ---> "+ str(sc.defaultMinPartitions))
# print("sc.defaultParallelism --->" + str(sc.defaultParallelism))
# print("rdd has {} partitions ".format(rdd.getNumPartitions()))
# print("rdd2 has {} partitions ".format(rdd2.getNumPartitions()))
# print("glom(rdd) ---> " + str(rdd.glom()))
# mapped_rdd = rdd.map(lambda line : line.split(" "))
# fmapped_rdd = rdd.flatMap(lambda line : line.split(" "))
# print("type(rdd)  --> " + str(type(rdd)))
# print("rdd.first  --> " + str(rdd.take(3)))
# print("type(rdd.first)  --> " + str(type(rdd.take(3))))
# print("type(mapped_rdd)  --> " + str(type(mapped_rdd)))
# print("mapped_rdd.first  --> " + str(mapped_rdd.take(3)))
# print("type(mapped_rdd.first)  --> " + str(type(mapped_rdd.take(3))))
# print("type(fmapped_rdd)  --> " + str(type(fmapped_rdd)))
# print("fmapped_rdd.first  --> " + str(fmapped_rdd.collect()))
# print("type(fmapped_rdd.first)  --> " + str(type(fmapped_rdd.take(3))))

"""--------------------------------------------------------------------------------------------------------"""
