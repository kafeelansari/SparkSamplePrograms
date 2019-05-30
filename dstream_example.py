from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc,1)

lines = ssc.socketTextStream('localhost',9999)
words = lines.flatMap(lambda line : line.split(" "))
pairs = words.map(lambda word : (word,1))
word_count = pairs.reduceByKey(lambda x,y : x+y)
word_count.pprint()
ssc.start()
ssc.awaitTermination()