from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split


ss = SparkSession.builder.master('local[1]').getOrCreate()
df_port = ss.readStream.format("socket").option("host","localhost").option("port",9999).load()

words = df_port.select(
    explode(
        split(df_port.value," ")
    ).alias('word')
)

wordCount = words.groupby('word').count()
query = wordCount.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()