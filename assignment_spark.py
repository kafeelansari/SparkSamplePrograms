"""This is the assignment to analyse logs

Log Parsing


Below is a log of events from various channels (like Android/Web) of an ecommerce platform.
The various events captured in the logs are-

Impression - these are views on the various content in the homepage, searchpage etc.
PageView corresponds to a product page view, it has additional attributes like price and viewTime
AddToCart whenever a user adds an item to the cart
PageServeError indicates a failure to load a product page
--
2018-08-01T12:00:00 Android - event=Impression,pid=SAMPLE_PRODUCT_1
2018-08-01T12:00:00 Android - event=Impression,pid=SAMPLE_PRODUCT_2
2018-08-01T12:00:00 Web - event=Impression,pid=SAMPLE_PRODUCT_3
2018-08-01T12:00:00 Web - event=PageView,pid=SAMPLE_PRODUCT_1,price=2000,viewTime=100s
2018-08-01T12:00:00 Web - event=PageView,pid=SAMPLE_PRODUCT_2,price=1500,viewTime=80s
2018-08-01T12:00:00 Android - event=AddToCart,pid=SAMPLE_PRODUCT_1
2018-08-01T12:00:00 Android - event=AddToCart,pid=SAMPLE_PRODUCT_ERR
2018-08-01T12:00:00 Web - event=PageView,pid=SAMPLE_PRODUCT_2,price=-20,viewTime=10s
2018-08-01T12:00:00 Web - event=PageServeError,pid=SAMPLE_PRODUCT_4
2018-08-01T12:00:00 Web - event=PageView,pid=SAMPLE_PRODUCT_5,price=300,viewTime=10m
--


You may use any language of your choice to code the solution.
Q1. Copy the above logs into a file. Process the logs into a structured CSV file, schema should be in the following format. Try to finish this question before attempting the below questions.

ts,event,channel,pid,price,viewTime

Derive the following aggregates from the CSV, print to the console
Q2. Count events by channel
Q3. Conversion = AddToCarts/PageViews
Q4. Distinct products viewed in PageView event
Q5. Perform validations, move such logs to error.logs file. Code should be written in a way that it’s easy to add new rules.
pid should be exactly 16 chars

price should be positive for applicable event
viewTime should end with 's'


"""
from sys import argv
import re
from pyspark import SparkContext,SparkConf
from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import length,substring

rules= {}


def convert_to_row(regx):
    reg_ex1 = "([^\s]+)T([^\s]+) ([^\s]+) - event=([^\s]+),pid=([^\s]+)"
    reg_ex2 = "([^\s]+)T([^\s]+) ([^\s]+) - event=([^\s]+),pid=([^\s]+),price=([^\s]+),viewTime=([^\s]+)"
    type_regex = "event=([^\s][^,]+)"
    type_check = re.compile(type_regex)
    final_type = type_check.search(regx)
    if final_type.group(1)=="Impression" or final_type.group(1)=="AddToCart" or final_type.group(1)=="PageServeError":
        reg = re.compile(reg_ex1)
        record = reg.search(regx)
        return record.group(1)+" "+record.group(2),record.group(4),record.group(3),record.group(5),str(0),str(0)
    elif final_type.group(1)=="PageView" :
        reg = re.compile(reg_ex2)
        record = reg.search(regx)
        return record.group(1)+" "+record.group(2),record.group(4),record.group(3),record.group(5),record.group(6),record.group(7)


conf = SparkConf().setAppName("Log Parser").setMaster("local[1]")
sc = SparkContext(conf=conf)
sc.setLogLevel("Error")
raw_record = sc.textFile(argv[1])
log = raw_record.map(lambda x: convert_to_row(x))
log_schema = log.map(lambda p: Row(ts=p[0],event=p[1],channel=p[2],pid=p[3],price=p[4],ViewTime=p[5]))
spark = SparkSession.builder.master("local[1]").getOrCreate()
log_df = spark.createDataFrame(log_schema)
#cached_log = log_df.groupby('event').count().cache()
#cached_log.filter(log_df.event=='AddToCart').show()
# count = cached_log.filter(log_df.event=="AddToCart").count()/ cached_log.filter(log_df.event=="PageView").count()
# print(count)
#log_df.show()
#Adding new rules
ruled_df = log_df.where((length('pid') == 16) & (log_df.price>=0)).filter("ViewTime like '%s'")
# ruled_df = log_df.filter("ViewTime like '%s'")
ruled_df.show()
