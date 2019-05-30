from spark_context import spark_context

sc_init = spark_context("local[1]","tst_import")
sc = sc_init.get_sc()

rdd = sc.parallelize([1,2,3,4])
print(rdd.collect())
