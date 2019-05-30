from pyspark import SparkContext,SparkConf

class spark_context:
    def __init__(self,master,app_name):
        self.conf = SparkConf().setAppName(app_name).setMaster(master)
        self.sc = SparkContext(conf=self.conf)

    def get_sc(self):
        return self.sc
