from pyspark import *
from pyspark.streaming import *
from pyspark import SparkContext, SparkConf

class Common:
    def __init__(self, app_name='myapp', master='local'):        
        conf = SparkConf().setAppName(app_name).setMaster(master)
        self.sc = SparkContext(conf=conf)
        
    def get_sc(self):
        print (self.sc)
        print (self.sc.version)
        return self.sc