from pyspark import *
from pyspark.streaming import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


class Common:
    def __init__(self, app_name='myapp', master='local[2]'):
        self.app_name = app_name
        self.master = master
        
    def get_spark_core(self):
        conf = SparkConf().setAppName(self.app_name).setMaster(self.master)
        sc = SparkContext(conf=conf)
        print (sc)
        print (sc.version)
        return sc
    
    def get_spark_sql(self):
        spark = SparkSession.builder.appName(self.app_name).enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        print (sc)
        print (sc.version)
        return sc,spark
    
    def get_spark_streaming(self):
        conf = SparkConf()
        conf.setAppName(self.app_name)
        sc = SparkContext(conf=conf)
        ssc = StreamingContext(sc, 10)
        print (sc)
        print (ssc)
        return sc,ssc