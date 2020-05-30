from pyspark import *
from pyspark.streaming import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


class Common:
    def __init__(self, app_name='myapp', master='local'):        
        conf = SparkConf().setAppName(app_name).setMaster(master)
        self.sc = SparkContext(conf=conf)
        
    def get_spark_core(self):
        print (self.sc)
        print (self.sc.version)
        return self.sc
    
    def get_spark_sql(self):
        spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        print (sc)
        print (sc.version)
        return sc,spark