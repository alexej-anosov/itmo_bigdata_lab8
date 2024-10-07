from pyspark.sql import SparkSession, DataFrame

class DataMart:
    def __init__(self, spark: SparkSession):

        self.spark_context = spark.sparkContext
        self.datamart = self.spark_context._jvm.DataMart() 

    def get_manager(self):
        return self.datamart
