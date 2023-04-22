from pyspark.sql import SparkSession, DataFrame

# function for create Spark session
def create_spark_session() -> SparkSession:
    spark = SparkSession.builder\
        .appName("test")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark
