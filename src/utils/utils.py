from pyspark.sql import SparkSession, DataFrame

# function for create Spark session
def create_spark_session() -> SparkSession:
    spark = SparkSession.builder\
        .appName("test")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark

# function for reading data from csv file
def read_csv_file(file_path: str, spark: SparkSession, delimeter: str = ";", header: bool = True, infer_schema: bool = True) -> DataFrame:
    df = spark.read.csv(file_path, sep=delimeter, header=header, inferSchema=infer_schema)
    print("Read data from {} successfully".format(file_path))
    return df

# function for save data to single csv file using toPandas
def save_single_csv_file(df: DataFrame, file_path: str, delimeter: str = ",", header: bool = True) -> None:
    df.toPandas().to_csv(file_path, index=False, header=header, sep=delimeter)
    print("Save data to {} successfully".format(file_path))