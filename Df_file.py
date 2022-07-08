from pyspark.sql import SparkSession
def create_Spark_DF():
    spark = SparkSession.builder.appName('Read Many Stocks Details').getOrCreate()
    spark_df = spark.read.csv("/Users/sumanchoudhary/PycharmProjects/pythonProject/python-spark-assignment/Data/*.csv", sep=',', header=True)
    return spark_df, spark
