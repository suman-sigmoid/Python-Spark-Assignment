import datetime
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import pandas as pd

app = Flask(__name__)


@app.route("/result", methods=["GET"])
def result():
    data1 = fun()
    return jsonify(data1)


def fun():
    output = {}

    spark = SparkSession.builder.appName('Read Many Stocks Details').getOrCreate()

    spark_df = spark.read.csv("/Users/sumanchoudhary/PycharmProjects/pythonProject/python-spark-assignment/Data/*.csv", sep=',', header=True)

    spark_df.createOrReplaceTempView("table")
    # for query 2
    sqlDF2 = spark.sql(
        "SELECT t1.Date, t1.Stock_Name, t1.Volume FROM table t1 WHERE t1.Volume = (SELECT MAX(t2.Volume) FROM table t2 "
        "WHERE t1.Date = t2.Date)").toPandas()

    my_output = sqlDF2.to_dict('records')
    output["stock was most traded stock on each day - "] = my_output

    return output


if __name__ == '__main__':
    app.run(debug=True, port=2002)
# for query 1
spark.sql(
        "CREATE TEMP VIEW positive_pctg AS SELECT t1.Date, t1.Stock_Name, ((t1.High-t1.Open)/t1.Open)*100 as "
        "Max_Pos_Pctg from table t1 where (( t1.High-t1.Open)/t1.Open)*100 = (Select Max((("
        "t2.High-t2.Open)/t2.Open)*100) from table t2 WHERE t1.Date = t2.Date)")
    spark.sql(
        "CREATE TEMP VIEW negative_pctg AS SELECT t1.Date, t1.Stock_Name, ((t1.Open-t1.Low)/t1.Open)*100 from table t1 "
        "where ((t1.Open-t1.Low)/t1.Open)*100 = (Select Max(((t2.Open-t2.Low)/t2.Open)*100) from table t2 WHERE "
        "t1.Date = t2.Date)")

    sqlDF1 = spark.sql(
        "Select t1.Date, t1.Stock_Name as Highest, t2.Stock_Name as Lowest from positive_pctg t1 join negative_pctg "
        "t2 on t1.Date=t2.Date").toPandas()

    my_output = sqlDF1.to_dict('records')
    output["stock has moved maximum %age wise in both directions - "] = my_output

    return output
# for query 4



    spark.sql("CREATE TEMP VIEW open_table AS Select Stock_Name, Open from table where "
              "Date='2017-07-10T00:00:00'")

    print("Tables having Maximum high from each stock")
    spark.sql("CREATE TEMP VIEW high_table AS Select Stock_Name , Max(High) as High from table group by Stock_Name")

    spark.sql("CREATE TEMP VIEW joined_table AS select t1.Stock_Name, t1.High, t2.Open from high_table t1 Inner join "
              "open_table t2 on t1.Stock_Name=t2.Stock_Name")

    spark.sql("select * from joined_table").show()

    sqlDF4 = spark.sql("Select t1.Stock_Name , t1.High-t1.Open as Maximum_Movement from joined_table t1 where "
                       "t1.High-t1.Open = (Select Max(t2.High-t2.Open) from joined_table t2)").toPandas()

    my_output = sqlDF4.to_dict('records')
    output["stock has moved maximum from 1st Day data to the latest day Data - "] = my_output

    return output

# api for quesry 5


sqlDF5 = spark.sql("SELECT Stock_Name, STD(Close) as Standard_Deviation from table group by Stock_Name").toPandas()

my_output = sqlDF5.to_dict('records')
output["standard deviations for each stock - "] = my_output

return output

# api for query 6
sqlDF6 = spark.sql(
        "Select Stock_Name, avg(open) as Mean, percentile_approx(open,0.5) as Median from table group by "
        "Stock_Name").toPandas()

    my_output = sqlDF6.to_dict('records')
    output["mean  and median prices - "] = my_output

    return output
# for query 7

sqlDF7 = spark.sql("SELECT Stock_Name, avg(Volume) Average_volume from table group by Stock_Name").toPandas()

my_output = sqlDF7.to_dict('records')
output["average volume "] = my_output

return output
# for query 8

sqlDF8 = spark.sql("select Stock_Name, avg(Volume) as Max_Average_Volume from table group by Stock_Name order by "
                   "Max_Average_Volume DESC limit 1").toPandas()

my_output = sqlDF8.to_dict('records')
output["stock has higher average volume - "] = my_output

return output

# for query 9

sqlDF9 = spark.sql("select Stock_Name, MIN(Low) AS Highest_Price, MAX(High) AS Lowest_Price from table group by "
                   "Stock_Name").toPandas()

my_output = sqlDF9.to_dict('records')
output["highest and lowest prices for a stock - "] = my_output

return output

