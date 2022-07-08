from Df_file import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Query - 1 --> On each of the days find which stock has moved maximum %age wise in both directions (+ve, -ve)")
spark.sql(
    "CREATE TEMP VIEW positive_pctg AS SELECT t1.Date, t1.Stock_Name, ((t1.High-t1.Open)/t1.Open)*100 as Max_Pos_Pctg "
    "from table t1 where (( t1.High-t1.Open)/t1.Open)*100 = (Select Max(((t2.High-t2.Open)/t2.Open)*100) from table "
    "t2 WHERE t1.Date = t2.Date)")
spark.sql(
    "CREATE TEMP VIEW negative_pctg AS SELECT t1.Date, t1.Stock_Name, ((t1.Open-t1.Low)/t1.Open)*100 from table t1 "
    "where ((t1.Open-t1.Low)/t1.Open)*100 = (Select Max(((t2.Open-t2.Low)/t2.Open)*100) from table t2 WHERE t1.Date = "
    "t2.Date)")

sqlDF1 = spark.sql("Select t1.Date, t1.Stock_Name as Highest, t2.Stock_Name as Lowest from positive_pctg t1 join "
                   "negative_pctg t2 on t1.Date=t2.Date")
#sqlDF1.show()



print("Question - 2 -> Which stock was most traded stock on each day.")

sqlDF2 = spark.sql(
    "SELECT t1.Date, t1.Stock_Name, t1.Volume FROM table t1 WHERE t1.Volume = (SELECT MAX(t2.Volume) FROM table t2 "
    "WHERE t1.Date = t2.Date)")

#sqlDF2.show()


print("Question - 3 -> ")

#sqlDF3 = spark.sql("")
#sqlDF3.show()


print("Query - 4 - Which stock has moved maximum from 1st Day data to the latest day Daya")

print("Tables having Only Open Price from each stock at first Day")
spark.sql("CREATE TEMP VIEW open_table AS Select Stock_Name, Open from table where "
          "Date='2017-07-10T00:00:00'")
#spark.sql("select * from open_table").show()

print("Tables having Maximum high from each stock")
spark.sql("CREATE TEMP VIEW high_table AS Select Stock_Name , Max(High) as High from table group by Stock_Name")
#spark.sql("select * from high_table").show()

spark.sql("CREATE TEMP VIEW joined_table AS select t1.Stock_Name, t1.High, t2.Open from high_table t1 Inner join "
          "open_table t2 on t1.Stock_Name=t2.Stock_Name")

print("join of Above two Tables :- ")
#spark.sql("select * from joined_table").show()

print("Final Desired Output : ")
sqlDF4 = spark.sql("Select t1.Stock_Name , t1.High-t1.Open as Maximum_Movement from joined_table t1 where "
                   "t1.High-t1.Open = (Select Max(t2.High-t2.Open) from joined_table t2)")
#sqlDF4.show()

print("Query - 5 -> Find the standard deviations for each stock over the period")
sqlDF5 = spark.sql("SELECT Stock_Name, STD(Close) as Standard_Deviation from table group by Stock_Name")
#sqlDF5.show()


print("Query - 6.1 -> Mind the mean  and median prices for each stock")

sqlDF6 = spark.sql("Select Stock_Name, avg(open) as Mean, percentile_approx(open,0.5) as Median from table group by "
                   "Stock_Name")

#sqlDF6.show()

print("question - 7 -> Find the average volume over the period")
sqlDF7 = spark.sql("SELECT Stock_Name, avg(Volume) Average_volume from table group by Stock_Name")
#sqlDF7.show()


print("Question - 8 -> Find which stock has higher average volume")
sqlDF8 = spark.sql("select Stock_Name, avg(Volume) as Max_Average_Volume from table group by Stock_Name order by "
                   "Max_Average_Volume DESC limit 1")
#sqlDF8.show()


print("Question - 9 -> Find the highest and lowest prices for a stock over the period of time")
sqlDF9 = spark.sql("select Stock_Name, MIN(Low) AS Highest_Price, MAX(High) AS Lowest_Price from table group by "
                   "Stock_Name")
sqlDF9.show()

