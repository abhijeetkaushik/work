from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, to_date, max, row_number, lit
import os
import sys
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.config("spark.driver.host", "localhost").appName('SparkByExamples.com').getOrCreate()
simpleData = ((1002001, 101, "NewYork", "12-04-2021"),
              (1002001, 101, "NewYork2", "12-05-2021"),
              (1002001, 102, "California", "12-03-2021"),
              (1002001, 103, "Florida", "12-09-2021"),
              (1002001, 103, "Florida1", "13-09-2021"),
              (1002001, 103, "Florida11", "12-10-2021"),
              (1002001, 104, "NM", "14-06-2021"),
              (1002002, 101, "NewYork", "01-08-2021"),
              (1002002, 110, "WDC", "12-09-2022"),
              (1002002, 105, "PHp", "12-09-2020"),
              (1002002, 105, "PHp100", "12-11-2020"),
              (1002002, 102, "California", "12-01-2021"),
              (1002003, 101, "NewYork", "12-02-2021"),
              (1002003, 103, "Florida", "12-03-2021"),
              (1002003, 106, "Seattle", "15-09-2021"),
              (1002003, 107, "Texas", "19-09-2021"),
              (1002004, 103, "Florida", "12-03-2021"),
              (1002004, 103, "Florida06", "12-06-2021"),
              (1002004, 104, "NM", "19-09-2021"),
              (1002004, 105, "PHp", "19-10-2021")
              )
columns = ["E_ID", "F_ID", "F_Value", "F_Date"]

df = spark.createDataFrame(data=simpleData, schema=columns).select("E_ID", "F_ID", "F_Value",
                                                                   to_date(col("F_Date"), "dd-MM-yyyy").alias("F_Date"))
windowSpecAgg = Window.partitionBy("E_ID","F_ID").orderBy(col("F_Date").desc())
df = df.withColumn("row",row_number().over(windowSpecAgg)) \
  .filter(col("row") == 1).drop("row")

pivotDF = df.groupBy("E_ID").pivot("F_ID").agg(first(col("F_Value")))
pivotDF2 = df.groupBy("E_ID").agg(max(col("F_Date")).alias('F_Date')).selectExpr("E_ID as E_ID_2", "F_Date as F_Date")

finalDF = pivotDF.join(pivotDF2, pivotDF.E_ID == pivotDF2.E_ID_2, "inner").select(*pivotDF.columns, pivotDF2.F_Date)

src_column = finalDF.columns

col_dictionary={
    'E_ID': 'E_ID',
    'F_Date': 'F_Date',
    '101': 'F_Name',
    '102': 'F_Sal',
    '119': 'F_Desig',
    '134': 'F_HA'
}
col_dict = list(col_dictionary.keys())
col_final = list(col_dictionary.values())

for col_values in col_dict:
    if col_values in src_column:
        finalDF = finalDF.withColumnRenamed(col_values,col_dictionary.get(col_values))
    else:
        finalDF = finalDF.withColumn(col_dictionary.get(col_values),lit(None))

finalDF.printSchema()
finalDF = finalDF.select(*col_final)
finalDF.show(100, 0)
