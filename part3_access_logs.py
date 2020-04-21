import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
import datetime

# Initialize spark session
spark = SparkSession \
    .builder \
    .appName("Access Logs") \
    .getOrCreate()


# Reading from HDFS - configurations are set correctly so the "hdfs://" scheme is not required.
path = "Mini_proj_2/access_log"
df = spark.read.csv(path, sep = " ")
print(df.printSchema())
print(df.show())

df.createOrReplaceTempView("myTable")

df2 = spark.sql("SELECT _c0 AS IP, _c3 as Time from myTable")

df2.persist()
print(df2.printSchema())
print(df2.show())
print(df2.columns)

# Remove '[' from the Time column
df2 = df2.withColumn("Time", regexp_replace(col("Time"),"[\$\[]",""))
df2 = df2.withColumn("Time", to_timestamp(df2.Time, 'd/MMM/yyyy:HH:mm:ss'))
print(df2.show())

# Group IP counts - by month of year
result = df2.select(['IP','Time']) \
        .groupBy(year('Time'), month('Time')) \
        .agg({'IP':'count'}) \
        .orderBy(year('Time'), month('Time'))

result.persist()
result = result.withColumn("year(Time)", result["year(Time)"].cast(StringType()))
result = result.withColumn("month(Time)", result["month(Time)"].cast(StringType()))

print(result.show())

result = result.withColumn('Time',concat(col('year(Time)'),lit('/'), col('month(Time)')))
result = result.drop('year(Time)')
result = result.drop('month(Time)')

print(result.show())

result = result.withColumn("Time", to_date(result.Time, 'yyyy/MM'))
print(result.show())

# Rename Column count(IP) to IP
result = result.withColumnRenamed("count(IP)", "IP")
# Drop null values
result = result.dropna(how = "any", subset = ["IP", "Time"])
print(result.show())

# Converting datetime to unix_timestamp
result = result.withColumn("Time", unix_timestamp(result.Time))
print(result.show())
result = result.withColumn("IP", result["Time"].cast(IntegerType()))

# Convert features to vectors with VectorAssembler - required by ML models
assembler = VectorAssembler(inputCols=['IP', 'Time'], outputCol = 'features')
v_result = assembler.transform(result)
v_result = v_result.select(['features', 'Time'])
splits = v_result.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]
lr = LinearRegression(featuresCol = 'features', labelCol='Time', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))
lrModelSummary = lr_model.summary
print("Train R2 Score: ",lrModelSummary.r2)

# Save Model
lr_model.save("lr_model.model")
spark.stop()

# To overwrite model
#lr_model.write().overwrite().save("lr_model.model")

# To load model
#lr_model = LinearRegressionModel.load("lr_model.model")
