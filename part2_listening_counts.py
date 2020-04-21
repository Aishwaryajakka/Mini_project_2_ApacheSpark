import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, count, sort_array, isnan

# Initialize the spark session
spark = SparkSession \
    .builder \
    .appName("ListeningCountsPerArtist") \
    .getOrCreate()

# Reading from HDFS - configurations are set correctly so the "hdfs://" scheme is not required.
path = "Mini_proj_2/user_artists.dat"
artist_log = spark.read.csv(path, header = "true", sep = '\t')


artist_log.show()
artist_log.describe().show()
print('Count: ',artist_log.count())

# Dataset has no null vals - but nulls can be dropped as shown below.
#user_log_valid = artist_log.dropna(how = "any", subset = ["userID", "artistID","weight"])
#print('Valid_Count: ', user_log_valid.count())
#user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")
#print('Filter : ',user_log_valid.count())


# Display total listening counts of each artist in descending order 

artist_log.select(['artistID','weight']) \
        .groupBy('artistID') \
        .agg({'weight':'sum'}) \
        .withColumnRenamed('sum(weight)','listeningCounts') \
        .sort(desc('listeningCounts')) \
        .show(artist_log.count())

spark.stop()
