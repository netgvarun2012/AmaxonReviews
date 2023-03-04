# importing required libraries
import json
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Obtaining a connection to a Spark cluster, which will be used to create RDDs.
sc  = SparkContext.getOrCreate()

# Reading the reviews json file and converting it to a RDD using the 'map' function which applies a function to each item in an iterable.
dataset_json = sc.textFile(sys.argv[1])
dataset = dataset_json.map(lambda x: json.loads(x))

# Setting the tuple (the product ID/asin,review time) as the 'key' and the tuple (#review, average_ratings) as the 'value'. #review is initialized to 1 to begin with.
pairs = dataset.map(lambda x: ((x['asin'],x['reviewTime']),(x['overall'],1)))

# Using 'reduceByKey' function to sum up all the ratings and the number of ratings for a 'asin' in a given 'day'.
sum_and_count = pairs.reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1]))

# Below function transformation divides the first entry (sum-of-ratings) by the second entry (count-of-ratings) to obtain average.
average_rating = sum_and_count.mapValues(lambda x: (x[1],(x[0]/x[1])))

# Step 2

# Below, 2nd json file is read into a dataframe, and is subsequently converted to a RDD.
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("readJSON").getOrCreate()
readJSONDF = spark.read.json(sys.argv[2])
rddFile = readJSONDF.rdd

# filter out items which returns False - in our case , the items which have brand set to 'None'.
brandDetails = rddFile.map(lambda x: (x['asin'], x['brand'] if 'brand' in x else None))
brandDetails = brandDetails.filter(lambda x: x[1] != None)

# Inner join is executed over the 2 RDDs created in above steps based on the common key 'asin'.
combinedRDD = average_rating.map(lambda x: (x[0][0],(x[0][1],x[1]))).join(brandDetails)

# Finally, top function with argument of '15' is used to return a list sorted in descending order with top 15 greatest number of reviews in a day.
newRDD = combinedRDD.map(lambda x: (x[0],x[1][0][1][0],x[1][0][0],x[1][0][1][1],x[1][1])).top(15, key = lambda x: x[1])

# Lastly, converting the list to RDD and saving the output in a file
outputRDD = sc.parallelize(newRDD)
outputRDD.saveAsTextFile(sys.argv[3])
