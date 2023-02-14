# AmazonReviews
Spark program in python to find the top 15 products based on the number of reviews in a day and report their average ratings, review time and product brand name.

- The first step is to import all the libraries required to run the program.
-  Them a connection to a Spark cluster is obtained, which will be used to create RDDs by using **SparkContext.getOrCreate()** function
