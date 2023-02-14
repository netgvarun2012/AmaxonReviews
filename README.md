# AmazonReviews
Spark program in python to find the top 15 products based on the number of reviews in a day and report their average ratings, review time and product brand name.

- The first step is to import all the libraries required to run the program.
- Then using **SparkContext.getOrCreate()** function, a connection to a Spark cluster is obtained, which will be used to create RDDs.
- Further, 'json' file is read and converted to a RDD using the 'map' function which applies a function to each items in a iterable.
- RDD is further refined by setting the tuple (the product ID/asin,review time) as the 'key' and the tuple (#review, average_ratings) as the 'value'. #review is initialized to 1 to begin with.
- Next, **ReduceByKey** function groups tuples (kay-value pairs) in a list by the first items, and perform reduce operation on the second items. We use it to sum up all the ratings and the number of ratings for a 'asin' in a given 'day'.
- TO get the actual average ratings of the review in each day, **mapValues()** function transformation is used which divides the first entry (sum-of-ratings) by the second entry (count-of-ratings).
- 
