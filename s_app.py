from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession, HiveContext
from pyspark.sql.types import StructField, StructType, FloatType, StringType
import sys
import requests
import findspark
import os 
import shutil
from aylienapiclient import textapi

if os.path.isdir('ck_whatabout'):
    shutil.rmtree('ck_whatabout')
if os.path.isdir('spark-warehouse'):
    shutil.rmtree('spark-warehouse')
if os.path.isdir('metastore_db'):
    shutil.rmtree('metastore_db')

findspark.init()

# Configuration for the SparkContext
conf = SparkConf().setMaster("local[*]").setAppName('WhatAbout')

# Create spark instance
sc = SparkContext.getOrCreate(conf)

# Enable Hive support
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
# Create empty table tweets that will store the data
spark.sql('CREATE TABLE tweets (tweet string, score float) STORED AS TEXTFILE')

# Create the Streaming Context from the above spark context with window size 5 seconds
ssc = StreamingContext(sc, 5)
# Setting a checkpoint to allow RDD recovery
ssc.checkpoint("ck_whatabout")

# Read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)

def send_df_to_dashboard(df):
    # Extract the tweet from dataframe and convert them into array
    tweets = [str(t.tweet) for t in df.select("tweet").collect()]
    print(tweets)
    # Extract the sentiment_scores from dataframe and convert them into array
    sentiment_scores = [s.score for s in df.select("score").collect()]
    print(sentiment_scores)
    # Initialize and send the data through REST API
    url = 'http://localhost:5000/updateData'
    request_data = {'tweets': str(tweets), 'scores': str(sentiment_scores)}
    requests.post(url, data=request_data)


def analyzeSentiment(tweet):
    # CARE
    # 1000 calls/day
    client = textapi.Client("e4c91a1c", "7af649d5a8502da656033172bf37ca7a")
    sentiment = client.Sentiment({'mode': 'tweet','text': tweet})
    score = round(float(sentiment.get('polarity_confidence')),2)

    print("-----------------------------------"+tweet+'| Sentiment: '+str(score)+"----------------------------------")
    return score


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        sql_context = HiveContext(rdd.context)
        # Convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(tweet=w, score=analyzeSentiment(w)))
        schema = StructType([StructField("tweet", StringType(), True), StructField("score", FloatType(), True)])
        # Create a DF with the specified schema
        new_tweets_df = sql_context.createDataFrame(row_rdd, schema=schema)
        # Register the dataframe as table
        new_tweets_df.registerTempTable("new_tweets")
        # Insert new tweets,scores into table tweets
        sql_context.sql("INSERT INTO TABLE tweets SELECT * FROM new_tweets")
        # Get all the tweets from the table using SQL
        tweets_sentiment_df = sql_context.sql("SELECT * FROM tweets")
        tweets_sentiment_df.show()

        # Sends the tweets and their sentiment score to the dashboard
        send_df_to_dashboard(tweets_sentiment_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# Do processing for each RDD generated in each interval
dataStream.foreachRDD(process_rdd)

# Start the streaming computation
ssc.start()

# Wait for the streaming to finish
ssc.awaitTermination()
