from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.types import StructField, StructType, FloatType, StringType
import sys
import requests
import findspark
import os 
import shutil

if os.path.isdir('ck_whatabout'):
    shutil.rmtree('ck_whatabout')
if os.path.isdir('spark-warehouse'):
    shutil.rmtree('spark-warehouse')

findspark.init()
# Create spark instance
sc = SparkContext(appName='WhatAbout')
sc.setLogLevel("ERROR")
# Creat the Streaming Context from the above spark context with window size 5 seconds
ssc = StreamingContext(sc, 5)
# Setting a checkpoint to allow RDD recovery
ssc.checkpoint("ck_whatabout")
# Read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df):
    # Extract the tweet from dataframe and convert them into array
    tweets = [str(t.tweet) for t in df.select("tweet").collect()]
    print(tweets)
    # Extract the sentiment_scores from dataframe and convert them into array
    sentiment_scores = [s.sentiment_score for s in df.select("sentiment_score").collect()]
    print(sentiment_scores)
    # Initialize and send the data through REST API
    url = 'http://localhost:5000/updateData'
    request_data = {'label': str(tweets), 'data': str(sentiment_scores)}
    requests.post(url, data=request_data)

def analyzeSentiment(tweet):
    r = requests.post("https://api.deepai.org/api/sentiment-analysis",
        data={
            'text': tweet,
        },
        headers={
            'api-key': 'eeae3c4e-7b77-42dc-91a5-865f809e4c0d'
        })

    sentiment = r.json()
    print("-----------------------------------"+tweet+'| Sentiment: '+str(sentiment)+"----------------------------------")
    sentiment_score_sum = 0.0
    sentiment_output = sentiment.get('output')

    if type(sentiment_output) is list and len(sentiment_output)>0:
        for s in sentiment_output:
            if s == 'Verynegative':
                sentiment_score_sum+=0
            elif s == 'Negative':
                sentiment_score_sum+=0.25
            elif s == 'Positive':
                sentiment_score_sum+=0.75
            elif s == 'Verypositive':
                sentiment_score_sum+=1
            else:
                sentiment_score_sum+=0.5

        sentiment_score = round(sentiment_score_sum/len(sentiment_output),2)
    else:
        sentiment_score = 0.5

    return sentiment_score

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # Convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(tweet=w, sentiment_score=analyzeSentiment(w)))
        schema = StructType([StructField("tweet", StringType(), True), StructField("sentiment_score", FloatType(), True)])
        # Create a DF from the Row RDD
        tweets_df = sql_context.createDataFrame(row_rdd, schema=schema)
        # Register the dataframe as table
        tweets_df.registerTempTable("tweets")
        # Get all the tweets from the table using SQL and print them
        tweets_sentiment_df = sql_context.sql("SELECT * FROM tweets ORDER BY sentiment_score DESC")
        tweets_sentiment_df.show()
        # Sends the tweets and their sentiment score to the dashboard
        # send_df_to_dashboard(tweets_sentiment_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# Map each tweet_data [tweet,sentiment] to be a pair of (tweet,sentiment)
#tweets = dataStream.map(lambda tweet: (tweet, analyzeSentiment(tweet)))

# Do processing for each RDD generated in each interval
dataStream.foreachRDD(process_rdd)

# Start the streaming computation
ssc.start()

# Wait for the streaming to finish
ssc.awaitTermination()
