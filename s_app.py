from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession, HiveContext
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
ssc = StreamingContext(sc, 3)
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
    sentiment_scores = [s.score for s in df.select("score").collect()]
    print(sentiment_scores)
    # Initialize and send the data through REST API
    url = 'http://localhost:5000/updateData'
    request_data = {'tweets': str(tweets), 'scores': str(sentiment_scores)}
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
        tweets_sentiment_df = sql_context.sql("SELECT * FROM tweets ORDER BY score DESC")
        tweets_sentiment_df.show()

        # Sends the tweets and their sentiment score to the dashboard
        send_df_to_dashboard(tweets_sentiment_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# Map each tweet_data [tweet,sentiment] to be a pair of (tweet,sentiment)
# tweets = dataStream.map(lambda tweet: (tweet, analyzeSentiment(tweet)))

# Do processing for each RDD generated in each interval
dataStream.foreachRDD(process_rdd)

# Start the streaming computation
ssc.start()

# Wait for the streaming to finish
ssc.awaitTermination()
