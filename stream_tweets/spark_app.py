from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.types import StructField, StructType, StringType, LongType
from collections import namedtuple
import findspark
import sys
import requests
import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns
import pandas

findspark.init()
# create spark instance with the above configuration
sc = SparkContext(appName='WhatAbout')
# creat the Streaming Context from the above spark context with window size 2 seconds
ssc = StreamingContext(sc, 10)
# setting a checkpoint to allow RDD recovery
# ssc.checkpoint("checkpoint_WhatAbout")
# initiate SQLContext
sqlContext = SQLContext(sc)
# read data from port 9009
socket_stream = ssc.socketTextStream("localhost",9009)
# tweets with window 2s
lines = socket_stream.window(60)

fields = ('hashtag', 'count')
Tweet = namedtuple('Tweet', fields)

(lines.flatMap(lambda text: text.split(' '))
    .filter(lambda word: word.lower().startswith('#'))
    .map(lambda word: (word.lower(),1))
    .reduceByKey(lambda a,b: a+b)
    .map(lambda rec: Tweet(rec[0],rec[1]))
    .foreachRDD(lambda rdd: rdd.toDF(['hashtag','count']).orderBy('count', ascending=False)
    .limit(10).registerTempTable('tweets')))

ssc.start()

# count = 0
# while count < 5:
#     time.sleep(5)
#     top_10_tags = sqlContext.sql('select hashtag,count from tweets')
#     top_10_df = top_10_tags.toPandas()
#     display.clear_output(wait=True)
#     plt.figure(figsize=(10,8))
#     sns.barplot(x='count', y='hashtag', data=top_10_df)
#     plt.show()
#     count += 1
#     print(count)

ssc.awaitTermination()
