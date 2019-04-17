import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
ACCESS_TOKEN = '838794388750942209-Zrj5wNI9vaTOYsl8m4Kp7ma8YCYBXeR'
ACCESS_SECRET = 'BHJdWUjVbWMdyIxRvUXrMxpwXifk9yiXIEOjnMWgKZblY'
CONSUMER_KEY = 'JmOly0SsPxhcsj0FPjqU7ucLW'
CONSUMER_SECRET = '809X1X90UTyhoCvV6GTe0ZlYsYjGRVtKjxvIW4nvDxPUI5p56l'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

# Commented for development 
# search_word = input("Enter a word to filter by: ") or "football"
# search_language = input("Enter prefered language: ") or "en"
# search_location = input("Enter location of the tweets: ")

def send_tweets_to_spark(http_resp, tcp_connection):

    for line in http_resp.iter_lines():

        full_tweet = json.loads(line)
        tweet_text = full_tweet['text'].encode('utf-8')
        tweet_str = tweet_text.decode("utf-8")

        try:
            print("Tweet Text: " + tweet_str)
            print ("------------------------------------------")
            tcp_connection.send(tweet_text)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    #query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    #query_data = [('locations', '-130,-20,100,50'), ('track', '#')]
    query_data = [('track', 'football')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)

    def analyzeSentiment(tweet):
        r = requests.post("https://api.deepai.org/api/sentiment-analysis",
            data={
                'text': tweet,
            },
            headers={
                'api-key': 'eeae3c4e-7b77-42dc-91a5-865f809e4c0d'
            })
        return r.json()

    for line in response.iter_lines():
        full_tweet = json.loads(line)
        tweet_text = full_tweet['text'].encode('utf-8')
        tweet_str = tweet_text.decode("utf-8")
        sentiment = analyzeSentiment(tweet_str)

        sentiment_score_sum = 0.0
        for s in sentiment.get('output'):
            if s == 'Verynegative':
                sentiment_score_sum+=0
            elif s == 'Negative':
                sentiment_score_sum+=0.25
            elif s == 'Neutral':
                sentiment_score_sum+=0.5
            elif s == 'Positive':
                sentiment_score_sum+=0.75
            elif s == 'Verypositive':
                sentiment_score_sum+=1
        sentiment_score = sentiment_score_sum/len(sentiment.get('output'))

        print("Tweet: {} | Sentiment: {} | Sentiment score: {}".format(tweet_str,sentiment,sentiment_score))

    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(5)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp,conn)