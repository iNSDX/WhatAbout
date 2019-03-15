import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json


def get_auth():
    ACCESS_TOKEN = '838794388750942209-Zrj5wNI9vaTOYsl8m4Kp7ma8YCYBXeR'
    ACCESS_SECRET = 'BHJdWUjVbWMdyIxRvUXrMxpwXifk9yiXIEOjnMWgKZblY'
    CONSUMER_KEY = 'JmOly0SsPxhcsj0FPjqU7ucLW'
    CONSUMER_SECRET = '809X1X90UTyhoCvV6GTe0ZlYsYjGRVtKjxvIW4nvDxPUI5p56l'
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    return auth

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket
    
    def on_data(self, data):
        try:
            full_tweet = json.loads(data)
            tweet_text = full_tweet['text'].encode('utf-8')
            print('Tweet: ' + str(tweet_text))
            print ("------------------------------------------")
            self.client_socket.send(tweet_text)
            return True
        except BaseException as e:
            print('Error on_data: {}'.format(e))
            return True

    def if_error(self, status):
        print(status)
        return True


def send_tweets_to_spark(c_socket):
    auth = get_auth()
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['football']) # searching for this

if __name__ == "__main__":
    new_socket = socket.socket()
    host = 'localhost'
    port = 9009
    new_socket.bind((host,port))
    print("Listening on port {}...".format(str(port)))
    new_socket.listen(5)
    conn, addr = new_socket.accept()
    print("Connected... Getting tweets from {}".format(addr))
    send_tweets_to_spark(conn)