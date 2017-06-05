from tweepy import OAuthHandler, StreamListener, Stream, API
import time
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from django.http import HttpResponse, JsonResponse
import geocoder
import ConfigParser

Config = ConfigParser.ConfigParser()
Config.read("./config.ini")

twitter_auth = OAuthHandler(Config.get('twitter', 'consumer_key'), Config.get('twitter', 'consumer_secret'))
twitter_auth.set_access_token(Config.get('twitter', 'access_token'), Config.get('twitter', 'access_token_secret'))
twitter_api = API(twitter_auth, wait_on_rate_limit_notify=True, retry_count=3, retry_delay=5)

es_host = '<Write your ES end-point>'

es = Elasticsearch(host = es_host,
                    port = 443,
                   use_ssl=True,
                   verify_certs=True,
                   connection_class=RequestsHttpConnection)

class cricketlistener(StreamListener):
    def __init__(self, time_limit=60):
        self.time = time.time()
        self.count = 0
        self.limit = time_limit
        self.data = []
        super(cricketlistener, self).__init__()


    def on_data(self, data):
        tweet_data = json.loads(data)
        if tweet_data['text'] is not None:
            try:
                print tweet_data['text']
                hashtags = [i['text'] if tweet_data['entities']['hashtags'] is not None else None
                            for i in tweet_data['entities']['hashtags']]
                print hashtags
                doc = {
                    'title': tweet_data['text'],
                    'username' : tweet_data['user']['name'],
                    'user': tweet_data['user']['screen_name'],
                    'hashtags': hashtags,
                    'retweet_count': tweet_data['retweet_count']
                }
                print doc
                es.index(index="crickettweets", doc_type="Clustering", id=tweet_data['id'], body=doc)
                self.count += 1
                if self.count == 10:
                    self.count = 0
                    print("Sleeping")
                    time.sleep(10)
            except Exception as e:
                print "Error is: ", e
        else:
            print "Title not found"

    def on_error(self, status):
        print("response: %s" % status)
        if status == 420:
            return False

if __name__ == '__main__':
    print("Twitter Stream Begin!!")
    l = cricketlistener()
    while True:
        try:
            twitter_stream = Stream(twitter_api.auth, l)
            twitter_stream.filter(track=['#indvspak', '#ct17'])
        except Exception as e:
            print (e)