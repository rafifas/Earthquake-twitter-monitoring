"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a PubSub topic.
"""
import base64
from tweepy 
from google.cloud import pubsub

PROJECT_ID = os.getenv('GCP_PROJECT')
TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'tweets')
PS = pubsub.PublisherClient()

def publish(client, topics, data_lines):
    """Publish to the given pubsub topic."""
    messages = []
    for line in data_lines:
        pub = base64.urlsafe_b64encode(line)
        messages.append({'data': pub})
    return client.publish(topics, messages)

class MyStreamListener(tweepy.StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """
    count = 0
    tweets = []
    batch_size = 10

	def write_to_pubsub(message):
		return publish(PS, TOPIC, message)

    def on_data(self, data):
        """What to do when tweet data is received."""
        keywords=['gempa','earthquake']
        
        if any(word in data.lower() for word in keywords):
            self.tweets.append(data)
        if len(self.tweets) >= self.batch_size:
            # print(self.tweets)
            self.write_to_pubsub(self.tweets)
            self.tweets = []
        self.count += 1

    def on_error(self, status):
        print (status)

class read_auth:
  def __init__(self, source):
    file1 = open(f"{source}","r") 
    authentication ={}
    for line in file1:
      key, value = line.split()
      authentication[key] = value
    self.access_token = authentication['accessToken']
    self.access_token_secret = authentication['accessTokenSecret']
    self.consumer_key = authentication['consumerKey']
    self.consumer_secret = authentication['consumerSecret']

if __name__ == '__main__':
 # ra= read_auth('/content/twitter.txt')
  auth = tweepy.OAuthHandler('GPxXPgFX0vx59OFH9ARJnnL2S', 'MxIiK36c1ExFsLMH6d7C9dWGYG1fCRTYuErE6OCqD6yzzsISkd')
  auth.set_access_token('70897288-1PZGyKKX7Gdj4J1AITEkKR9GYHf8yxPpbGJeZT7Pk', 'lXjKbcDv5rOtyy8o9a7y8PiHxXELL0d9SVsm2D6XMAT7D')
  api = tweepy.API(auth)
  myStreamListener = MyStreamListener()
  myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
  # region=[94.716668,-10.222998,141.673661,6.491309]
  # myStream.filter(locations = region)
  myStream.filter(track=['earthquake','gempa','gempabumi'])