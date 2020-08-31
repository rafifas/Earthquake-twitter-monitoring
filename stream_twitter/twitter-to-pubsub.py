"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a PubSub topic.
"""
import base64
import datetime
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


PROJECT_NAME = os.getenv('GCP_PROJECT')
PUBSUB_TOPIC_NAME = 'projects/%s/topics/%s' % (PROJECT_NAME, 'tweets')


class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """
    count = 0
    twstring = ''
    tweets = []
    batch_size = 50
    total_tweets = 10000000
    client = pubsub.PublisherClient()
	
    def encode(data_lines):
        messages = []
        for line in data_lines:
            pub = base64.urlsafe_b64encode(line)
            messages.append({'data': pub})
        body = {'messages': messages}
        return body

    def on_data(self, data):
        """What to do when tweet data is received."""
        self.tweets.append(data)
        if len(self.tweets) >= self.batch_size:
        #    self.write_to_pubsub(self.tweets)
           messages = self.encode(self.tweets)
           self.client.publish(PUBSUB_TOPIC_NAME, messages)
           self.tweets = []
        self.count += 1
        # if we've grabbed more than total_tweets tweets, exit the script.
        if self.count > self.total_tweets:
           return False
        if (self.count % 1000) == 0:
           print (f'count is: {self.count} at {datetime.datetime.now()}')
        return True

    def on_error(self, status):
        print status

if __name__ == '__main__':
    listener = StdOutListener()
    auth = OAuthHandler('GPxXPgFX0vx59OFH9ARJnnL2S', 'MxIiK36c1ExFsLMH6d7C9dWGYG1fCRTYuErE6OCqD6yzzsISkd')
    auth.set_access_token('70897288-1PZGyKKX7Gdj4J1AITEkKR9GYHf8yxPpbGJeZT7Pk', 'lXjKbcDv5rOtyy8o9a7y8PiHxXELL0d9SVsm2D6XMAT7D')
    stream = Stream(auth, listener)
    stream.filter(track=['earthquake', 'gempa'])