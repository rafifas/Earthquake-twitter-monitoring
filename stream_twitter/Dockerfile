FROM python:3

RUN pip install --upgrade pip
RUN pip install tweepy

ADD twitter-to-pubsub.py /twitter-to-pubsub.py

CMD python twitter-to-pubsub.py