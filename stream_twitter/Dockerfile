FROM python:3.7

RUN pip install --upgrade pip
RUN pip install tweepy
RUN pip install --upgrade google-api-python-client
RUN pip install --upgrade oauth2client
RUN pip install python-dateutil
RUN pip install --upgrade google-cloud-pubsub

ADD twitter-to-pubsub.py /twitter-to-pubsub.py

CMD python twitter-to-pubsub.py