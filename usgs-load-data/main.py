#!/usr/bin/python

import requests
import json
import datetime 
import io
from google.cloud import storage
 
def get_time():
    now = datetime.datetime.utcnow()
    timedelta = datetime.timedelta(days=10)
    starttime = (now - timedelta).strftime("%Y-%m-%dT%H:%M:%S")
    endtime = now.strftime("%Y-%m-%dT%H:%M:%S")
    return [starttime, endtime]

def full_url(starttime, endtime):
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
    minlatitude=str(-10.222998)
    maxlatitude=str(6.491309)
    minlongitude=str(94.716668)
    maxlongitude=str(141.673661)
    limit=str(20000)
    starttime = get_time()[0]
    endtime = get_time()[1]
    fullurl = url+'&starttime='+starttime+'&endtime='+endtime+'&minlatitude='+minlatitude+'&maxlatitude='+maxlatitude+'&minlongitude='+minlongitude+'&maxlongitude='+maxlongitude+'&limit'+limit
    return fullurl

def upload_blob(text, starttime, endtime):
    bucket_name = "usgs_earthquake_data"
    destination_blob_name = f"usgs_{starttime}_{endtime}.json"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    output = io.StringIO(text)
    blob.upload_from_string(output.read(), content_type='text/plain', timeout=60)
    output.close() 

def main(args):
    # get time
    starttime = get_time()[0]
    endtime = get_time()[1]

    #define url
    in_json = requests.get(full_url(starttime, endtime)).json()
    result =in_json["features"]
    nd_json = ('\n'.join([json.dumps(record) for record in result]))

    #upload file
    upload_blob(nd_json, starttime, endtime)