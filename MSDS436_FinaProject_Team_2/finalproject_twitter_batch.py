# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 23:00:22 2022

@author: arthu
"""


import tweepy
import pandas as pd
import io
# import re
import boto3
from datetime import datetime,timedelta

base = datetime.now()-timedelta(hours=2)



# Authentication
consumerKey = "gdDo4PQroDLU2Qy1NKVt1xETx"
consumerSecret = "rjIoHcze9SM5LfUf0wj7EXt8VYQaETJHLiVYSRdfgwyuJDJa5B"
bearertoken  = "AAAAAAAAAAAAAAAAAAAAAA4TYAEAAAAAYzTuv%2FHKW8fk9VQ4TDhj8Yai38c%3DF7ijL0PF4ky3Ul1vU7uHX0jpdr9fl4fO0XfvLjTJK2WIA11r2W"
accessToken = "1481123891078733826-CnQAmCa9W2R5TbzHWxoI7ciqolKzrP"
accessTokenSecret = "kV4fjMDaliZelb8DZzgYx5nPxxKvDBTlFKFQJhBRXlk5b"
auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessToken, accessTokenSecret)
api = tweepy.API(auth)
client = tweepy.Client(bearer_token=bearertoken)


keyword = ["GME","AMD","X","NAV",
           "FORD","GM","HOOD","AMC","TSLA"]
noOfTweet = 50

boto3.setup_default_session(profile_name='msds436')

client1 = boto3.client('s3', use_ssl=False)
    
# tweets = tweepy.Cursor(api.search_tweets(50), q=keyword).items(noOfTweet)

for k in range(len(keyword)):
    df_out = pd.DataFrame()
    tweets = client.search_recent_tweets(query=keyword[k],
                                         tweet_fields=['context_annotations', 'created_at'],
                                         start_time = base,
                                         end_time = base+timedelta(hours=2),
                                         max_results=100)
    
    tweet_list = pd.DataFrame(columns = ['id','time_stamp','tweet'])
    
    for tweet in tweets:
        for i in range(len(tweet)):  
            try:
                id_ = api.get_status(tweet[i]['id'])
                created_at = id_.created_at 
                tweet_list.loc[i,'id'] = tweet[i]['id']
                tweet_list.loc[i,'time_stamp'] = str(created_at)
                tweet_list.loc[i,'tweet'] = tweet[i]['text']
            except:
                print(tweet)
    tweet_list['ticker'] = keyword[k]
    df_out = pd.concat([df_out,tweet_list])
    csv_buffer = io.StringIO()
    df_out.to_csv(csv_buffer,header=False,date_format='%Y-%m-%d %H:%M:%S.%f')
    s3_resource = boto3.resource('s3')
    s3_resource.Object('msds436-group2', f'{str(base)[:20]}/{keyword[k]}_{base}.csv').put(Body=csv_buffer.getvalue())