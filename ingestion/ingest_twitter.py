import tweepy
import os, sys
sys.path.append(os.getcwd())
from ingestion.utils import load_json

config = load_json('ingestion/config.json')

bearer_token = config.get("TWITTER_BEARER_TOKEN")

# Your app's API/consumer key and secret can be found under the Consumer Keys
# section of the Keys and Tokens tab of your app, under the
# Twitter Developer Portal Projects & Apps page at
# https://developer.twitter.com/en/portal/projects-and-apps
consumer_key = config.get("TWITTER_API_KEY")
consumer_secret = config.get("TWITTER_API_KEY_SECRET")

# Your account's (the app owner's account's) access token and secret for your
# app can be found under the Authentication Tokens section of the
# Keys and Tokens tab of your app, under the
# Twitter Developer Portal Projects & Apps page at
# https://developer.twitter.com/en/portal/projects-and-apps
access_token = config.get("TWITTER_ACCESS_TOKEN")
access_token_secret = config.get("TWITTER_ACCESS_TOKEN_SECRET")

# You can authenticate as your app with just your bearer token
client = tweepy.Client(bearer_token=bearer_token)

# You can provide the consumer key and secret with the access token and access
# token secret to authenticate as a user
# client = tweepy.Client(
#     consumer_key=consumer_key, consumer_secret=consumer_secret,
#     access_token=access_token, access_token_secret=access_token_secret
# )

response = client.search_recent_tweets("BTC")

print(response.meta)