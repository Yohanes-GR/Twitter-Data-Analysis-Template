#python library
import json
import pandas as pd
from textblob import TextBlob
from zipfile import ZipFile

#read the json file 
def read_json(json_file: str)->list:
    tweets_data = []
    for tweets in open("data/global_twitter_data.json",'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data
class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list
  
    def find_full_text(self)->list:
        text = []
        for x in self.tweets_list:
            try:
                text.append(x['retweeted_status']['extended_tweet']['full_text'])
            except KeyError:
                text.append(x['full_text'])
        
        return text
    #find sentiments of text using TextBlob
    def find_sentiments(self, full_text)->list:

        polarity = [TextBlob(x).polarity for x in full_text]
        subjectivity = [TextBlob(x).subjectivity for x in full_text]
        
        return polarity, subjectivity
    #created time of tweet
    def find_created_time(self)->list:
       created_at = [x["created_at"] for x in self.tweets_list]
       
       return created_at
    #source of tweet text
    def find_source(self)->list:
        source = [["source"] for x in self.tweets_list]

        return source

    def is_sensitive(self)->list:
        is_sensitive=[]
        for tweet in self.tweets_list:
            if 'possibly_sensitive' in tweet.keys():
                is_sensitive.append(tweet['possibly_sensitive'])
            else: is_sensitive.append(None)

        return is_sensitive
    #find favourite count
    def find_favourite_count(self)->list:
        favourite_count = [x.get("retweeted_status", {}).get("favourite_count", 0) for x in self.tweets_list]
        
        return favourite_count
    #retweet count
    def find_retweet_count(self) -> list:
        retweet_count = []
        for tweet in self.tweets_list:
            if 'retweeted_status' in tweet.keys():
                retweet_count.append(
                    tweet['retweeted_status']['retweet_count'])
            else:
                retweet_count.append(0)
        return retweet_count
    def find_lang(self)->list:
        lang = [x['lang'] for x in self.tweets_list]

        return lang
    #hash tag count
    def find_hashtags(self)->list:
        hashtags =[]
        for tweethash in self.tweets_list:
            hashtags.append(", ".join([hashtag_item['text'] for hashtag_item in tweethash['entities']['hashtags']]))

        return hashtags
    #mention
    def find_mentions(self)->list:
        mentions = []
        for tweethash in self.tweets_list:
            mentions.append(", ".join([mention['screen_name'] for mention in tweethash['entities']['user_mentions']]))

        return mentions
    #location of tweet
    def find_location(self)->list:
        location = []
        for tweet in self.tweets_list:
            location.append(tweet['user']['location'])

        """try:
            location = self.tweets_list['user']['location']
        except TypeError:
            location = ''
        """
        return location
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        #columns for global datasets
        columns = ['created_at', 'source', 'full_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count','possibly_sensitive','hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        full_text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(full_text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        sensitive  = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, full_text, polarity, subjectivity, lang, fav_count, retweet_count,sensitive,hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('data/processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

#main code 
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'full_text','polarity','subjectivity','lang','favorite_count','retweet_count','possibly_sensitive','hashtags', 'user_mentions', 'place']
    _, tweet_list = read_json("data/global_twitter_data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df(save=True) 
