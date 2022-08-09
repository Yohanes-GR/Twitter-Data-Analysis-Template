import unittest
import pandas as pd
import sys, os

sys.path.append(os.path.abspath(os.path.join("../..")))

from extract_dataframe import read_json
from extract_dataframe import TweetDfExtractor

# For unit testing the data reading and processing codes, 
# we will need about 5 tweet samples. 
# Create a sample not more than 10 tweets and place it in a json file.
# Provide the path to the samples tweets file you created below
sampletweetsjsonfile = ""   #put here the path to where you placed the file e.g. ./sampletweets.json. 
_, tweet_list = read_json("../data/global_twitter_data.json")

columns = [
    "created_at",
    "source",
    "original_text",
    "clean_text",
    "sentiment",
    "polarity",
    "subjectivity",
    "lang",
    "favorite_count",
    "retweet_count",
    "original_author",
    "screen_count",
    "followers_count",
    "friends_count",
    "possibly_sensitive",
    "hashtags",
    "user_mentions",
    "place",
    "place_coord_boundaries",
]


class TestTweetDfExtractor(unittest.TestCase):
    """
		A class for unit-testing function in the fix_clean_tweets_dataframe.py file

		Args:
        -----
			unittest.TestCase this allows the new class to inherit
			from the unittest module
	"""

    def setUp(self) -> pd.DataFrame:
        self.df = TweetDfExtractor(tweet_list)
        self.df.df=self.df[:5]
        # tweet_df = self.df.get_tweet_df()

    def test_find_statuses_count(self):
        self.assertEqual(
            self.df.find_statuses_count(),[8097,5831,1627,18958,4173] #<provide a list of the first five status counts>
        )

    def test_find_full_text(self):
        text = ['Lets focus in one very specific zone of the western coast -&gt; China,s media explains the military reasons for each area of the drills in the #Taiwan Strait,I told you my friend, Taiwan will be a vassal state, including nukes, much like the Ukrainian model. I warned you... But it took Pelosi to open Chinas eyes.,I’m sorry, I thought Taiwan was an independent country because it had its own government, currency, military, travel,We must be ready.We must defend #Taiwan']#<provide a list of the first five full texts>

        self.assertEqual(self.df.find_full_text(), text)

    def test_find_sentiments(self):
        self.assertEqual(self.df.find_sentiments(self.df.find_full_text()),([0.0, 0.13333333333333333, 0.316666666666666, 0.08611111111111111, 0.27999999999999997], [0.18888888888888888, 0.45555555555555555, 0.48333333333333334, 0.19722222222222224, 0.6199999999999999]));
            


    def test_find_screen_name(self):
        name = ['IndoPac_Info', 'ZIisq', 'ZelenskyyUa','Fin21Free',  'Fin21Free', 'ChinaUncensored']
        self.assertEqual(self.df.find_screen_name(), name)

    def test_find_followers_count(self):
        f_count = [910, 207, 12, 870, 127]#<provide a list of the first five follower counts>
        self.assertEqual(self.df.find_followers_count(), f_count)

    def test_find_favourite_count(self):
        self.assertEqual(self.df.find_favourite_count(), [548, 195, 2, 1580, 72])

    def test_find_retweet_count(self):
        self.assertEqual(self.df.find_retweet_count(), [0, 0, 0, 0, 0])

    def test_find_hashtags(self):
         self.assertEqual(self.df.find_hashtags(), [ [], [],[{'indices': [103, 116], 'text': 'red4research'}],[], []])

    def test_find_mentions(self):
         self.assertEqual(self.df.find_mentions(), '')

    def test_find_location(self):
        self.assertEqual(self.df.find_location(), ['Florida, ', 'El mundo periférico', None, None, 'Netherlands'])





if __name__ == "__main__":
    unittest.main()

