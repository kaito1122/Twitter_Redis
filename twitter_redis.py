# Kaito Minami
# DS4300: Large-scale Information Storage and Retrieval

import redis
import csv
from datetime import datetime
import random as rand
from perf_tester import perf_tester

"""
ORGANIZATION OF KEYS FOR HW2:

(RECOMMENDATIONS / IDEAS)
What are the keys and key categories that we need to manage?

KEY                  VALUE
tweet:<id>           Serialized string, HASH
timeline:<userid>    LIST of tweets or tweet keynames or tweet ids
                     OR SORTED SET?
followers:<userid>   LIST of user ids   5 has three followers [6, 7, 8]
followees:<userid>   LIST of user ids

Optional:
hashtag:<word>       LIST of tweet ids that contain that hashtag
hashtag:neu          6, 8, 10, ..... ---> tweet:6. tweet:8. tweet:10
"""

class TwitterRedisAPI:

    def __init__(self):
        # Create a connection
        self.r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.r.flushall()

        self.account_added()

    def account_added(self):
        """ loads the follows.csv content to follows table programmatically """
        # sql_script = "INSERT INTO follows VALUES (?, ?)"
        follows_data = "data/follows.csv"

        with open(follows_data, 'r') as f:
            # reads csv file, separated by commas
            reader = csv.reader(f, delimiter=',')
            # skips header
            next(reader)
            for accounts in reader:
                self.r.lpush(f'followee_of:{accounts[0]}', accounts[1])
                self.r.lpush(f'follower_of:{accounts[1]}', accounts[0])
                self.r.sadd('user:', accounts[0], accounts[1])

    def my_user_id(self):
        """ randomly selected twitter user id for a test login use """
        return self.r.srandmember('user:')

    @perf_tester
    def postTweets(self):
        """ posts tweet one at a time to tweet table """
        tweet_data = "data/tweet.csv"
        tweet_id = 0

        with open(tweet_data, 'r') as f:
            # read csv file, separated by commas
            reader = csv.reader(f, delimiter=',')
            # skips the header
            next(reader)
            for tweets in reader:
                # insert the tweet_id, user_id, tweet_ts, and tweet_text.
                tweet = str(tweet_id) + ': ' + tweets[0] + ': ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': '+ tweets[1]
                followers = self.getFollowers(tweets[0], show=False)
                for f in followers:
                    self.r.sadd(f'timeline:{f}', tweet)
                tweet_id += 1

    @perf_tester
    def postTweetsStr1(self):
        """ posts tweet one at a time to tweet table """
        tweet_data = "data/tweet.csv"
        tweet_id = 0

        with open(tweet_data, 'r') as f:
            # read csv file, separated by commas
            reader = csv.reader(f, delimiter=',')
            # skips the header
            next(reader)
            for tweets in reader:
                # insert the tweet_id, user_id, tweet_ts, and tweet_text.
                tweet = str(tweet_id) + ': ' + tweets[0] + ': ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': ' + tweets[1]
                self.r.set(f'tweet1:{tweet_id}', tweet)
                self.r.sadd(f'tweet1_by:{tweets[0]}', tweet_id)
                tweet_id += 1

    @perf_tester
    def getHomeTimeline(self, login='1', show=True):
        """ gets 10 random tweets by the user's followees in the most recent order """
        if show:
            print('Logged in as user_id: ', login)
        # randomly retrieve 10 tweets from timeline
        timeline = self.r.srandmember(f'timeline:{login}', 10)
        # sort the timeline by time stamp
        timeline.sort(key=lambda x: x.split(': ')[2], reverse=True)
        return timeline

    @perf_tester
    def getHomeTimelineStr1(self, login='1', show=True):
        """ gets 10 random tweets by the user's followees in the most recent order """
        if show:
            print('Logged in as user_id: ', login)
        # gets followees list
        user_list = self.getFollowees(login, show=False)
        timeline = list()

        # from randomly picked followee, randomly retrieve tweets, and append it on timeline
        for i in range(10):
            user = rand.choice(user_list)
            tid = self.r.srandmember(f'tweet1_by:{user}')
            timeline.append(self.r.get(f'tweet1:{tid}'))

        # sort the timeline by timestamp
        timeline.sort(key=lambda x: x.split(': ')[2], reverse=True)

        return timeline

    def getFollowees(self, login='1', show=True):
        """ gets the list of followees by logged in random user """
        if show:
            print('Logged in as user_id: ', login)
            print('Followees:')

        return self.r.lrange(f'followee_of:{login}', 0, -1)

    def getFollowers(self, login='1', show=True):
        """ gets the list of followers by logged in random user """
        if show:
            print('Logged in as user_id: ', login)
            print('Followers:')

        return self.r.lrange(f'follower_of:{login}', 0, -1)
