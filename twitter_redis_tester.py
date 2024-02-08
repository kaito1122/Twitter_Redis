# Kaito Minami
# DS4300: Large-scale Information Storage and Retrieval

from twitter_redis import TwitterRedisAPI

def main():
    api = TwitterRedisAPI()

    print(api.postTweets())
    print(api.postTweetsStr1())
    print(api.getHomeTimeline(show=False))
    print(api.getHomeTimelineStr1(show=False))
    print(api.getFollowees(api.my_user_id()))
    print(api.getFollowers(api.my_user_id()))

if __name__ == "__main__":
    main()
