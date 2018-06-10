# graph = Graph(bolt=False, host="localhost", http_port=7687, user='neo4j', password='11111')

##################################################################

# with open('top21_30coins_2018_05_10_to_19.json') as data_file:
#     data = json.load(data_file)
# data = {"tweets": {}}
# twtList = []
# twtInfo = {'id':}

### Using py2neo to create graph
# from py2neo import Graph, Node, Relationship
# g = Graph()
# tx = g.begin()
# a = Node("Tweet", name="Alice")
# tx.create(a)
# b = Node("Person", name="Bob")
# ab = Relationship(a, "KNOWS", b)
# tx.create(ab)
# tx.commit()
# g.exists(ab)

"""WITH {json} AS document
UNWIND document.tweets AS tweets
UNWIND tweets.hashtags AS hashtag
RETURN tweets.tid, tweets.created_at, hashtag"""

# """
#     //Hashtag->Hashtag
#     MERGE (h)-[rh:together]->(h)
#     ON CREATE SET rh.count = 1
#     ON MATCH  SET rh.count = coalesce(rh.count, 0)+1
#     """
import json
from py2neo import Graph, authenticate
from neo4j.v1 import GraphDatabase
import os
import logging
import datetime

### Setup logging
logging.basicConfig(filename='G:\\work\\TwitterAPI\\Neo2\\insert_history.log',level=logging.INFO, format='%(asctime)s.%(msecs)03d : %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

## data
data = {"tweets": [
    {
        "hashtags": [
            "bitcoin",
            "ethereum",
            "Bitcoin",
            "ico",
            "binance",
            "xoxo"
        ],
        "tweet": "Big shoutout to @devnullius , Bitto's number one source for Crypto gossips! :)\nJust kidding about gossips of course - Devvie is a true crypto expert, make sure to follow them!\n\n#bitcoin #ethereum #ico #binance #xoxo https://t.co/yRCaIm7LP1",
        "tid": 995453567296159744,
        "created_at": "2018-05-12 23:59:59",
        "country": None,
        "link_count": 0,
        "uid": 975044115317403649,
        "screen_name": "mu_hoc",
        "followers_count": 12,
        "friends_count": 103,
        "mentioned_uids": [
            940886642197274625,
            41140413
        ],
        "reply_to_tid": None,
        "reply_to_uid": None,
        "retweet_from_tid": 995353213896417281,
        "retweet_from_uid": 940886642197274625
    },
    {
        "hashtags": [
            "HappyWeekend",
            "crypto",
            "Telegram",
            "LetsMakeItReal",
            "Cryptocurrency",
            "Blockchain",
            "ToTheMOON",
            "xoxo"
        ],
        "tweet": "#HappyWeekend to all the #crypto community ",
        "tid": 995453565689790466,
        "created_at": "2018-05-12 23:59:59",
        "country": None,
        "link_count": 0,
        "uid": 3255922813,
        "screen_name": "emtseth",
        "followers_count": 2515,
        "friends_count": 1546,
        "mentioned_uids": [
            954521193205436417
        ],
        "reply_to_tid": None,
        "reply_to_uid": None,
        "retweet_from_tid": 995327759110037505,
        "retweet_from_uid": 954521193205436417
    }
]
}

# Connect to database
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "11111"))

# create nodes
# with driver.session() as session:
#     tx = session.begin_transaction()
#     for t in data['tweets']:
#         for h in t['hashtags']:
#             # Open transaction
#             q0 = """ Create (h1:Hashtag{tag: $tag1})"""
#             ret = tx.run(q0, tag1=h)
#             # ret = tx.run(q, tag1=hashtag[i], tag2=hashtag[j])
#     tx.commit()


## test json import
with driver.session() as session:
    # Open transaction
    tx = session.begin_transaction()

    q = """WITH {json} AS document
                UNWIND document.tweets AS tweets
                UNWIND tweets.hashtags AS hashtag
                //Hashtag
                MERGE (h:Hashtag {tag: hashtag})
                    ON CREATE SET
                        h.tag = hashtag
                RETURN count(h)
                """
    ret = tx.run(q, json=data).single().value()
    print(ret)
    # ret = tx.run(q, tag1=hashtag[i], tag2=hashtag[j])
    tx.commit()

    # Open transaction
    tx = session.begin_transaction()

    q = """WITH {json} AS document
                    UNWIND document.tweets AS tweets
                    UNWIND tweets.hashtags AS hashtag
                    //Hashtag
                    MERGE (h:Hashtag {tag: hashtag})
                        ON CREATE SET
                            h.tag = hashtag
                    RETURN count(h)
                    """
    ret = tx.run(q, json=data).single().value()
    print(ret)
    # ret = tx.run(q, tag1=hashtag[i], tag2=hashtag[j])
    tx.commit()


# add hash-hash relationship
with driver.session() as session:
    # Open transaction
    tx = session.begin_transaction()
    for t in data['tweets']:
        hashtag = t['hashtags']
        hashLen = len(hashtag)
        # Loop matching hashtag but not include previous eg. 3 tags -> (1,2),(1,3),(2,3) -> not include 2,1 /2,2 / 3,2
        for i in range(0, hashLen - 1):
            for j in range(i + 1, hashLen):
                q = """ MATCH (h1:Hashtag{tag: $tag1}),
                              (h2:Hashtag{tag: $tag2})
                        MERGE (h1)-[r:together]-(h2)
                        ON CREATE SET r.count = 1
                        ON MATCH SET r.count = coalesce(r.count,0) + 1
                        RETURN count(r)"""
                ret = tx.run(q, tag1=hashtag[i], tag2=hashtag[j]).single().value()
    tx.commit()
print(str(ret)+" Hash-hash links added..")
logging.info(str(ret)+" Hash-hash links added..")

# with open("G:/work/TwitterAPI/data/top1/test.json") as f:
#     data = json.load(f)

#
# q = """ MERGE (h1:Hashtag {tag: hh1})-(r:together)-(h2:Hashtag {tag: hh2})
#           ON CREATE SET r.count = 1
#           ON MATCH SET
#             r.count = coalesce(r.count, 0) + 1 """;
#
# q_build_initial_graph = """WITH {json} AS document
#     UNWIND document.tweets AS tweets
#     UNWIND tweets.hashtags AS hashtag
#     //Tweet
#     MERGE (t:Tweet {id: tweets.tid})
#         ON CREATE SET
#             t.id = tweets.tid,
#             t.text = tweets.tweet,
#             t.created_at = tweets.created_at,
#             t.country = tweets.country,
#             t.link_count = tweets.link_count
#     //Users
#     MERGE (u:User {id: tweets.uid})
#         ON CREATE SET
#             u.id = tweets.uid,
#             u.screen_name = tweets.screen_name,
#             u.follower_count = tweets.friends_count,
#             u.following_count = tweets.followers_count
#     //Hashtag
#     MERGE (h:Hashtag {tag: toLower(hashtag)})
#         ON CREATE SET
#             h.tag = toLower(hashtag)
#
#     //Tweet->Hashtag
#     MERGE (t)-[:has]->(h)
#
#     //User->Tweet
#     MERGE (u)-[:post]->(t)
#
#     RETURN count(t)
#     """
# q = q_build_initial_graph
# a = graph.run(q, json=data).dump()
# print(a)

