import json
from py2neo import Graph, authenticate
from neo4j.v1 import GraphDatabase
import os
import logging
import datetime

### Setup logging
# logging.basicConfig(filename='G:\\work\\TwitterAPI\\Neo2\\insert_history.log',level=logging.INFO, format='%(asctime)s.%(msecs)03d : %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

## data
data = {"tweets": [
    {
        "hashtags": [
            "bitcoin",
            "ethereum",
            "xoxo",
            "test1",
            "test2"
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
            "bitcoin",
            "xoxo",
            "ethereum"
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
#
# # Connect to database
# uri = "bolt://localhost:7687"
# driver = GraphDatabase.driver(uri, auth=("neo4j", "11111"))
#
# # create nodes
# # with driver.session() as session:
# #     tx = session.begin_transaction()
# #     for t in data['tweets']:
# #         for h in t['hashtags']:
# #             # Open transaction
# #             q0 = """ Create (h1:Hashtag{tag: $tag1})"""
# #             ret = tx.run(q0, tag1=h)
# #             # ret = tx.run(q, tag1=hashtag[i], tag2=hashtag[j])
# #     tx.commit()
#
#
# ## test json import
# with driver.session() as session:
#     # Open transaction
#     tx = session.begin_transaction()
#
#     q = """WITH {json} AS document
#                 UNWIND document.tweets AS tweets
#                 UNWIND tweets.hashtags AS hashtag
#                 //Hashtag
#                 MERGE (h:Hashtag {tag: hashtag})
#                     ON CREATE SET
#                         h.tag = hashtag
#                 RETURN count(h)
#                 """
#     ret = tx.run(q, json=data).single().value()
#     print(ret)
#     # ret = tx.run(q, tag1=hashtag[i], tag2=hashtag[j])
#     tx.commit()
#
#     # Open transaction
#     tx = session.begin_transaction()
#
#     q = """WITH {json} AS document
#                     UNWIND document.tweets AS tweets
#                     UNWIND tweets.hashtags AS hashtag
#                     //Hashtag
#                     MERGE (h:Hashtag {tag: hashtag})
#                         ON CREATE SET
#                             h.tag = hashtag
#                     RETURN count(h)
#                     """
#     ret = tx.run(q, json=data).single().value()
#     print(ret)
#     # ret = tx.run(q, tag1=hashtag[i], tag2=hashtag[j])
#     tx.commit()
#
#
# # add hash-hash relationship
# with driver.session() as session:
#     # Open transaction
#     tx = session.begin_transaction()
#     for t in data['tweets']:
#         hashtag = t['hashtags']
#         hashLen = len(hashtag)
#         # Loop matching hashtag but not include previous eg. 3 tags -> (1,2),(1,3),(2,3) -> not include 2,1 /2,2 / 3,2
#         for i in range(0, hashLen - 1):
#             for j in range(i + 1, hashLen):
#                 q = """ MATCH (h1:Hashtag{tag: $tag1}),
#                               (h2:Hashtag{tag: $tag2})
#                         MERGE (h1)-[r:together]-(h2)
#                         ON CREATE SET r.count = $count
#                         ON MATCH SET r.count = coalesce(r.count,0) + $count
#                         RETURN count(r)"""
#                 ret = tx.run(q, tag1=hashtag[i], tag2=hashtag[j], count=count).single().value()
#     tx.commit()
# print(str(ret)+" Hash-hash links added..")
# logging.info(str(ret)+" Hash-hash links added..")


class Hash2hash:
    def __init__(self, tag1, tag2):
        self.tag1 = tag1
        self.tag2 = tag2
        self.count = 1


def count_hash2hash(hashtags):
    ret = []
    # Distinct the hashtag list first
    hashtags = list(set(hashtags))

    # count co-occurrence hashtag
    hashLen = len(hashtags)

    # Loop matching hashtag but not include previous eg. 3 tags -> (1,2),(1,3),(2,3) -> not include 2,1 /2,2 / 3,2
    for i in range(0, hashLen - 1):
        for j in range(i + 1, hashLen):
            pair = Hash2hash(hashtags[i], hashtags[j])
            ret.append(pair)
    return ret


def count_hash2hash_dict(hashtags):
    ret = {}
    # Distinct and sort the hashtag list
    hashtags = list(set(hashtags))
    hashtags.sort()

    # count co-occurrence hashtag
    hashLen = len(hashtags)

    # Loop matching hashtag but not include previous eg. 3 tags -> (1,2),(1,3),(2,3) -> not include 2,1 /2,2 / 3,2
    for i in range(0, hashLen - 1):
        for j in range(i + 1, hashLen):
            key = hashtags[i] + "," + hashtags[j]
            ret[key] = 1
    return ret

file_list = [
             "G:\\work\\TwitterAPI\\data\\used_data\\test\\top1_2018-05-26_to_2018-05-27.json"]
tweetList = []
for filename in file_list:
    # Load json to dict
    with open(filename) as f:
        data = json.load(f)

    #### V2 - collection of hash2hash with count
    hash_collection = {}
    start = datetime.datetime.now()
    print(str(start) + ' - processing..')
    for t in data['tweets']:
        # get pairs of hash2hash from set of hashtags

        hashDict = count_hash2hash_dict(t['hashtags'])
        # print(str(datetime.datetime.now() - start) + ' - hashtags takes')

        # Update the collection
        for key_h in hashDict:
            found = 0

            # start = datetime.datetime.now()
            for key_hc in hash_collection:
                # If sorted, no need to check hashtag for 2 direction
                if key_hc == key_h:
                    hash_collection[key_hc] += hashDict[key_h]
                    found = 1
                    break
                # split key to tag
                # tag1, tag2 = key_h.split(sep=",")
                # col_tag1, col_tag2 = key_hc.split(sep=",")
                #
                # # Update if hash2hash already existed
                # if (tag1 == col_tag1 or tag1 == col_tag2) and (tag2 == col_tag1 or tag2 == col_tag2):
                #     hash_collection[key_hc] += hashDict[key_h]
                #     found = 1
                #     break
            # # Append, if not found in collection
            if found == 0:
                hash_collection[key_h] = hashDict[key_h]

            # print(str(datetime.datetime.now() - start) + ' - add collection takes')
    # add to list for each set of crypto's hash
    tweetList.append(hash_collection)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ' Done '+ filename + ' with '+ str(len(hash_collection))+'/'+str(datetime.datetime.now() - start) + ' pairs/times')

# ##### V1 - collection of hash2hash with count
# hash_collection = []
# for t in data['tweets']:
#     # get pairs of hash2hash from set of hashtags
#     hashList = count_hash2hash(t['hashtags'])
#     # Update the collection
#     for h in hashList:
#         found = 0
#         for c in hash_collection:
#             # Update if hash2hash already existed
#             if  (h.tag1 == c.tag1 or h.tag1 == c.tag2) and (h.tag2 == c.tag1 or h.tag2 == c.tag2):
#                 c.count += h.count
#                 found = 1
#                 break
#         # Append, if not found in collection
#         if found == 0:
#             hash_collection.append(h)
# print(len(hash_collection))



# len(tweetList[1])
import pickle
def load_object(filename):
    with open(filename, 'rb') as input:
        obj = pickle.load(input)
    return obj

def save_object(obj, filename):
    with open(filename, 'wb') as output:  # Overwrites any existing file.
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)

# sample usage
save_object(hash_collection, 'twt_top1.pkl')

obj = load_object('twt_top6-30_2-5.pkl')
top6_30 = obj[0]
top2_5 = obj[1]
top1 = load_object('twt_top6-30_2-5.pkl')

len(top6_30)
len(top2_5)
len(top1)

import operator
import csv
import sys

x = top1
sorted_x = sorted(x.items(), key=operator.itemgetter(1), reverse=True)

coins = {
     "bitcoin": ["bitcoin","btc"],
    # "ethereum": ["ethereum","eth","ether"],
    # "ripple": ["ripple","xrp"],
    # "bitcoincash": ["bitcoincash","bch"],
    # "eosio": ["eosio","eos"],
    # "litecoin":  ["litecoin", "ltc"],
    # "cardano":   ["cardano", "ada"],
    # "stellar":   ["stellar", "xlm"],
    # "iota":      ["iota", "miota"],
    # "tron":      ["tron", "trx"],
    # "neo":       ["neo"],
    # "monero":    ["monero", "xmr"],
    # "dash":      ["dash"],
    # "nem":       ["nem", "xem"],
    # "tether":    ["tether", "usdt"],
    # "vechain":   ["vechain", "ven"],
    # "ethereum_classic":  ["ethereumclassic", "etc"],
    # "bytecoin":          ["bytecoin", "bcn"],
    # "binance_coin":      ["binancecoin", "bnb"],
    # "qtum":              ["qtum"],
    # "zcash":             ["zcash", "zec"],
    # "icon":              ["icx"],
    # "omisego":           ["omisego"],
    # "lisk":              ["lisk", "lsk"],
    # "zilliqa":           ["zilliqa", "zil"],
    # "bitcoingold":       ["bitcoingold", "btg"],
    # "aeternity":         ["aeternity", "ae"],
    # "ontology":          ["ontology", "ont"],
    # "verge":             ["verge", "xvg"],
    # "steem":             ["steem"]
}
top=20
coins_h2h = {}
for k, v in coins.items():
    h2hcount_list=[]
    for h2hcount in sorted_x:
        countMatch = 0
        # check if h2h contain considered crypto hashtag
        for hash in v:
            countMatch += h2hcount[0].startswith(hash) + h2hcount[0].endswith(hash)
        # If h2h is in the hashtag list, add to h2hcount list
        if countMatch > 0:
            h2hcount_list.append(h2hcount)
    # add h2hcount list to the coins list
    coins_h2h[k] = h2hcount_list

for c in coins_h2h:
    outdata = coins_h2h[c][0:20]
    # print(outdata)
    with open(c+'.csv', 'w') as f:
        writer = csv.writer(f , lineterminator='\n')
        for tup in outdata:
            print(tup)
            a = list(tup)
            a[0] = a[0].encode(sys.stdout.encoding, errors='replace')
            writer.writerow(a)
