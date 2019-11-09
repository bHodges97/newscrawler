import tweepy as tw
from keys import *
import newspaper
from newspaper import Article
import os
import time
import threading
from queue import Queue
q=Queue()

filepath = "../news_outlets.txt"
downloadpath = "./data"
auth = tw.OAuthHandler(key, secret)
api = tw.API(auth)
import csv

#skipto = 0

if not os.path.exists(downloadpath):
    os.makedirs(downloadpath)
print("reading tweets files")
#with open(filepath,"r") as f:
#    for i,line in enumerate(f):
#        pass
#    linecount = i+1
linecount = 39695156
print(linecount, "tweets to processes")
skipto = 236430

batchsize = 100

def flushQueue():
    while q.qsize():
        filename,fields = q.get()
        with open(filename, 'a') as csvf:
            writer = csv.writer(csvf,quoting=csv.QUOTE_ALL)
            writer.writerow(fields)
            print("Article added successfully:", fields[3])

def addtweet(tweet):
    #tweet = api.get_status(tweetid)
    filename = downloadpath + "/" + tweet.author.id_str
    if len(tweet.entities["urls"]) == 1:
        link = tweet.entities["urls"][0]
        expanded = link["expanded_url"]
        if expanded.startswith("https://twitter.com/i/"):
            #print("Links is twitter status! Skipping")
            return
        try:
            article = Article(expanded)
            article.download()
            article.parse()
            fields = [tweetid,tweet.created_at,expanded,article.title,article.authors,article.text]
            q.put((filename,fields))
        except (newspaper.article.ArticleException,ValueError) as e:
            pass
            #print(e)
    else:
        pass
        #print("No links found!");

def gettweets(tweets):
    try:
        tweets = api.statuses_lookup(bulkids)
    except tw.TweepError as e:
        time.sleep(0.1)#avoid rate limiting
        return

    print("Retrieved ",len(tweets), "tweets")
    threads = []
    for tweet in tweets:
        t = threading.Thread(target=addtweet, args = (tweet,) )
        threads.append(t)
        t.start()
    for t in threads:
        t.join()                
        
    flushQueue()

with open(filepath,"r") as f:
    bulkids = []
    for idx,line in enumerate(f):
        if idx < skipto:
            continue
        tweetid = line[:-1]
        bulkids.append(tweetid)

        if len(bulkids) == batchsize:
            gettweets(bulkids)
            bulkids = []
            print(f"Progress {idx}/{linecount}" ,f"({idx/linecount:.2f})")

        if os.path.exists("stop"):
            print(idx)
            exit()
