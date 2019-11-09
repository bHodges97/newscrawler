import tweepy as tw
from keys import *
import newspaper
from newspaper import Article
import os
import time

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
skipto = 233170

batchsize = 100

def addtweet(tweet,idx):
    #tweet = api.get_status(tweetid)
    filename = downloadpath + "/" + tweet.author.id_str
    if len(tweet.entities["urls"]) == 1:
        link = tweet.entities["urls"][0]
        expanded = link["expanded_url"]
        if expanded.startswith("https://twitter.com/i/"):
            print("Links is twitter status! Skipping")
            return
        try:
            article = Article(expanded)
            article.download()
            article.parse()
            fields = [tweetid,tweet.created_at,expanded,article.title,article.authors,article.text]
            with open(filename, 'a') as csvf:
                writer = csv.writer(csvf,quoting=csv.QUOTE_ALL)
                writer.writerow(fields)
                print("Article added successfully:", article.title)
        except (newspaper.article.ArticleException,ValueError) as e:
            print(e)
    else:
        print("No links found!");
    with open("status","w") as f:
        f.write(str(idx))

with open(filepath,"r") as f:
    bulkids = []
    for idx,line in enumerate(f):
        if idx < skipto:
            continue
        tweetid = line[:-1]
        bulkids.append(tweetid)

        if len(bulkids) == batchsize:
            try:
                tweets = api.statuses_lookup(bulkids)
                bulkids = []
            except tw.TweepError as e:
                print(e)
                time.sleep(0.1)#avoid rate limiting
            
            for i,tweet in enumerate(tweets):
                count = (idx+i+1) 
                percent = count / linecount
                print(tweetid,f"({count+1}/{linecount}({percent:.2f}))")
                addtweet(tweet,count)



