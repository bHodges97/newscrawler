import tweepy as tw
from keys import *
import newspaper
import os
import time
import csv
from newspaper import Article
from multiprocessing import Pool
import logging
logging.basicConfig(filename="log.log", level=logging.INFO)

filepath = "../news_outlets.txt"
downloadpath = "./data"
skipto =  0 #if script was interupted, use this to skip over parsed tweets
batchsize = 100 #100 tweets ber request (api limit)
processpoolsize = 4

auth = tw.OAuthHandler(key, secret)
api = tw.API(auth)
blacklist = ["https://twitter.com","https://youtube.com"]

#print("reading tweets files")
#with open(filepath,"r") as f:
#    for i,line in enumerate(f):
#        pass
#    linecount = i+1
linecount = 39695156
print(linecount, "tweets to process")

if not os.path.exists(downloadpath):
    os.makedirs(downloadpath)

def addtweet(tweet):
    if len(tweet.entities["urls"]) == 1:
        link = tweet.entities["urls"][0]
        expanded = link["expanded_url"]
        if not any([expanded.startswith(x) for x in blacklist]):
            try:
                article = Article(expanded)
                article.download()
                article.parse()
                fields = [tweet.id_str,tweet.created_at,expanded,article.title,article.authors,article.text]
                return (tweet.user.id_str,fields)
            except (newspaper.article.ArticleException,ValueError) as e:
                pass
    return None

def gettweets(tweets):
    while True:
        try:
            tweets = api.statuses_lookup(bulkids)
            print("Retrieved ",len(tweets), "tweets")
            break
        except tw.TweepError as e:
            time.sleep(60)#avoid rate limiting
            logging.exception("twitter api")
    try:
        stime = time.time()
        articles = [x for x in pool.imap_unordered(addtweet,tweets) if x]
        for user,fields in articles:
            filename = downloadpath + "/" + user
            with open(filename, 'a') as csvf:
                writer = csv.writer(csvf,quoting=csv.QUOTE_ALL)
                writer.writerow(fields)
        print(f"Parse complete. Writing {len(articles)} entries to file. Time taken:",time.time()-stime)
                 
    except Exception as e:
        print(e)
        logging.exception("pool error")


if __name__ == "__main__":
    pool = Pool(processes=processpoolsize)
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
                progress =f"Progress {idx}/{linecount} ({idx/linecount:.2f})" 
                print(progress)
                with open("status","w") as f:
                    f.write(progress)
                
            if os.path.exists("stop"):
                print(idx)
                exit()

