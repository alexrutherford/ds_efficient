'''
Script to count over tweets from last n days to determine most retweeted and
messages from users with most number of followers. Reads in daily tweet files
as outputted from process_ds_files.py. 
'''
import glob,re,sys,os,csv,json
import datetime,collections,operator
import traceback,time,bisect
from sortedcontainers import SortedSet

dataDirectory='../data_test/'
dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9].json'

topics=[u'None',u'General',u'Politics/Opinion',u'Economy',u'Risk/Disaster',u'Energy',u'Weather',u'Agriculture/Forestry',u'Oceans/Water',u'Arctic']
topicHash={'General':'general','Politics/Opinion':'politics','Economy':'economy','Risk/Disaster':'risk','Energy':'energy','Weather':'weather','Agriculture/Forestry':'agriculture','Oceans/Water':'oceans','Arctic':'arctic'}
# Maps from topics as in datasift tags and output file names

##############
def writeTopTweets(tweetCounter,tweetTopicCounter,topTopicFollowers,topFollowers):
##############
    sortedTweets=sorted(tweetCounter.iteritems(), key=operator.itemgetter(1))
    sortedTweets.reverse()
    outFile=csv.writer(open('all.top.retweet','w'),delimiter='\t')

    for t in sortedTweets[0:10]:
        outFile.writerow([t[0]])
# Write out top tweet ids for all tweets

    outFile=csv.writer(open('all.top.followers','w'),delimiter='\t')
    for t in reversed(topFollowers[0:10]):
        outFile.writerow([t[1]])
# Write out top follower count tweet id's for all tweets

    for k,v in reversed(topTopicFollowers.items()[0:10]):
        print k
        if not k=='None':
            fileName=topicHash[k]+'.top.followers'
            outFile=csv.writer(open(fileName,'w'),delimiter='\t')
            for id in reversed(SortedSet(v)):
            # Using SortedSet() as a hack as there inexplicably appears a duplicate 
            # final entry in cases of low volume
                if not id[0]==-1:
                    outFile.writerow([id[1]])
                    print '\t',id
# Write out top follow count tweet ids for tweets, by topic
            outFile=None
# Need to flush out file handle
# Sometimes last file doesn't get written otherwise

    for k,v in tweetTopicCounter.items():
        if not k=='None':
            sortedTweets=sorted(v.iteritems(),key=lambda x:x[1])
            sortedTweets.reverse()
            fileName=topicHash[k]+'.top.retweet'
            outFile=csv.writer(open(fileName,'w'),delimiter='\t')
            print k
#        print '------------'
            for t in sortedTweets[0:10]:
                print '\t',t[1],t[0][0],t[0][1]
                if t[1]>1:
                # Counts the original as a retweet, so throw out any that were tweeted only once
                    outFile.writerow([t[0][1]])
                print 'line',fileName
# Write out top retweeted tweet id's by topic
    outFile=None


##############
def inLastNDays(d,l,n=7):
##############
    fileTimes=d.partition(l+'/')[2].replace('.json','').split('_')
    fileTimes=[int(f) for f in fileTimes]
    # These are date components: 2104,7,21

#    print fileTimes,datetime.datetime(fileTimes[0],fileTimes[1],fileTimes[2])

    timeLimit=datetime.datetime.now()-datetime.timedelta(7)
#    print '\t',timeLimit   
#    print '\t',(datetime.datetime(fileTimes[0],fileTimes[1],fileTimes[2])-timeLimit).days

    timeDiff=(timeLimit-(datetime.datetime(fileTimes[0],fileTimes[1],fileTimes[2]))).days

    if timeDiff<=n:
        return True
    return False

##############
def countTweets(files):
##############
    nTotal=0
    nErrors=0
    nTopicErrors=0
    nFollowerError=0
    nInRange=0

    tweetCounter=collections.defaultdict(int)
    # Count each tweet in one big bucket
    tweetTopicCounter={}
    for t in topics:
        tweetTopicCounter[t]=collections.defaultdict(int)
# Count each tweet by topic

    topFollowers=[(-1,-1) for i in range(10)]
# This is a list, it holds 10 tweet id's with
# most number of followers
    currentTopFollower=-1
# Keep track of minimum value in top 10
# Optimises loop

    topTopicFollowers={t:[(-1,-1) for i in range(10)] for t in topics}
# Dictionary of lists

    currentTopTopicFollowers={t:-1 for t in topics}
# Likewise keep a record of current largest
# follower count
    for f in files:
        nLine=0
        for line in open(f,'r').read().split('\n')[0:-1]:
            nTotal+=1
            nLine+=1
            isRetweet=False
            try:
                tweet=json.loads(line)
            except:
                print 'PARSE ERROR',f,nLine
#                print traceback.print_exc()
#                print line
            try:

                try:
                    id=tweet['twitter']['retweeted']['id']
                    tweetCounter[id]+=1
                except:
                    id=tweet['twitter']['id']
                content=tweet['interaction']['content'].encode('utf-8').replace('\n',' ')
            except:
                nErrors+=1
   ##########################################################
            try:
                tweetTopics=tweet['interaction']['tag_tree']['topic']
                for topic in tweetTopics:
                    tweetTopicCounter[topic][(content,id)]+=1
            except:
                tweetTopicCounter[u'None'][(content,id)]+=1
                nTopicErrors+=1
                # Count tweets by topic
            try:                
                try:
                    nFollowers=tweet['twitter']['user']['followers_count']
                except:
                    nFollowers=tweet['twitter']['retweet']['user']['followers_count']
                
                if nFollowers>currentTopFollower and not isRetweet:
                    bisect.insort(topFollowers,(nFollowers,id))
                    currentTopFollower=topFollowers[0][0]
                # Insert tweet to maintain order
                # if new number of followers is larger than lowest value
                    if len(topFollowers)>10:topFollowers=topFollowers[-10:]
                # Allow lower values to drop out
                
                for topic in tweetTopics:
                    if nFollowers>currentTopTopicFollowers[topic] and not isRetweet:
                        currentTopTopicFollowers[topic]=topTopicFollowers[topic][0][0]                            
                        bisect.insort(topTopicFollowers[topic],(nFollowers,id))
                        if len(topTopicFollowers[topic])>10:topTopicFollowers[topic]=topTopicFollowers[topic][-10:]
            except:
                nFollowerError+=1
                print traceback.print_exc()
                time.sleep(1000)
            # Get tweets with top followers
    print nErrors,nTopicErrors,nFollowerError
    print topTopicFollowers['General']
    for k,v in tweetTopicCounter.items():
        print k,len(v)

    writeTopTweets(tweetCounter,tweetTopicCounter,topTopicFollowers,topFollowers)
##############
def main():
##############

    languageDirectories=glob.glob(dataDirectory+'*')
    
    for l in languageDirectories:
    
        dateFileNames=glob.glob(l+dateFileFormat)

    filesInRange=[f for f in dateFileNames if inLastNDays(f,l,14)]

    countTweets(filesInRange)

    print len(dateFileNames)

if __name__=='__main__':
    main()
