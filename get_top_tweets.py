'''
Script to count over tweets from last n days to determine most retweeted and
messages from users with most number of followers. Reads in daily tweet files
as outputted from process_ds_files.py. 
'''
import glob,re,sys,os,csv,json
import datetime,collections,operator
import traceback,time,bisect,pickle
from sortedcontainers import SortedSet

dataDirectory='../data_test/'
dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9].json'
#dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9]_BR.json'


nDays=14
# Length of window for top tweets
if '-n' in sys.argv:
    i=(sys.argv).index('-n')
    nDays=int(sys.argv[i+1])
    print 'LENGTH OF WINDOW',nDays
    time.sleep(1)

if '-d' in sys.argv:
    i=(sys.argv).index('-d')
    dataDirectory=sys.argv[i+1]
    pickleFileName=dataDirectory+'counters.dat'
    print 'SET DATA DIRECTORY',dataDirectory
    time.sleep(1)
   
if '-C' in sys.argv:
    i=(sys.argv).index('-C')
    chosenCountry=sys.argv[i+1]
    dateFileFormat=dateFileFormat.partition('.json')[0]+'_'+chosenCountry+'.json'
    pickleFileName=dataDirectory+'counters_'+chosenCountry+'.dat'
    print 'SET COUNTRY',chosenCountry
    time.sleep(1)

if '-clean' in sys.argv:
    print 'TRYING TO CLEAN..'
    try:
        os.system("rm -r "+dataDirectory+'data/')
    except:
        print 'CANT CLEAN data/'
    try:
        os.system("rm -r "+dataDirectory+'plots/')
    except:
        print 'CANT CLEAN plots/'
    time.sleep(1)

try:
    os.mkdir(dataDirectory+'plots/')
except:
    print traceback.print_exc()
    print 'plots directories already exist'

try:
    os.mkdir(dataDirectory+'data/')
except:
    print traceback.print_exc()
    print 'data directories already exist'

topics=[u'None',u'General',u'Politics/Opinion',u'Economy',u'Risk/Disaster',u'Energy',u'Weather',u'Agriculture/Forestry',u'Oceans/Water',u'Arctic']
topicHash={'General':'general','Politics/Opinion':'politics','Economy':'economy','Risk/Disaster':'risk','Energy':'energy','Weather':'weather','Agriculture/Forestry':'agriculture','Oceans/Water':'oceans','Arctic':'arctic'}

topics=[u'campaign english',u'prevention neutral',u'testing neutral',u'campaign portuguese',u'prevention negative',u'prevention positive',u'discrimination positive',u'discrimination negative']
topics=[u'campaign',u'prevention',u'testing',u'discrimination']

topicHash={}
for t in topics:topicHash[t]=t

# Maps from topics as in datasift tags and output file names
# TODO construct this list automatically
##############
def writeTopTweets(tweetCounter,tweetTopicCounter,topTopicFollowers,topFollowers):
##############
    sortedTweets=sorted(tweetCounter.iteritems(), key=operator.itemgetter(1))
    sortedTweets.reverse()
    outFile=csv.writer(open(dataDirectory+'data/all.top.retweet','w'),delimiter='\t')

    for t in sortedTweets[0:10]:
        outFile.writerow([t[0]])
# Write out top tweet ids for all tweets

    outFile=csv.writer(open(dataDirectory+'data/all.top.followers','w'),delimiter='\t')
    for t in reversed(topFollowers[0:10]):
        outFile.writerow([t[1]])
# Write out top follower count tweet id's for all tweets

    for k,v in reversed(topTopicFollowers.items()[0:10]):
#        print '+++++',k,v
        if not k=='None':
            fileName=dataDirectory+'data/'+topicHash[k]+'.top.followers'
            outFile=csv.writer(open(fileName,'w'),delimiter='\t')
            for id in reversed(SortedSet(v)):
            # Using SortedSet() as a hack as there inexplicably appears a duplicate 
            # final entry in cases of low volume
                if not id[0]==-1:
                    outFile.writerow([id[1]])
#                    print '\t',id
# Write out top follow count tweet ids for tweets, by topic
            outFile=None
# Need to flush out file handle
# Sometimes last file doesn't get written otherwise

    for k,v in tweetTopicCounter.items():
        if not k=='None':
            sortedTweets=sorted(v.iteritems(),key=lambda x:x[1])
            sortedTweets.reverse()
            fileName=dataDirectory+'data/'+topicHash[k]+'.top.retweet'
            outFile=csv.writer(open(fileName,'w'),delimiter='\t')
#            print k
            for t in sortedTweets[0:10]:
#                print '\t',t[1],t[0][0],t[0][1]
                if t[1]>1:
                # Counts the original as a retweet, so throw out any that were tweeted only once
                    outFile.writerow([t[0][1]])
#                print 'line',fileName
# Write out top retweeted tweet id's by topic
    outFile=None

##############
def inLastNDays(d,l,n=7):
##############
    '''
    Tests if daily tweet file l+d corresponds to last n days
    '''
    fileTimes=d.partition(l)[2].replace('.json','').split('_')
    fileTimes=fileTimes[0:3]
    # Need this if there is a country code suffix

    fileTimes=[int(f) for f in fileTimes]
    # These are date components: 2104,7,21

    timeLimit=datetime.datetime.now()-datetime.timedelta(7)

    timeDiff=(timeLimit-(datetime.datetime(fileTimes[0],fileTimes[1],fileTimes[2]))).days

    if timeDiff<=n:
        return True
    return False

##############
def countTweets(files):
##############
    '''
    Loops through all files in last n days
    Counts retweets by tweets sharing same content
    Keeps track of tweets by users with msot followers
    '''
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
            id=None
            try:
                tweet=json.loads(line)
            except:
                print 'PARSE ERROR',f,nLine
            if 'twitter' in tweet.keys():
                try:

                    try:
                        id=tweet['twitter']['retweet']['id']
                        tweetCounter[id]+=1
                        isRetweet=True
                    except:
                        id=tweet['twitter']['id']
                        tweetCounter[id]+=1
                    content=tweet['interaction']['content'].encode('utf-8').replace('\n',' ')
                except:
                    nErrors+=1
       ##########################################################
                try:
                    tweetTopics=tweet['interaction']['tag_tree']['topic'].items()
#                    print tweetTopics
                    #print "---"
                    #AtweetTopics=[m[0]+' '+m[1][0] for m in tweetTopics]
                    #print AtweetTopics
                    tweetTopics=[m[0].lower() for m in tweetTopics] # temporary in (TL)
                    #print "==="
                    #print BtweetTopics
                    #print "..."
                    #continue
#                    print tweetTopics
                    # TODO find a general way to parse topics
                    # Currently over engineered to brazil schema
                    
                    for topic in tweetTopics:
                        tweetTopicCounter[topic.lower()][(content,id)]+=1
                except KeyError:
#                tweetTopicCounter[u'None'][(content,id)]+=1
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
                        if nFollowers>currentTopTopicFollowers[topic.lower()] and not isRetweet:
                            currentTopTopicFollowers[topic.lower()]=topTopicFollowers[topic.lower()][0][0]                            
                            bisect.insort(topTopicFollowers[topic.lower()],(nFollowers,id))
                            if len(topTopicFollowers[topic.lower()])>10:topTopicFollowers[topic.lower()]=topTopicFollowers[topic.lower()][-10:]
                except:
                    nFollowerError+=1
                    print traceback.print_exc()
            # Get tweets with top followers
#    print nErrors,nTopicErrors,nFollowerError
#    print topTopicFollowers['General']
#    for k,v in tweetTopicCounter.items():
#        print k,len(v)
#    for k,v in topTopicFollowers.items():
#        print k,type(v)

    writeTopTweets(tweetCounter,tweetTopicCounter,topTopicFollowers,topFollowers)
##############
def main():
##############

    languageDirectories=glob.glob(dataDirectory+'*')

    pickleFile=open(pickleFileName,'r')
    data=pickle.load(pickleFile)
    topics=[k for k in data['tw']['topics'].keys()]
    topicHash=dict([(k,k) for k in topics])
    pickleFile.close()

    print topics
    print topicHash
    
#    for l in languageDirectories:
#        dateFileNames=glob.glob(l+dateFileFormat)
    dateFileNames=glob.glob(dataDirectory+dateFileFormat)
    filesInRange=[f for f in dateFileNames if inLastNDays(f,dataDirectory,nDays)]

    countTweets(filesInRange)

    print len(dateFileNames)

if __name__=='__main__':
    main()
