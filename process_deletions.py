'''
Script to read in file of deleted tweets (deletions*.txt)
From this hastags, mentions etc are extracted and a set of series
constructed to be *subtracted* from existing totals in counter*.dat
Also removes deleted tweets from daily files of form 2014_[0-9]_[0-9].json
'''
import sys
sys.path.append('/mnt/home/ubuntu/projects/tools/')
import gender,geolocator
from nltk import bigrams,trigrams
import traceback
from sets import Set
from utils import *
import cPickle as pickle
import csv,glob,sys,json,time,re
import pandas as pd
import collections,dateutil

geo=geolocator.Geolocator()
geo.init()

genderClassifier=gender.Gender()

dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9].json'

chosenCountry=None
######################
if '-c' in sys.argv:
    # Flag for filtering by country
    i=(sys.argv).index('-c')
    chosenCountry=sys.argv[i+1]
    dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9]_'+chosenCountry+'.json'
    print 'ADDED COUNTRY FLAG',chosenCountry
    time.sleep(1)
    # If a flag used to filter by country
    # need to change the format of daily files

languageDirectory='../data_test/english/'

deletionsFile=csv.reader(open(languageDirectory+'/'+'deletions.csv','r'),delimiter='\t')

############
def overWriteDailyFiles():
############
    print 'MV *tmp.json to *.json'
    print len(glob.glob(languageDirectory+'*tmp.json'))
############
def updateSets(old,new):
############
    return old['ids']-new['ids']
############
def updateData(old,new):
############
    for k in ['hashtags','mentions','users','domains']:
    # Leave out unigrams/bigrams/trigrams for now
    # Counters
        print 'UPDATING',k
        for kk in new[k].keys():
            if kk in old[k].keys():
#                print kk,old[k][kk],
                old[k][kk]-=new[k][kk]
#                print old[k][kk]
            else:
                print 'ERROR',k,kk,new[k][kk]
            # This shouldn't ever happen
    old['ids']-=new['ids']
    # Id set
    for k in ['time','pos','neg']:
        print k
        old[k]=old[k].subtract(new[k],fill_value=0)
    #series

    for t in new['topicCountry'].keys():
        for c in new['topicCountry'][t].keys():
            old['topicCountry'][t][c]-=new['topicCountry'][t][c]
    # Topics by country

    return old
############
def overWriteData(newData,l):
############
    dataFile=open(l+'/counters_updated.dat','w')
    if chosenCountry:     
        dataFile=open(l+'/counters_'+chosenCountry+'.dat','w')
    pickle.dump(newData,dataFile)
############
def getOldData(l):
############
    print 'READING OLD DATA'
    dataFile=open(l+'/counters.dat','r')
    if chosenCountry:     
        dataFile=open(l+'/counters_'+chosenCountry+'.dat','r')
    d=pickle.load(dataFile)
    dataFile.close()
    return d
############
def countDuplicate(tweet,times,positives,negatives,topicTimes,topics,counterDict,content):
############
    '''
    Take tweet, count mentions, hashtag etc and add to temporoary list
    '''
    ###############################################   
    '''Geolocation'''
    inCountry=False
    if not chosenCountry:inCountry=True
#    print inCountry
    
    tweetContent=tweet['interaction']['content'].encode('utf-8')
#    print 'CONTENT=>',tweetContent
         
    try:
        loc=geo.geoLocate(tweet['twitter']['user']['location'])
        if len(loc)>0:
#                    print tweet['twitter']['user']['location'],loc
            if loc[0][3]==chosenCountry:inCountry=True
            # We only want to count tweets in our chosen country if there is one

            tweet['ungp']['geolocation']=loc[0][3]
    except:
        pass
    ###############################################   
    try:
        tweetContent=tweet['interaction']['content'].encode('utf-8')
    except:
        pass
    ###############################################   
    try:
        tweetTime=dateutil.parser.parse(tweet['interaction']['created_at'])
        if inCountry:times.append(tweetTime)
    except:
        pass
        tweetTime=None
    ###############################################   
    '''Hashtags'''
    try:
        if inCountry:
            for h in tweet['interaction']['hashtags']:
                counterDict['hashtags'][h.lower()]+=1   
    except:
        pass
    ###############################################   
    '''Mentions'''
    try:
        if inCountry:
            for m in tweet['twitter']['mentions']:
                counterDict['mentions'][m.lower()]+=1 
    except:
        pass
    ###############################################   
    '''Users'''
    twitterUser=None
    try:
        twitterUser=tweet['twitter']['retweeted']['user']['screen_name']
    except:
        try:
            twitterUser=tweet['twitter']['user']['screen_name']
        except:
            pass
    if twitterUser: 
        if inCountry:
            counterDict['users'][twitterUser]+=1 
    ###############################################   
    '''Domains'''
    try:
        if inCountry:
            for d in tweet['links']['domain']:
                counterDict['domains'][d]+=1 
    except:
        pass
    ###############################################   
    '''Topics'''
    try:
        tweetTopics=tweet['interaction']['tag_tree']['topic']
        for t in tweetTopics:
            content.append(tweetContent)
            topicTimes.append(tweetTime)
            topics.append(t)
            # Need this to count over topics
            # TODO Can this be improved
    except:
        pass
    ####################################   
    '''Topics by country'''
    try:
        tweetTopics=tweet['interaction']['tag_tree']['topic']
        for t in tweetTopics:
            if not t in counterDict['topicCountry'].keys():
                counterDict['topicCountry'][t]=collections.defaultdict(int)
                counterDict['topicCountry'][t][loc[0][3]]+=1
    except:
        pass
    ####################################   
    '''N-grams'''
    if tweetTime and inCountry:
        toks=tweetContent.lower().split(' ')
        for w in [t for t in toks if not t in stopWords]:
            counterDict['unigrams'][w]+=1
        for b in bigrams(toks):
            counterDict['bigrams'][b]+=1
        for t in trigrams(toks):
            counterDict['trigrams'][t]+=1
    ####################################   
    '''Sentiment'''
    if tweetTime and inCountry:
    # Only add sentiment if time successfully extracted
    # else cannot make dataframe 
        if re.search(r':-\)|:\)',tweetContent):
            positives.append(1)
        else:
            positives.append(0)
        if re.search(r':-\(|:\(',tweetContent):
            negatives.append(1)
        else:
            negatives.append(0)

    return times,positives,negatives,topicTimes,topics,counterDict,content

############
def main():
############
    nError=0

    nFb=0
    nTwitter=0

    deletions=[int(l[0]) for l in deletionsFile]

    print len(deletions),'TO BE FOUND'

    dailyFiles=glob.glob(languageDirectory+dateFileFormat)
    dailyFiles.sort()
    dailyFiles.reverse()
# Start with most recent file first

    '''Make fresh counter'''
    counterDict={}
    counterDict['hashtags']=collections.defaultdict(int)
    counterDict['users']=collections.defaultdict(int)
    counterDict['domains']=collections.defaultdict(int)
    counterDict['rawdomains']=collections.defaultdict(int)
    counterDict['mentions']=collections.defaultdict(int)
    counterDict['unigrams']=collections.defaultdict(int)
    counterDict['bigrams']=collections.defaultdict(int)
    counterDict['trigrams']=collections.defaultdict(int)
    counterDict['ids']=Set()
    counterDict['topics']={}
    counterDict['topicCountry']={}

    times=[]
    positives=[]
    negatives=[]

    topicTimes=[]
    topics=[]
    content=[]
    nGrams=collections.defaultdict(int)

    breakFlag=False

    for f in dailyFiles:
        '''Loop through daily files starting with most recent
        keep looking for deleted tweets until all found
        '''
        tempDailyFile=f.replace('.json','_tmp.json')
        print f,tempDailyFile

        fileHandle=open(f,'r')
        tempDailyFileHandle=open(tempDailyFile,'w')
        fileString=fileHandle.read().decode('utf-8')
        # Read file as one long string and convert to unicode
        
        nLine=0
                
        for tweet in fileString.split('\n')[0:-1]:
            try:
                tweet=json.loads(tweet)
            except:
                print tweet
                print traceback.print_exc()
                time.sleep(1000)

            isFb=False
            if u'facebook' in tweet.keys():
                isFb=True

            if tweet['interaction']['type']==u'twitter':nTwitter+=1
            if isFb:nFb+=1
            
            tweetId=None

            try:
                tweetId=int(tweet['twitter']['id'])
            except:
                pass
            try:
                if tweetId in deletions:
#                    print 'FOUND IT',tweetId
                   
                    deletions.remove(tweetId)
                  
                    counterDict['ids'].add(tweetId)
                    
                    times,positives,negatives,topicTimes,topics,counterDict,content=countDuplicate(tweet,times,positives,negatives,topicTimes,topics,counterDict,content)
                    
                    if tweetId in deletions:
                        print 'DUPLICATE!!!'
                        time.sleep(10000)
#                    print 'REMOVING',tweetId,len(deletions)
                else:
                    tempDailyFileHandle.write(json.dumps(tweet)+'\n')
            except:
                print traceback.print_exc()
                nError+=1

            if len(deletions)==0:
                print 'FOUND ALL DELETIONS'
                breakFlag=True
                break
            nLine+=1
        if breakFlag:break
        print len(deletions),'LEFT TO FIND',deletions[0]
        tempDailyFileHandle.close()
    print 'LOOPED THROUGH ALL FILES'
    print len(deletions),'DELETIONS LEFT TO FIND'
    print nError
    print nFb,nTwitter
    
    '''Now aggregate'''  
    '''topics'''
#    tempTopicDf=pd.DataFrame(data={'topics':topics,'content':content},index=topicTimes)
    tempTopicDf=pd.DataFrame(data={'topics':topics,'content':content},index=topicTimes)
    # Make a dataframe with contents of this file
    if len(topics)>0:
    # Only aggregate topics if some tweets were tagged with topics
        topicGroups=tempTopicDf.groupby(topics)
        # Group dataframe by topics

        for topic,topicDf in topicGroups:
            if not topic in counterDict['topics'].keys():
                counterDict['topics'][topic]=topicDf.resample('D',how='count')['content']
                # First time through, add downsampled series
            else:
                counterDict['topics'][topic]=counterDict['topics'][topic].add(topicDf.resample('D',how='count')['content'],fill_value=0)
                # Then add series from each new file to running total
                # If time ranges don't overlap explicitly add a zero
                totalDf=pd.concat([tdf for t,tdf in topicGroups])
    ''' Total'''
    tempDf=pd.DataFrame(data={'time':times,'pos':positives,'neg':negatives},index=times)
    counterDict['time']=tempDf.resample('D',how='count')['time']
    counterDict['pos']=tempDf.resample('D',how='sum')['pos']
    counterDict['neg']=tempDf.resample('D',how='sum')['neg']

    for l in times,positives,negatives,topicTimes,topics:print len(l)

    oldData=getOldData(languageDirectory)
    newData=updateData(oldData,counterDict)
    overWriteData(newData,languageDirectory)
    overWriteDailyFiles()
if __name__=='__main__':
    main()
