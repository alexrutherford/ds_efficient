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
import argparse
parser=argparse.ArgumentParser()

parser.add_argument("input",help='specify input directory') # Compulsory
parser.add_argument("--clean",help='flag to delete existing daily files and counter objects in input directory',action='store_true')
parser.add_argument("-C","--country",help='specify 2 letter uppercase ISO codes of country',nargs='+')
parser.add_argument("-L","--language",help='specify 2 letter lowercase codes of languages',nargs='+')
parser.add_argument("-H","--hack",help='specify topic hack',action="store_true")
args=parser.parse_args()

topicHack=args.hack

dataDirectory=args.input

chosenCountry=args.country

chosenLanguages=args.language

chosenLanguagesCountries=[]
if chosenLanguages:
    chosenLanguagesCountries.extend([c.lower() for c in chosenLanguages])
if chosenCountry:
    chosenLanguagesCountries.extend([c.upper() for c in chosenCountry])
if len(chosenLanguagesCountries)==0:chosenLanguagesCountries=None

print chosenLanguagesCountries


genderClassifier=gender.Gender()

dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9].json'
if chosenLanguagesCountries:dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9]_'+'_'.join(chosenLanguagesCountries)+'.json'

print 'FORMAT',dateFileFormat
time.sleep(3)

chosenCountry=None

if chosenLanguagesCountries:
    counterFileName='/counters_'+'_'.join(chosenLanguagesCountries)+'.dat'
    deletionsFile=csv.reader(open(dataDirectory+'/'+'deletions_'+'_'.join(chosenLanguagesCountries)+'.csv','r'),delimiter='\t')
else:
    counterFileName='/counters.dat'
    deletionsFile=csv.reader(open(dataDirectory+'/'+'deletions.csv','r'),delimiter='\t')
############
def overWriteDailyFiles():
############
    '''
    Replaces daily files including deleted content
    with daily file _excluding_ deleted content
    e.g. replaces 2014_01_01.json by 2014_01_01_tmp.json
    TODO implement overwriting
    '''
    print 'MV *tmp.json to *.json'
    print len(glob.glob(dataDirectory+'*tmp.json'))
############
def updateSets(old,new):
############
    return old['ds']-new['ds']
############
def updateData(old,new):
############
    for s in ['tw']:
# For now only have to consider TW deletions
        print 'UPDATING',s
        for k in ['hashtags','mentions','users','domains','unigrams','languages','links']:#,'bigrams','trigrams']:
        # Leave out unigrams/bigrams/trigrams for now
        # Counters
            print '\tUPDATING',k,len(new[s][k].keys())
            for kk in new[s][k].keys():
                try:
                    old[s][k][kk]-=new[s][k][kk]
                except:
                    print 'ERROR',k,kk,new[s][k][kk]
                # This shouldn't ever happen
        for k in ['time','pos','neg']:
        # Update series
            print '\tUPDATING',k
            old[s][k]=old[s][k].subtract(new[s][k],fill_value=0)

        for k in old[s]['topics'].keys():
            print '\tUPDATING TOPIC',k
            if k in new[s]['topics'].keys():
                old[s]['topics'][k]=old[s]['topics'][k].subtract(new[s]['topics'][k],fill_value=0)

        for t in new[s]['topicCountry'].keys():
            for c in new[s]['topicCountry'][t].keys():
                old[s]['topicCountry'][t][c]-=new[s]['topicCountry'][t][c]
        # Topics by country
    old['ds']-=new['ds']
    # Id set

    return old
############
def overWriteData(newData,l):
############
    '''
    Update serialised file
    TODO overwrite
    '''
    dataFile=open(l+'/counters.dat','w')
    if chosenLanguagesCountries:     
        dataFile=open(l+'/counters_'+'_'.join(chosenLanguagesCountries)+'.dat','w')
    pickle.dump(newData,dataFile)
############
def getOldData(l):
############
    '''
    Read in old serialsied file and return dictionary
    '''
    print 'READING OLD DATA'
    dataFile=open(l+'/counters.dat','r')
    if chosenLanguagesCountries:     
        dataFile=open(l+'/counters_'+'_'.join(chosenLanguagesCountries)+'.dat','r')
    d=pickle.load(dataFile)
    dataFile.close()
    return d
############
def countDuplicate(tweet,times,positives,negatives,topicTimes,topics,counterDict,content):
############
    '''
    Take tweet, count mentions, hashtag etc and add to temporary list
    '''

    s='tw'
    # Hardcode this for now, but deletions should always be TW

    tweetContent=tweet['interaction']['content'].encode('utf-8')
    tweetContent=re.sub(cleanRe,' ',tweetContent)
    
    chosenTopic=None
    ###########
    '''choose a topic here '''
    try:
        if not topicHack:
            messageTopics=tweet['interaction']['tag_tree']['topic']
        else:
            messageTopics=tweet['interaction']['tag_tree']['topic'].items()
            messageTopics=[m[0] for m in messageTopics]
        # We need this for Brazil; topics and sub-topics

        rawTopics=[m[0]+'_'+m[1][0] for m in messageTopics] # We need these for dc.js file

        messageTopics=[m.lower() for m in messageTopics]
        print messageTopics
        
        if len(messageTopics)==1:
#                chosenTopic=messageTopics[0][0]+'_'+messageTopics[0][1][0]
            chosenTopic=messageTopics[0]
            rawTopic=rawTopics[0]
        else:
            chosenTopic=random.choice(messageTopics)
            rawTopic=rawTopics[messageTopics.index(chosenTopic)]
            '''TODO How to make random selection of a topic reversible?'''
#                chosenTopic=chosenTopic[0]+'_'+chosenTopic[1][0]
        if topicHack:
            chosenTopic=chosenTopic.partition(' ')[0]

        if rawTopic=='campaign_english':
            print 'WOAH',rawTopics,messageTopics
            time.sleep(1000)
    except:
        print traceback.print_exc()
        pass
    ##############
    '''Language'''
    try:
        lang3=langid.classify(tweetContent)[0]
        if str(lang3) in chosenLanguages:
            languageMatch=True

        counterDict['tw']['languages'][lang3]+=1
    except:pass
    ###############################################   
    '''Topics by country'''
    for t in messageTopics:
        if not t in counterDict['tw']['topicCountry'].keys():
            counterDict['tw']['topicCountry'][t]=collections.defaultdict(int)
        try:
            counterDict['tw']['topicCountry'][t][tweet['ungp']['geolocation']]+=1
        except:
            pass
        # Tweet not geolocated 
    ###############################################   
    '''Hashtags'''
    try:
        if inCountry:
            for h in tweet['interaction']['hashtags']:
                counterDict[s]['hashtags'][h.lower()]+=1   
    except:
        pass
    ###############################################   
    '''Mentions'''
    try:
        if inCountry:
            for m in tweet['twitter']['mentions']:
                counterDict[s]['mentions'][m.lower()]+=1 
    except:
        pass
    ###############################################   
    '''Links'''
    try:
        for m in tweet['links']['normalized_url']:
            counterDict['tw']['links'][m.lower()]+=1 
    except:
        pass
    ###############################################   
    '''Domains'''
    try:
        for d in tweet['links']['domains']:
            counterDict['tw']['domains'][d.lower()]+=1 
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
        counterDict[s]['users'][twitterUser]+=1 
    ###############################################   
    '''Domains'''
    try:
        if inCountry:
            for d in tweet['links']['domain']:
                counterDict[s]['domains'][d]+=1 
    except:
        pass
    ###############################################   
    '''Topics'''
    try:
        for t in messageTopics:
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
        for t in messageTopics:
            if not t in counterDict['topicCountry'].keys():
                counterDict[s]['topicCountry'][t]=collections.defaultdict(int)
            counterDict[s]['topicCountry'][t][loc[0][3]]+=1
    except:
        pass
    ####################################   
    '''N-grams'''
#    if tweetTime and inCountry:
    if True:
        toks=tweetContent.lower().split(' ')
        for w in [t for t in toks if not t in stopWords]:
            counterDict[s]['unigrams'][w]+=1
        '''
        for b in bigrams(toks):
            counterDict[s]['bigrams'][b]+=1
        for t in trigrams(toks):
            counterDict[s]['trigrams'][t]+=1
        '''
    ####################################   
    '''Sentiment'''
#    if tweetTime and inCountry:
    if True:
    # Only add sentiment if time successfully extracted
    # else cannot make dataframe 
        if re.search(posRe,tweetContent):
            positives.append(1)
        else:
            positives.append(0)
        if re.search(negRe,tweetContent):
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

    deletions=[l[0] for l in deletionsFile]

    print len(deletions),'TO BE FOUND'

    dailyFiles=glob.glob(dataDirectory+dateFileFormat)
    dailyFiles.sort()
    dailyFiles.reverse()
    # Start with most recent file first
    # As deletions most likely to be recent

    '''Make fresh counter'''
    counterDict={'tw':{},'fb':{}}
    counterDict['tw']['topic_coloc']=collections.defaultdict(int)
    counterDict['tw']['languages']=collections.defaultdict(int)
    counterDict['tw']['topic_sums']=collections.defaultdict(int)
    counterDict['tw']['links']=collections.defaultdict(int)
    counterDict['tw']['country']=collections.defaultdict(int)
    counterDict['tw']['hashtags']=collections.defaultdict(int)
    counterDict['tw']['users']=collections.defaultdict(int)
    counterDict['tw']['domains']=collections.defaultdict(int)
    counterDict['tw']['rawdomains']=collections.defaultdict(int)
    counterDict['tw']['mentions']=collections.defaultdict(int)
    counterDict['tw']['unigrams']=collections.defaultdict(int)
    counterDict['tw']['bigrams']=collections.defaultdict(int)
    counterDict['tw']['trigrams']=collections.defaultdict(int)
    counterDict['ds']=Set()
    counterDict['tw']['topics']={}
    counterDict['tw']['topicCountry']={}

    times=[]
    positives=[]
    negatives=[]

    topicTimes=[]
    topics=[]
    content=[]
    nGrams=collections.defaultdict(int)

    breakFlag=False

    for f in dailyFiles:
        '''
        Loop through daily files starting with most recent
        keep looking for deleted tweets until all found
        TODO sometimes not all deletions are found, why?
        '''
        tempDailyFile=f.replace('.json','_tmp.json')
        print f,tempDailyFile

        fileHandle=open(f,'r')
        tempDailyFileHandle=open(tempDailyFile,'w')
        fileString=fileHandle.read().decode('utf-8')
        # Read file as one long string and convert to unicode
        
        nLine=0
               
        s='tw'
        # Hardcode for now, probably deletions will always only be Twitter
                
        for tweet in fileString.split('\n')[0:-1]:
            try:
                tweet=json.loads(tweet)
            except:
#                print tweet
                print traceback.print_exc()
                tweet=None
                time.sleep(10)

            isFb=False
            if tweet:
                if u'facebook' in tweet.keys():
                    isFb=True

                if tweet['interaction']['type']==u'twitter':nTwitter+=1
                if isFb:nFb+=1
            
            tweetId=None

            try:
                tweetId=tweet['interaction']['id']
            except:
                pass
            try:
                if tweetId in deletions:
                   
                    deletions.remove(tweetId)
                  
                    counterDict['ds'].add(tweetId)
                    
                    times,positives,negatives,topicTimes,topics,counterDict,content=countDuplicate(tweet,times,positives,negatives,topicTimes,topics,counterDict,content)
                    
                    while tweetId in deletions:
                        print 'DUPLICATE!!!',tweetId
                        deletions.remove(tweetId)
                        time.sleep(1)
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
    tempTopicDf=pd.DataFrame(data={'topics':topics,'content':content},index=topicTimes)
    # Make a dataframe with contents of this file
    if len(topics)>0:
    # Only aggregate topics if some tweets were tagged with topics
        topicGroups=tempTopicDf.groupby(topics)
        # Group dataframe by topics

        for topic,topicDf in topicGroups:
            if not topic in counterDict[s]['topics'].keys():
                counterDict[s]['topics'][topic]=topicDf.resample('D',how='count')['content']
                # First time through, add downsampled series
            else:
                counterDict[s]['topics'][topic]=counterDict[s]['topics'][topic].add(topicDf.resample('D',how='count')['content'],fill_value=0)
                # Then add series from each new file to running total
                # If time ranges don't overlap explicitly add a zero
                totalDf=pd.concat([tdf for t,tdf in topicGroups])
    ''' Total'''
    tempDf=pd.DataFrame(data={'time':times,'pos':positives,'neg':negatives},index=times)
    counterDict[s]['time']=tempDf.resample('D',how='count')['time']
    counterDict[s]['pos']=tempDf.resample('D',how='sum')['pos']
    counterDict[s]['neg']=tempDf.resample('D',how='sum')['neg']

    oldData=getOldData(dataDirectory)
    newData=updateData(oldData,counterDict)
    overWriteData(newData,dataDirectory)
    overWriteDailyFiles()
if __name__=='__main__':
    main()
