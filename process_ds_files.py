'''
Script to parse streaming DataSift files in JSON format and
extract to daily files.
Also counts attributes such as mentions, most 
popular tweets etc and writes to pickle file
Tweets can be filtered by geolocated country
Alex Rutherford
alex@unglobalpulse.org
'''
import sys,time,csv
sys.path.append('/mnt/home/ubuntu/projects/tools/')
import gender,geolocator
from utils import *
import glob,os.path,re,json,collections,operator
from sets import Set
import cPickle as pickle
import datetime,dateutil.parser
import traceback
import pandas as pd
from nltk import bigrams,trigrams

geo=geolocator.Geolocator()
geo.init()

genderClassifier=gender.Gender()

dataDirectory='../data/'
dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9].json'

nDuplicates=0
# Make this global

r=True
r=False
# Flag to remove DS files once parsed

v=True
#v=False
# Verbose flag

chosenCountry=None

if '-c' in sys.argv:
    # Flag for filtering by country
    i=(sys.argv).index('-c')
    chosenCountry=sys.argv[i+1]
    dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9]_'+chosenCountry+'.json'
    print 'ADDED COUNTRY FLAG',chosenCountry
    time.sleep(1)
    # If a flag used to filter by country
    # need to change the format of daily files
    
counterFileName='/counters.dat'
cartoFileName='/carto.txt'

if chosenCountry:
    counterFileName='/counters_'+chosenCountry+'.dat'
    cartoFileName='/carto_'+chosenCountry+'.txt'
# Have a separate counter file for each country
#############
def dateFileName(timeStamp,l):
#############
    '''Takes datetime object, language directory stem and chosen country for filtering 
    and returns corresponding filename
    of form YYYY_MM_DD<_CC>.json'''
    if not chosenCountry:return l+'/'+str(timeStamp.year)+'_'+str(timeStamp.month).zfill(2)+'_'+str(timeStamp.day).zfill(2)+'.json'
    else:return l+'/'+str(timeStamp.year)+'_'+str(timeStamp.month).zfill(2)+'_'+str(timeStamp.day).zfill(2)+'_'+chosenCountry+'.json'

#############
def fileStream(l):
#############
    '''
    Generator returning files in subdirectories of root directory l
    '''
    for root,dirs,files in os.walk(l):
    # Recursively looks through language directories 
    # in root data directory to find all DatSift files
        if len(files)>0:
        # Find all files
#                print root,dirs,files
            for f in files:
                if re.match(r'DataSift',f):
                # Check that they are actual datasift files
#                        print '\t',root+f
                    yield root+'/'+f

#############
def processFile(l,f,dateFileHash,counterDict,idSet,cartoFile,deletionsFile):
#############
    '''Takes directory corresponding to language directory, file to process
    and list of existing daily files. Adds contents of file to counters
    Creates daily files that are not yet created and returns.'''

    nTimeError=0
    nHashTagError=0
    nMentionError=0
    nDomainError=0
    nLocationError=0
    nGenderError=0
    nTopicError=0
    nTotal=0
    nFileDuplicate=0
    nDeleted=0
    nGeoError=0
    nUserError=0

    times=[]
    positives=[]
    negatives=[]

    content=[]
    topicTimes=[]
    topics=[]
    # We need a different time counter as there can be multiple topics
    fileHandle=open(f,'r')
    fileString=fileHandle.read().decode('utf-8')
    # Read file as one long string and convert to unicode
    for tweet in fileString.split('\n'):

        tweet=json.loads(tweet)
        if not 'deleted' in tweet.keys() and not 'facebook' in tweet.keys():
        # Catch deletions and facebook content

            tweet['ungp']={}
            # For adding our own augmentations

            if not tweet['interaction']['id'] in idSet:
                nTotal+=1
                idSet.add(tweet['interaction']['id'])

                try:
                    id=tweet['twitter']['id']
                except:
                    try:
                        id=tweet['twitter']['retweeted']['id']
                    except:
                        pass
                ###############################################   
                '''Geolocation'''
                inCountry=False
                if not chosenCountry:inCountry=True
                try:
                    loc=geo.geoLocate(tweet['twitter']['user']['location'])
                    if len(loc)>0:
#                    print tweet['twitter']['user']['location'],loc
                        if loc[0][3]==chosenCountry:inCountry=True
                        # We only want to count tweets in our chosen country if there is one

                        tweet['ungp']['geolocation']=loc[0][3]
                except:
                    nLocationError+=1
                ###############################################   
                '''Topics by country'''
                try:
                    tweetTopics=tweet['interaction']['tag_tree']['topic']
                    for t in tweetTopics:
                        if not t in counterDict['topicCountry'].keys():
                            counterDict['topicCountry'][t]=collections.defaultdict(int)
                        counterDict['topicCountry'][t][loc[0][3]]+=1
                except:
#                    print traceback.print_exc()
                    pass
                ###############################################   
                try:
                    tweetTime=dateutil.parser.parse(tweet['interaction']['created_at'])
                    if inCountry:times.append(tweetTime)
                except:
                    nTimeError+=1
                    '''print traceback.print_exc()
                    print tweet.keys()
                    print tweet['interaction']
                    time.sleep(1000000)'''
                    tweetTime=None
                ###############################################   
                '''Carto'''
                try:
                    isoTime=datetime.datetime.strptime(tweet['interaction']['created_at'],'%a, %d %b %Y %H:%M:%S +0000')
                    cartoFile.writerow([tweet['twitter']['id'],str(tweet['twitter']['geo']['latitude']),str(tweet['twitter']['geo']['longitude']),isoTime])
                except:
                    nGeoError+=1
                ####################################   
                '''Hashtags'''
                try:
                    if inCountry:
                        for h in tweet['interaction']['hashtags']:
                            counterDict['hashtags'][h.lower()]+=1   
                except:
                    nHashTagError+=1
                ###############################################   
                '''Mentions'''
                try:
                    if inCountry:
                        for m in tweet['twitter']['mentions']:
                            counterDict['mentions'][m.lower()]+=1 
                except:
                    nMentionError+=1
                ###############################################   
                '''Domains'''
                try:
                    if inCountry:
                        for d in tweet['links']['domain']:
                            counterDict['domains'][d]+=1 
                except:
                    nDomainError+=1
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
                else:
                    nUserError+=1
                
                ###############################################   
                '''Gender'''
                try:
                    g=genderClassifier.gender(tweet['twitter']['user']['name'])
                    g=g.values()[0]['gender']
                    tweet['ungp']['gender']=g
                except:
                    nGenderError+=1
                ###############################################   
                '''Sentiment'''
                if tweetTime and inCountry:
                # Only add sentiment if time successfully extracted
                # else cannot make dataframe 
                    tweetContent=tweet['interaction']['content'].encode('utf-8')
                    if re.search(posRe,tweetContent):
                        positives.append(1)
                    else:
                        positives.append(0)
                    if re.search(negRe,tweetContent):
                        negatives.append(1)
                    else:
                        negatives.append(0)
                ####################################   
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
                    nTopicError+=1
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

                ###############################################   
                '''Write tweet to file'''
                if not tweetTime==None:
                # If time is missing, cannot write to daily file
                    tweetFileName=dateFileName(tweetTime,l)
                    # This is the file where tweet should be put
                                
                    if inCountry and not tweetFileName in dateFileHash.keys():
                        dateFileHash[tweetFileName]=open(tweetFileName,'w')
#                print '!!!!!!ADDING',tweetFileName,'TO HASH'
                    # Add to hash
                    if inCountry:
                        dateFileHash[tweetFileName].write(json.dumps(tweet).encode('utf-8')+'\n')
                    #Write tweet to daily file
            else:
                nFileDuplicate+=1
        elif 'facebook' in tweet.keys():
            pass
        else:
#            print tweet
#            time.sleep(1000)
            try:
                deletionsFile.write(str(tweet['twitter']['id'])+'\n')
            except:
                deletionsFile.write(str(tweet['twitter']['retweeted']['id'])+'\n')
            nDeleted+=1
    # This tweet has been deleted
    # TODO add call to delete this tweet from historical files
    # as per http://dev.datasift.com/docs/resources/twitter-deletes

    fileString=None
    fileHandle.close()

    '''Now aggregate'''  
    '''topics'''
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
    '''sentiments'''
    tempDf=pd.DataFrame(data={'time':times,'pos':positives,'neg':negatives},index=times)
    # Make a dataframe with contents of this file
    # TODO count over something better

    if not type(counterDict['time'])==pd.Series:
        counterDict['time']=tempDf.resample('D',how='count')['time']
        counterDict['pos']=tempDf.resample('D',how='sum')['pos']
        counterDict['neg']=tempDf.resample('D',how='sum')['neg']
        # First time through, add downsampled series
    else:
        counterDict['time']=counterDict['time'].add(tempDf.resample('D',how='count')['time'],fill_value=0)
        counterDict['pos']=counterDict['pos'].add(tempDf.resample('D',how='sum')['pos'],fill_value=0)
        counterDict['neg']=counterDict['neg'].add(tempDf.resample('D',how='sum')['neg'],fill_value=0)

    if v:
        print 'TIME\tHASHTAG\tMENTION\tDOMAIN\tLOC\tGENDER\tUSER\tDEL\tDUP\tTOTAL'
        print nTimeError,'\t',nHashTagError,'\t',nMentionError,'\t',nDomainError,'\t',nLocationError,'\t',nGenderError,'\t',nUserError,'\t',nDeleted,'\t',nFileDuplicate,'\t',nTotal
    if r:os.remove(f)
    # Delete file when not needed any more
    return counterDict,dateFileHash,idSet,nFileDuplicate,nTotal,nDeleted,nUserError

#############
def writeCounters(l,counterDict):
#############
    '''Overwrites old pickle file, having read in any previous counter pickle file'''
    if v:print '\tWRITING PICKLE FILES'

    try:
        os.remove(l+counterFileName)
    except:
        if v:print 'NO OLD PICKLE FILE TO REMOVE'
    outFile=open(l+counterFileName,'w')
   
    pickle.dump(counterDict,outFile)
    
    outFile.close()
#############
def initCarto(cartoFileName,l):
#############
    if os.path.isfile(l+cartoFileName):return csv.writer(open(l+cartoFileName,'a'),delimiter='\t')
    else:return csv.writer(open(l+cartoFileName,'w'),delimiter='\t')

#############
def initDeletionsFile(l):
#############

    if not chosenCountry:
        f=open(l+'/deletions.csv','w')
    else:
        f=open(l+'/deletions_'+chosenCountry+'.csv','w')
    # Open a fresh file for now

    return f

#############
def initCounters(l):
#############
    '''Make a dictionary of counters over useful things'''
    counterDict={}
    
    if os.path.isfile(l+counterFileName):
        print '\tFOUND PICKLE FILE',l+counterFileName
        inFile=open(l+counterFileName,'r')
        
        data=pickle.load(inFile)
        
        topicCountryCounter=data['topicCountry']        
        userCounter=data['users']
        mentionCounter=data['mentions']
        unigramCounter=data['unigrams']
        bigramCounter=data['bigrams']
        trigramCounter=data['trigrams']
        hashTagCounter=data['hashtags']
        domainCounter=data['domains']
        rawDomainCounter=data['rawdomains']
        timeSeries=data['time']
        posSeries=data['pos']
        negSeries=data['neg']
        idSet=data['ids']
        dsFileSet=data['ds']
        topicCounter=data['topics']

        inFile.close()
    else:
        hashTagCounter=collections.defaultdict(int)
        domainCounter=collections.defaultdict(int)
        rawDomainCounter=collections.defaultdict(int)
        mentionCounter=collections.defaultdict(int)
        unigramCounter=collections.defaultdict(int)
        bigramCounter=collections.defaultdict(int)
        trigramCounter=collections.defaultdict(int)
        userCounter=collections.defaultdict(int)
        topicCounter={}
        topicCountryCounter={}
        timeSeries=None
        posSeries=None
        negSeries=None
        idSet=Set()
        dsFileSet=Set()

    counterDict['hashtags']=hashTagCounter
    counterDict['domains']=domainCounter
    counterDict['rawdomains']=rawDomainCounter
    counterDict['mentions']=mentionCounter
    counterDict['time']=timeSeries
    counterDict['pos']=posSeries
    counterDict['neg']=negSeries
    counterDict['ids']=idSet
    counterDict['ds']=dsFileSet
    counterDict['topics']=topicCounter
    counterDict['unigrams']=unigramCounter
    counterDict['bigrams']=bigramCounter
    counterDict['trigrams']=trigramCounter
    counterDict['users']=userCounter
    counterDict['topicCountry']=topicCountryCounter
    return counterDict,idSet,dsFileSet
#############
def main():
#############

#    syncFromS3()
    # Add call here to grab data from S3
    # Might be redundant with streaming data

    languageDirectories=glob.glob(dataDirectory+'*')
    # Top level directories in dataDirectory refer to language buckets
    # Count these separately. Directory structure within these can be arbitrary

#    idDateRanges={}
    # This holds the highest range

    for l in languageDirectories:
        print 'LANGUAGE',l
        nDuplicates=0
        # Reset this for each language
      
        nSkippedFiles=0
        # Counts DataSift fiels that we already processed
       
        nTotal=0
        # Total number of tweets processed, excluding duplicates and skipped files

        nDeletes=0
        # Count number of tweets in stream that are deleted messages

        idSet=Set()
        # This is a set that hlds all tweet ids, to ensure duplicates are removed
        # New set for each language (assumes langages will have separate DS streams)
       
        dsFileSet=Set()
        # This set holds all the DataSift file names
        # Thus duplicate files are skipped over
        
        counterDict,idSet,dsFileSet=initCounters(l)
        # Look for dumpfile to load in counters, if not there create empty ones
   
        deletionsFile=initDeletionsFile(l)
    
        cartoFile=initCarto(cartoFileName,l)
     
        dateFileNames=glob.glob(l+dateFileFormat)
        # These are the daily files which already exist in language directory
    
        dateFileHash={}
        for d in dateFileNames:
            dateFileHash[d]=open(d,'a')
            print d
        # Create a dictionary, key is date file name
        # value is file opened for appending

        for f in fileStream(l):
            if not f in dsFileSet:
                if v:print '\tPROCESSING',f

                counterDict,dateFileHash,idSet,nFileDuplicates,nFile,nFileDeletes,nUserError=processFile(l,f,dateFileHash,counterDict,idSet,cartoFile,deletionsFile) 
                # Read file and process line by line
                # Update hash of daily files in case new ones are created
                nDuplicates+=nFileDuplicates
                nTotal+=nFile
                nDeletes+=nFileDeletes
                dsFileSet.add(f)
            else:
                if v:print '\tSKIPPING DUPLICATE',f 
                nSkippedFiles+=1
        counterDict['ids']=idSet
        counterDict['ds']=dsFileSet

        writeCounters(l,counterDict)
        # Persist counters to pickle file

        deletionsFile.close()

        if v:
            print 'AT END', 
            printTop(counterDict['hashtags'],10)
            print '----------------------------------------------'
        if True:
#            print counterDict['time']
            print 'SKIPPED',nSkippedFiles
            print 'DUPLICATES',nDuplicates
            print 'DELETED',nDeletes
            print 'TOTAL NEW',nTotal
#############################
if __name__=="__main__":
    main()
