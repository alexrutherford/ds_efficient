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
import glob,os.path,re,json,collections,operator,random
from sets import Set
import cPickle as pickle
import datetime,dateutil.parser
import traceback
import pandas as pd
from nltk import bigrams,trigrams
import langid,itertools
import argparse
parser=argparse.ArgumentParser()

geo=geolocator.Geolocator()
geo.init()

genderClassifier=gender.Gender()

nDuplicates=0
# Make this global

fileCounter=0

v=True
#v=False
# Verbose flag

r=True
r=False
# Flag to remove DS files once parsed

cities=None

#topicHack=False

parser.add_argument("input",help='specify input directory') # Compulsory
parser.add_argument("-c","--cities",help='specify file of city names for snapping')
parser.add_argument("-H","--hack",help='specify topic hack',action="store_true")
parser.add_argument("--clean",help='flag to delete existing daily files and counter objects in input directory',action='store_true')
parser.add_argument("-C","--country",help='specify 2 letter uppercase ISO codes of country',nargs='+')
parser.add_argument("-L","--language",help='specify 2 letter lowercase codes of languages',nargs='+')
args=parser.parse_args()

dataDirectory=args.input

cityFileName=args.cities
if cityFileName:cities,nCityError=getCities(cityFileName,geo)

topicHack=args.hack

chosenCountry=args.country

chosenLanguages=args.language

chosenLanguagesCountries=[]
if chosenLanguages:
    chosenLanguagesCountries.extend([c.lower() for c in chosenLanguages])
if chosenCountry:
    chosenLanguagesCountries.extend([c.upper() for c in chosenCountry])
if len(chosenLanguagesCountries)==0:chosenLanguagesCountries=None

print chosenLanguagesCountries

dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9].json'
if chosenLanguagesCountries:dateFileFormat='/[0-9][0-9][0-9][0-9]_*[0-9][0-9]_[0-9][0-9]_'+'_'.join(chosenLanguagesCountries)+'.json'

print 'FORMAT',dateFileFormat
time.sleep(3)

counterFileName='/counters.dat'
cartoFileName='/carto.txt'

if chosenLanguagesCountries:
    counterFileName='/counters_'+'_'.join(chosenLanguagesCountries)+'.dat'
    cartoFileName='/carto_'+'_'.join(chosenLanguagesCountries)+'.txt'
# Have a separate counter file for each country

if args.clean:
    removeDailyFiles(dataDirectory,dateFileFormat)
    try:
        os.remove(dataDirectory+counterFileName)    
    except:
        print 'NO OLD COUNTER FILE'
    try:
        os.remove(dataDirectory+cartoFileName)    
    except:
        print 'NO OLD CARTO FILE'

#############
def dateFileName(timeStamp,l):
#############
    '''Takes datetime object, language directory stem and chosen country for filtering 
    and returns corresponding filename
    of form YYYY_MM_DD<_CC>.json'''
    if not chosenLanguagesCountries:return l+'/'+str(timeStamp.year)+'_'+str(timeStamp.month).zfill(2)+'_'+str(timeStamp.day).zfill(2)+'.json'
    else:return l+'/'+str(timeStamp.year)+'_'+str(timeStamp.month).zfill(2)+'_'+str(timeStamp.day).zfill(2)+'_'+'_'.join(chosenLanguagesCountries)+'.json'

#############
def fileStream(l):
#############
    '''
    Generator returning files in subdirectories of root directory l
    '''
    for root,dirs,files in os.walk(l):
        print root
    # Recursively looks through language directories 
    # in root data directory to find all DatSift files
        if len(files)>0 and re.match(l+'2014-[0-9][0-9]',root):
        # Find all files
#            print root,dirs,files
            for f in files[0:20]:
                if re.match(r'DataSift',f):
                # Check that they are actual datasift files
#                        print '\t',root+f
                    yield root+'/'+f

#############
def processFile(l,f,dateFileHash,counterDict,cartoFile,deletionsFile,dcFile):
#############
    '''Takes directory corresponding to language directory, file to process
    and list of existing daily files. Adds contents of file to counters
    Creates daily files that are not yet created and returns.'''
    nLinkError=0

    nTwTimeError=0
    nFbTimeError=0
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
    nFacebook=0
    nDcError=0
    nTopicError=0
    fbContentError=0

    twTimes=[]
    twPositives=[]
    twNegatives=[]
    twContent=[]
    twTopicTimes=[]
    twTopics=[]
    # We need a different time counter as there can be multiple topics
    
    fbTimes=[]
    fbPositives=[]
    fbNegatives=[]
    fbTopicTimes=[]
    fbTopics=[]

    fileHandle=open(f,'r')
    fileString=fileHandle.read().decode('utf-8')
    # Read file as one long string and convert to unicode
    for line in fileString.split('\n'):

        message=json.loads(line)
        chosenTopic=None
        ###########
        '''choose a topic here '''
        try:
            if not topicHack:
                messageTopics=message['interaction']['tag_tree']['topic']
            else:
                messageTopics=message['interaction']['tag_tree']['topic'].items()
                messageTopics=[m[0] for m in messageTopics]
            # We need this for Brazil; topics and sub-topics

            rawTopics=[m[0]+'_'+m[1][0] for m in messageTopics] # We need these for dc.js file
   
            messageTopics=[m.lower() for m in messageTopics]

            
            if len(messageTopics)==1:
#                chosenTopic=messageTopics[0][0]+'_'+messageTopics[0][1][0]
                chosenTopic=messageTopics[0]
                rawTopic=rawTopics[0]
            else:
                chosenTopic=random.choice(messageTopics)
                rawTopic=rawTopics[messageTopics.index(chosenTopic)]
#                chosenTopic=chosenTopic[0]+'_'+chosenTopic[1][0]
            if topicHack:
                chosenTopic=chosenTopic.partition(' ')[0]

            if rawTopic=='campaign_english':
                print 'WOAH',rawTopics,messageTopics
                time.sleep(1000)
        except:
            nTopicError+=1
        ##############
        if not 'deleted' in message.keys() and not 'facebook' in message.keys():
        # Catch deletions and facebook content
            tweetContent=message['interaction']['content'].encode('utf-8')
            tweetContent=re.sub(cleanRe,' ',tweetContent)
            
            languageMatch=True
            if chosenLanguages:languageMatch=False
                    
            lang1,lang2,lang3='','','' 
            try:
                lang1=message['language']['tag']
            except:pass
            try:
                lang2=message['twitter']['lang']
            except:pass
            try:
                lang3=langid.classify(tweetContent)[0]
                if str(lang3) in chosenLanguages:
                    languageMatch=True
            except:pass
            # Count detected language from twitter and DataSift and through langid
            
            if not message['interaction']['id'] in counterDict['tw']['ids']:
                nTotal+=1
                counterDict['tw']['ids'].add(message['interaction']['id'])
                
#                counterDict['tw']['languages'][(lang1,lang2,lang3)]+=1
                counterDict['tw']['languages'][lang3]+=1

                message['ungp']={}
                # For adding our own augmentations
                try:
                    id=message['twitter']['retweeted']['id']
                except:
                    try:
                        id=message['twitter']['id']
                    except:
                        pass
                ###############################################   
                '''Geolocation'''
                inCountry=False
                if not chosenCountry:inCountry=True
                # If chosenCountry has been set, test to see if
                # tweet geolocated there
                try:
                    loc=geo.geoLocate(message['twitter']['user']['location'])
                    if len(loc)>0:
                        if loc[0][3] in chosenCountry:inCountry=True
                        # We only want to count tweets in our chosen country if there is one

                        message['ungp']['geolocation']=loc[0][3]
                except:
                    nLocationError+=1
                ###############################################   
                ''' Country counts'''
                try:
                    tweetCountry=message['twitter']['place']['country']
                    counterDict['tw']['country'][tweetCountry]+=1
                except:
                    pass

                ###############################################   
                '''Topics by country'''
                try:
                    tweetTopics=message['interaction']['tag_tree']['topic']
                    tweetTopics=messageTopics
                    if inCountry and languageMatch:
                        for t in tweetTopics:
                            if not t in counterDict['tw']['topicCountry'].keys():
                                counterDict['tw']['topicCountry'][t]=collections.defaultdict(int)
                            counterDict['tw']['topicCountry'][t][loc[0][3]]+=1
                except:
#                    print traceback.print_exc()
                    pass
                ###############################################   
                try:
                    tweetTime=dateutil.parser.parse(message['interaction']['created_at'])
                    if inCountry and languageMatch:twTimes.append(tweetTime)
                except:
                    nTwTimeError+=1
                    tweetTime=None
                ###############################################   
                '''Carto'''
                try:
                    chosenTopic=None
                    coords=(message['twitter']['geo']['latitude'],message['twitter']['geo']['longitude'])
                    isoTime=getISODate(message['interaction']['created_at'])
                    cartoFile.writerow([message['twitter']['id'],str(coords[0]),str(coords[1]),isoTime,chosenTopic])
                except:
                    nGeoError+=1
                ####################################   
                '''DC.js'''
                try:
                    coords=(message['twitter']['geo']['latitude'],message['twitter']['geo']['longitude'])
                    isoTime=getISODate(message['interaction']['created_at'])
                    closestCityCoords,closestCity=getClosestCity(cities,coords,tol=120)
                    if closestCity:
                        dcFile.writerow([closestCity.encode('utf-8'),closestCityCoords[0],closestCityCoords[1],isoTime,chosenTopic])
                except:
#                    print traceback.print_exc()
                    nDcError+1
                ####################################   
                '''Hashtags'''
                try:
                    if inCountry and languageMatch:
                        for h in message['interaction']['hashtags']:
                            counterDict['tw']['hashtags'][h.lower()]+=1   
                except:
                    nHashTagError+=1
                ###############################################   
                '''Mentions'''
                try:
                    if inCountry and languageMatch:
                        for m in message['twitter']['mentions']:
                            counterDict['tw']['mentions'][m.lower()]+=1 
                except:
                    nMentionError+=1
                ###############################################   
                '''Links'''
                try:
                    if inCountry and languageMatch:
                        for m in message['links']['normalized_url']:
                            counterDict['tw']['links'][m.lower()]+=1 
                except:
                    nLinkError+=1
                ###############################################   
                '''Domains'''
                try:
                    if inCountry and languageMatch:
                        for d in message['links']['domain']:
                            counterDict['tw']['domains'][d]+=1 
                except:
                    nDomainError+=1
                ###############################################   
                '''Users'''
                twitterUser=None
                try:
                    twitterUser=message['twitter']['retweeted']['user']['screen_name']
                except:
                    try:
                        twitterUser=message['twitter']['user']['screen_name']
                    except:
                        pass
                if twitterUser: 
                    if inCountry and languageMatch:
                        counterDict['tw']['users'][twitterUser]+=1 
                else:
                    nUserError+=1
                ###############################################   
                '''Gender'''
                try:
                    if inCountry and languageMatch:
                        g=genderClassifier.gender(message['interaction']['author']['name'])
                        g=g.values()[0]['gender']
                        message['ungp']['gender']=g
                        if chosenTopic in counterDict['tw']['genderTopic'].keys():
                            counterDict['tw']['genderTopic'][chosenTopic][g]+=1
                        else:
                            counterDict['tw']['genderTopic'][chosenTopic]=collections.defaultdict(int)
                            counterDict['tw']['genderTopic'][chosenTopic][g]+=1
                except:
                    nGenderError+=1
                ###############################################   
                '''Sentiment'''
                if tweetTime and inCountry and languageMatch:
                # Only add sentiment if time successfully extracted
                # else cannot make dataframe 
                    if re.search(posRe,tweetContent):
                        twPositives.append(1)
                    else:
                        twPositives.append(0)
                    if re.search(negRe,tweetContent):
                        twNegatives.append(1)
                    else:
                        twNegatives.append(0)
                ####################################   
                '''Topics'''
                try:
                    tweetTopics=messageTopics
                    if inCountry and languageMatch:
                        if len(tweetTopics)>1:
                            for c in itertools.combinations(tweetTopics,2):
                                counterDict['tw']['topic_coloc'][c]+=1
                        for t in tweetTopics:
                            twContent.append(tweetContent)
                            twTopicTimes.append(tweetTime)
                            twTopics.append(t)
                            # Need this to count over topics
                            # TODO Can this be improved
                except:
#                    print traceback.print_exc()
                    nTopicError+=1
                ####################################   
                '''N-grams'''
                if tweetTime and inCountry and languageMatch:
                    toks=tweetContent.lower().split(' ')
                    for w in [t for t in toks if not t in stopWords]:
                        counterDict['tw']['unigrams'][w]+=1
#                    for b in bigrams(toks):
#                        counterDict['tw']['bigrams'][b]+=1
#                    for t in trigrams(toks):
#                        counterDict['tw']['trigrams'][t]+=1
                     # TODO figure out how to make this memory efficient
                ###############################################   
                '''Write tweet to file'''
                if not tweetTime==None:
                # If time is missing, cannot write to daily file
                    tweetFileName=dateFileName(tweetTime,l)
                    # This is the file where tweet should be put
                                
                    if inCountry and not tweetFileName in dateFileHash.keys() and languageMatch:
                        dateFileHash[tweetFileName]=open(tweetFileName,'w')
                    # Add to hash
                    if inCountry and languageMatch:
                        dateFileHash[tweetFileName].write(json.dumps(message).encode('utf-8')+'\n')

                    #Write tweet to daily file
            else:
                nFileDuplicate+=1
        ###################################
        elif 'facebook' in message.keys():
        ###################################
            fbTime=None
            languageMatch=True
            if chosenLanguages:languageMatch=False
            
            try:
                fbContent=message['interaction']['content'].encode('utf-8')
                fbContent=re.sub(cleanRe,' ',fbContent)

                lang1,lang2,lang3='','','' 
                try:
                    lang1=message['language']['tag']
                except:pass
                try:
                    lang2=message['facebook']['lang']
                except:pass
                try:
                    lang3=langid.classify(fbContent)[0]

                    if str(lang3) in chosenLanguages:
                        languageMatch=True
                except:
                    pass
                # Count detected language from twitter and DataSift and through langid

            except:
                fbContentError+1
                           
            if not message['interaction']['id'] in counterDict['fb']['ids']:
                nTotal+=1
                nFacebook+=1
                
                counterDict['fb']['ids'].add(message['interaction']['id'])
                
                counterDict['fb']['languages'][(lang1,lang2,lang3)]+=1

                message['ungp']={}
                # For adding our own augmentations
                ###############################################   
                '''Geolocation'''
                inCountry=False
                if not chosenCountry:inCountry=True
                # If chosenCountry has been set, test to see if
                # TODO message geolocated there
                
                ###############################################   
                '''Topics by country'''
                try:
#                    fbTopics=message['interaction']['tag_tree']['topic']
                    facebookTopics=messageTopics
                    if inCountry and languageMatch:
                        for t in fbTopics:
                            if not t in counterDict['fb']['topicCountry'].keys():
                                counterDict['fb']['topicCountry'][t]=collections.defaultdict(int)
                            counterDict['fb']['topicCountry'][t][loc[0][3]]+=1
                except:
                    pass
                    
                ###############################################   
                try:
                    fbTime=dateutil.parser.parse(message['interaction']['created_at'])
                    if inCountry and languageMatch:fbTimes.append(fbTime)
                except:
                    nFbTimeError+=1
                    fbTime=None
#                    print 'TIME',traceback.print_exc()
                ###############################################   
                '''Carto'''
                try:
                    coords=(message['facebook']['geo']['latitude'],message['facebook']['geo']['longitude'])
                    isoTime=getISODate(message['interaction']['created_at'])
                    cartoFile.writerow([message['interaction']['id'],str(coords[0]),str(coords[1]),isoTime])
                except:
                    nGeoError+=1
                ###############################################   
                '''Links'''
                try:
                    if inCountry and languageMatch:
                        for m in message['links']['normalized_url']:
                            counterDict['fb']['links'][m.lower()]+=1 
                except:
                    nLinkError+=1
#                    print traceback.print_exc()
                ###############################################   
                '''Domains'''
                try:
                    if inCountry and languageMatch:
                        for d in message['links']['domain']:
                            counterDict['fb']['domains']+=1
                except:
                    nDomainError+=1
                ###############################################   
                '''Users'''
                fbUser=None
                try:
                    fbUser=message['facebook']['interaction']['author']['hash_id']
                except:
                    nUserError+=1
                if fbUser and inCountry and languageMatch:
                    counterDict['fb']['users'][fbUser]+=1
                ###############################################   
                '''Gender'''
                try:
                    g=genderClassifier.gender(message['facebook']['demographic']['gender'])
                    message['ungp']['gender']=g
                    if chosenTopic in counterDict['fb']['genderTopic'].keys():
                        counterDict['fb']['genderTopic']['chosenTopic'][g]+=1
                    else:
                        counterDict['fb']['genderTopic']['chosenTopic']=collections.defaultdict(int)
                        counterDict['fb']['genderTopic']['chosenTopic'][g]=1
                except:
                    nGenderError+=1
                ###############################################   
                '''Sentiment'''
                if fbTime and inCountry and languageMatch:
                # Only add sentiment if time successfully extracted
                # else cannot make dataframe 
                    if re.search(posRe,fbContent):
                        fbPositives.append(1)
                    else:
                        fbPositives.append(0)
                    if re.search(negRe,fbContent):
                        fbNegatives.append(1)
                    else:
                        fbNegatives.append(0)
                ###############################################   
                '''Topics'''
                try:
                    facebookTopics=messageTopics
                    if inCountry and languageMatch:
                        for t in facebookTopics:
#                            fbContent.append(fbContent)
                            fbTopicTimes.append(fbTime)
                            fbTopics.append(t)
                        # Need this to count over topics
                        # TODO Can this be improved
                except:
#                    print traceback.print_exc()
                    nTopicError+=1
                ###############################################   
                '''N-grams'''
                if fbTime and inCountry and languageMatch:
                    toks=fbContent.lower().split(' ')
                    for w in [t for t in toks if not t in stopWords]:
                        counterDict['fb']['unigrams'][w]+=1
#                    for b in bigrams(toks):
#                        counterDict['fb']['bigrams'][b]+=1
#                    for t in trigrams(toks):
#                        counterDict['fb']['trigrams'][t]+=1
                    # This is exploding in memory
                    # TODO find better way to store, DB?

                ###############################################   
                '''Write FB message to file'''
                if not fbTime==None:
                # If time is missing, cannot write to daily file
                    tweetFileName=dateFileName(fbTime,l)
                    # This is the file where message should be put
                                
                    if inCountry and not tweetFileName in dateFileHash.keys() and languageMatch:
                        dateFileHash[tweetFileName]=open(tweetFileName,'w')
                    # Add to hash
                    if inCountry and languageMatch:
                        dateFileHash[tweetFileName].write(json.dumps(message).encode('utf-8')+'\n')
                    #Write tweet to daily file
            else:
                nFileDuplicate+=1
        ###################################
        elif 'deleted' in message.keys():
        ###################################
            try:
                deletionsFile.write(str(message['twitter']['id'])+'\n')
            except:
                deletionsFile.write(str(message['twitter']['retweeted']['id'])+'\n')
            nDeleted+=1
        else:
            print 'WEIRD',message.keys()
            time.sleep(10000)
    # This tweet has been deleted
    # process_deletions.py will deal with these
    # as per http://dev.datasift.com/docs/resources/twitter-deletes

    fileString=None
    fileHandle.close()

    '''Now aggregate'''  
    '''tw'''
    '''topics'''
    tempTopicDf=pd.DataFrame(data={'topics':twTopics,'content':1},index=twTopicTimes)
    # Make a dataframe with contents of this file
    if len(twTopics)>0:
    # Only aggregate topics if some tweets were tagged with topics
        topicGroups=tempTopicDf.groupby(twTopics)
        # Group dataframe by topics

        for topic,topicDf in topicGroups:
            if not topic in counterDict['tw']['topics'].keys():
                counterDict['tw']['topics'][topic]=topicDf.resample('D',how='count')['content']
                # First time through, add downsampled series
            else:
                counterDict['tw']['topics'][topic]=counterDict['tw']['topics'][topic].add(topicDf.resample('D',how='count')['content'],fill_value=0)
                # Then add series from each new file to running total
                # If time ranges don't overlap explicitly add a zero
                totalDf=pd.concat([tdf for t,tdf in topicGroups])
    '''sentiments'''
    tempDf=pd.DataFrame(data={'time':twTimes,'pos':twPositives,'neg':twNegatives},index=twTimes)
    # Make a dataframe with contents of this file
    # TODO count over something better

    if not type(counterDict['tw']['time'])==pd.Series:
        counterDict['tw']['time']=tempDf.resample('D',how='count')['time']
        counterDict['tw']['pos']=tempDf.resample('D',how='sum')['pos']
        counterDict['tw']['neg']=tempDf.resample('D',how='sum')['neg']
        # First time through, add downsampled series
    else:
        counterDict['tw']['time']=counterDict['tw']['time'].add(tempDf.resample('D',how='count')['time'],fill_value=0)
        counterDict['tw']['pos']=counterDict['tw']['pos'].add(tempDf.resample('D',how='sum')['pos'],fill_value=0)
        counterDict['tw']['neg']=counterDict['tw']['neg'].add(tempDf.resample('D',how='sum')['neg'],fill_value=0)
    
    ''' facebook'''
    '''topics'''
    tempTopicDf=pd.DataFrame(data={'topics':fbTopics,'content':1},index=fbTopicTimes)
    # Make a dataframe with contents of this file
    if len(fbTopics)>0:
    # Only aggregate topics if some tweets were tagged with topics
        topicGroups=tempTopicDf.groupby(fbTopics)
        # Group dataframe by topics

        for topic,topicDf in topicGroups:
            if not topic in counterDict['fb']['topics'].keys():
                counterDict['fb']['topics'][topic]=topicDf.resample('D',how='count')['content']
                # First time through, add downsampled series
            else:
#                print type(counterDict['tw']['topics'][topic]),type(counterDict['tw']['topics']),topic
                counterDict['fb']['topics'][topic]=counterDict['fb']['topics'][topic].add(topicDf.resample('D',how='count')['content'],fill_value=0)
                # Then add series from each new file to running total
                # If time ranges don't overlap explicitly add a zero
                totalDf=pd.concat([tdf for t,tdf in topicGroups])

    '''sentiments'''
    tempDf=pd.DataFrame(data={'time':fbTimes,'pos':fbPositives,'neg':fbNegatives},index=fbTimes)
    # Make a dataframe with contents of this file
    # TODO count over something better

    if not type(counterDict['fb']['time'])==pd.Series:
        counterDict['fb']['time']=tempDf.resample('D',how='count')['time']
        counterDict['fb']['pos']=tempDf.resample('D',how='sum')['pos']
        counterDict['fb']['neg']=tempDf.resample('D',how='sum')['neg']
        # First time through, add downsampled series
    else:
        counterDict['fb']['time']=counterDict['fb']['time'].add(tempDf.resample('D',how='count')['time'],fill_value=0)
        counterDict['fb']['pos']=counterDict['fb']['pos'].add(tempDf.resample('D',how='sum')['pos'],fill_value=0)
        counterDict['fb']['neg']=counterDict['fb']['neg'].add(tempDf.resample('D',how='sum')['neg'],fill_value=0)

    '''volume over time'''
    tempFbDf=pd.DataFrame(data={'time':fbTimes},index=fbTimes)

    if not type(counterDict['fb']['time'])==pd.Series:
        counterDict['fb']['time']=tempFbDf.resample('D',how='count')['time']
    else:
        counterDict['fb']['time']=counterDict['fb']['time'].add(tempFbDf.resample('D',how='count')['time'],fill_value=0)
   ####################### 
    if v:
        print 'TIME (tw,fb)\tHASHTAG\tMENTION\tDOMAIN\tLOC\tGENDER\tUSER\tDEL\tDUP\tFB\tTOPICS\tTOTAL'
        print '\t',nTwTimeError,nFbTimeError,'\t',nHashTagError,'\t',nMentionError,'\t',nDomainError,'\t',nLocationError,'\t',nGenderError,'\t',nUserError,'\t',nDeleted,'\t',nFileDuplicate,'\t',nFacebook,'\t',nTopicError,'\t',nTotal
    if r:os.remove(f)
    # Delete file when not needed any more
    return counterDict,dateFileHash,nFileDuplicate,nTotal,nDeleted,nUserError,nFacebook

#############
def writeCounters(l,counterDict):
#############
    '''Overwrites old pickle file, having read in any previous counter pickle file'''
    v=True
    if v:print '\tWRITING PICKLE FILES'

    try:
        os.remove(l+counterFileName)
    except:
        if v:print 'NO OLD PICKLE FILE TO REMOVE'
    outFile=open(l+counterFileName,'w')
    
    pickle.dump(counterDict,outFile,2)
    
    outFile.close()
    
#############
def initDC(l):
#############
    '''Opens file for input into DC.js writes header and returns file handle'''
    if chosenCountry:
        dcFile=csv.writer(open(l+'/'+'dc_'+'_'.join(chosenLanguagesCountries)+'.csv','w'),delimiter=',')
    else:
        dcFile=csv.writer(open(l+'/'+'dc.csv','w'),delimiter=',')
    dcFile.writerow(['city','lat','lon','origdate','topic'])

    return dcFile

#############
def initCarto(cartoFileName,l):
#############
    '''
    Returns an new CSV object to write cartoDB info to
    '''
    if os.path.isfile(l+cartoFileName):return csv.writer(open(l+cartoFileName,'a'),delimiter='\t')
    else:return csv.writer(open(l+cartoFileName,'w'),delimiter='\t')

#############
def initDeletionsFile(l):
#############
    '''
    Returns an open file handle to write deleted tweets
    '''
    if not chosenCountry:
        f=open(l+'deletions.csv','w')
    else:
        f=open(l+'deletions_'+'_'.join(chosenLanguagesCountries)+'.csv','w')
    # Open a fresh file for now

    return f

#############
def initCounters(l):
#############
    '''
    Returns a dictionary dictionaries. Keys are sources and values
    are counters, a set of all tweet IDs 
    processed and a set of DataSift files already processed
    '''
    counterDict={'fb':{},'tw':{}}
    
    if os.path.isfile(l+counterFileName):
        print '\tFOUND PICKLE FILE',l+counterFileName
        inFile=open(l+counterFileName,'r')
        
        data=pickle.load(inFile)
        twCountryCounter=data['tw']['country']
        twTopicSums=data['tw']['topic_sums']
        twTopicColocCounter=data['tw']['topic_coloc']        
        twTopicCountryCounter=data['tw']['topicCountry']        
        twGenderTopicCounter=data['tw']['genderTopic']        
        twUserCounter=data['tw']['users']
        twMentionCounter=data['tw']['mentions']
        twUnigramCounter=data['tw']['unigrams']
        twBigramCounter=data['tw']['bigrams']
        twTrigramCounter=data['tw']['trigrams']
        twHashTagCounter=data['tw']['hashtags']
        twDomainCounter=data['tw']['domains']
        twRawDomainCounter=data['tw']['rawdomains']
        twTimeSeries=data['tw']['time']
        twPosSeries=data['tw']['pos']
        twNegSeries=data['tw']['neg']
        twIdSet=data['tw']['ids']
        twTopicCounter=data['tw']['topics']
        twLanguageCounter=data['tw']['languages']
        twLinkCounter=data['tw']['links']

        
        fbCountryCounter=data['fb']['country']
        fbTimeSeries=data['fb']['time']
        fbPosSeries=data['fb']['pos']
        fbNegSeries=data['fb']['neg']
        fbIdSet=data['fb']['ids']
        fbTopicCounter=data['fb']['topics']
        fbUnigramCounter=data['fb']['unigrams']
        fbBigramCounter=data['fb']['bigrams']
        fbTrigramCounter=data['fb']['trigrams']
        fbUserCounter=data['fb']['users']
        fbLanguageCounter=data['fb']['languages']
        fbDomainCounter=data['fb']['domains']
        fbTopicCountryCounter=data['fb']['topicCountry']        
        fbGenderTopicCounter=data['fb']['genderTopic']        
        fbTopicSums=data['fb']['topic_sums']
        fbLinkCounter=data['fb']['links']
        
        dsFileSet=data['ds']

        inFile.close()
        data=None
    else:
        print 'DIDNT FIND OLD PICKLE FILE'
        print 'ATTEMPTING TO CLEAN OLD DAILY FILES'
        ''' 
        nRemoved=0        
        for f in glob.glob(l+dateFileFormat):
            os.remove(f)
            nRemoved+=1
        print 'REMOVED',nRemoved
        time.sleep(3)
        '''
        removeDailyFiles(l,dateFileFormat)
        try:
            os.remove(l+cartoFileName)
        except:
            pass

        fbTimeSeries=None
        fbPosSeries=None
        fbNegSeries=None
        fbIdSet=set([])
        fbCountryCounter=collections.defaultdict(int)
        fbTopicCounter={}
        fbTopicCountryCounter={}
        fbGenderTopicCounter={}
        fbTopicSums=collections.defaultdict(int)
        fbUnigramCounter=collections.defaultdict(int)
        fbBigramCounter=collections.defaultdict(int)
        fbTrigramCounter=collections.defaultdict(int)
        fbUserCounter=collections.defaultdict(int)
        fbLanguageCounter=collections.defaultdict(int)
        fbDomainCounter=collections.defaultdict(int)
        fbLinkCounter=collections.defaultdict(int)

        twTopicColocCounter=collections.defaultdict(int)
        twHashTagCounter=collections.defaultdict(int)
        twDomainCounter=collections.defaultdict(int)
        twRawDomainCounter=collections.defaultdict(int)
        twMentionCounter=collections.defaultdict(int)
        twUnigramCounter=collections.defaultdict(int)
        twBigramCounter=collections.defaultdict(int)
        twTrigramCounter=collections.defaultdict(int)
        twUserCounter=collections.defaultdict(int)
        twLanguageCounter=collections.defaultdict(int)
        twTopicSums=collections.defaultdict(int)
        twLinkCounter=collections.defaultdict(int)
        twTopicCounter={}
        twCountryCounter=collections.defaultdict(int)
        twTopicCountryCounter={}
        twGenderTopicCounter={}
        twTimeSeries=None
        twPosSeries=None
        twNegSeries=None
        twSeries=None
        twIdSet=set([])
        
        dsFileSet=Set()

    counterDict['tw']['country']=twCountryCounter
    counterDict['tw']['topic_sums']=twTopicSums
    counterDict['tw']['topic_coloc']=twTopicColocCounter
    counterDict['tw']['hashtags']=twHashTagCounter
    counterDict['tw']['domains']=twDomainCounter
    counterDict['tw']['rawdomains']=twRawDomainCounter
    counterDict['tw']['mentions']=twMentionCounter
    counterDict['tw']['links']=twLinkCounter
    counterDict['tw']['time']=twTimeSeries
    counterDict['tw']['pos']=twPosSeries
    counterDict['tw']['neg']=twNegSeries
    counterDict['tw']['ids']=twIdSet
    counterDict['tw']['topics']=twTopicCounter
    counterDict['tw']['unigrams']=twUnigramCounter
    counterDict['tw']['bigrams']=twBigramCounter
    counterDict['tw']['trigrams']=twTrigramCounter
    counterDict['tw']['users']=twUserCounter
    counterDict['tw']['topicCountry']=twTopicCountryCounter
    counterDict['tw']['genderTopic']=twGenderTopicCounter
    counterDict['tw']['languages']=twLanguageCounter

    counterDict['fb']['country']=fbCountryCounter
    counterDict['fb']['time']=fbTimeSeries
    counterDict['fb']['pos']=fbPosSeries
    counterDict['fb']['neg']=fbNegSeries
    counterDict['fb']['ids']=fbIdSet
    counterDict['fb']['topics']=fbTopicCounter
    counterDict['fb']['topic_sums']=fbTopicSums
    counterDict['fb']['unigrams']=fbUnigramCounter
    counterDict['fb']['bigrams']=fbBigramCounter
    counterDict['fb']['trigrams']=fbTrigramCounter
    counterDict['fb']['users']=fbUserCounter
    counterDict['fb']['languages']=fbLanguageCounter
    counterDict['fb']['domains']=fbDomainCounter
    counterDict['fb']['links']=fbLinkCounter
    counterDict['fb']['topicCountry']=fbTopicCountryCounter
    counterDict['fb']['genderTopic']=fbGenderTopicCounter
    
    counterDict['ds']=dsFileSet
    
    return counterDict,dsFileSet
#############
def main():
#############

    nDuplicates=0
    # Reset this for each language
  
    nSkippedFiles=0
    # Counts DataSift fiels that we already processed
   
    nTotal=0
    # Total number of tweets processed, excluding duplicates and skipped files

    nDeletes=0
    # Count number of tweets in stream that are deleted messages
    
    nFacebook=0
    # Count number of FB posts

    fileCounter=0

    idSet=Set()
    # This is a set that hlds all tweet ids, to ensure duplicates are removed
    # New set for each language (assumes langages will have separate DS streams)
   
    dsFileSet=Set()
    # This set holds all the DataSift file names
    # Thus duplicate files are skipped over
    
    l=dataDirectory

    counterDict,dsFileSet=initCounters(l)
    # Look for dumpfile to load in counters, if not there create empty ones

    deletionsFile=initDeletionsFile(l)

    cartoFile=initCarto(cartoFileName,l)
 
    dateFileNames=glob.glob(l+dateFileFormat)
    # These are the daily files which already exist in language directory
    
    dcFile=initDC(l)
    # This is file used by DC.js with geo-located messages

    dateFileHash={}
    for d in dateFileNames:
        dateFileHash[d]=open(d,'a')
        print d
    # Create a dictionary, key is date file name
    # value is file opened for appending
    for f in fileStream(l):
        if not f in dsFileSet:
            if v:print '\tPROCESSING',fileCounter,f
            counterDict,dateFileHash,nFileDuplicates,nFile,nFileDeletes,nUserError,nFileFacebook=processFile(l,f,dateFileHash,counterDict,cartoFile,deletionsFile,dcFile) 
            # Read file and process line by line
            # Update hash of daily files in case new ones are created
            
            nDuplicates+=nFileDuplicates
            nTotal+=nFile
            nDeletes+=nFileDeletes
            nFacebook+=nFileFacebook
            dsFileSet.add(f)
            fileCounter+=1
        else:
            if v:print '\tSKIPPING DUPLICATE',f 
            nSkippedFiles+=1
    counterDict['ds']=dsFileSet
    
    for kk,vv in counterDict['tw']['topics'].items():
        counterDict['tw']['topic_sums'][kk]=vv.sum()
    for kk,vv in counterDict['fb']['topics'].items():
        counterDict['fb']['topic_sums'][kk]=vv.sum()
    # Sum topic volumes

    writeCounters(l,counterDict)
    # Persist counters to pickle file

    deletionsFile.close()

    if v:
        print 'AT END', 
        printTop(counterDict['tw']['hashtags'],10)
        print '----------------------------------------------'
    if True:
        print 'SKIPPED',nSkippedFiles
        print 'DUPLICATES',nDuplicates
        print 'DELETED',nDeletes
        print 'TOTAL NEW',nTotal
        print 'TOTAL FACEBOOK',nFacebook

    for d in dateFileHash.values():
        d.close()
    # Close out fdaily files
#############################
if __name__=="__main__":
    main()
