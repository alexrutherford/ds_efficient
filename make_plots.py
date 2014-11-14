'''
Script to read in counters*.dat produced by process_ds_files.py
Produces plots of time series and bar charts of top mentions etc
Replaces plot_stuff_RCN.py and time_volume_plot_efficient.ipynb.
Considers twitter and Facebook streams
'''
import matplotlib
matplotlib.use('Agg')

import cPickle as pickle
import pandas as pd
from utils import *
import mpld3
import seaborn as sns
import matplotlib.pyplot as plt

import sys,os
sys.path.append('/mnt/home/ubuntu/projects/tools/')

from gp_colours import *
from matplotlib.ticker import ScalarFormatter
formatter = ScalarFormatter()
formatter.set_scientific(False)

import itertools
plotColours=['#1B6E44','#6D1431','#FF5500','#5D6263','#009D97','#c84699','#71be45','#3F8CBC','#FFC200']
# Currently have 9 nice colours, colours will be repeated if more than 9 topics

inDirectory='../data_test/english/'
inFileName=inDirectory+'counters.dat'
#inFileName=inDirectory+'counters_BR.dat'

if '-d' in sys.argv:
    # Set input directory
    i=(sys.argv).index('-d')
    inDirectory=sys.argv[i+1]
    inFileName=inDirectory+'counters.dat'
    print 'SET INPUT FILE',inFileName
    time.sleep(1)

if '-C' in sys.argv:
    # Flag for filtering by country
    chosenCountry=sys.argv[(sys.argv).index('-C')+1]
    inFileName=inDirectory+'counters_'+chosenCountry+'.dat'
    print 'ADDED COUNTRY FLAG',chosenCountry
    print 'SET INPUT FILE',inFileName
    time.sleep(1)
    # If a flag used to filter by country
    # need to change the format of daily files

if '-clean' in sys.argv:
    # Clean out plots and data directories 
    try:
        os.system('rm -r '+inDirectory+'data/')     
    except:
        print 'CANT REMOVE data/'
    try:
        os.system('rm -r '+inDirectory+'plots/')    
    except:
        print 'CANT REMOVE plots/'

    print 'CLEANING OLD OUTPUT'
    time.sleep(1)


inFile=open(inFileName,'r')
data=pickle.load(inFile)

nTopics=0
for source in ['tw','fb']:
    try:
        nSourceTopics=len(data[source]['topic_sums'].keys())
        if nSourceTopics>nTopics:nTopics=nSourceTopics
    except:
        print 'NO TOPICS FOR',source

plotCycle=itertools.cycle(plotColours[0:nTopics])
print 'GOT',nTopics,'TOPICS'
time.sleep(1)

try:
    os.mkdir(inDirectory+'plots')
    os.mkdir(inDirectory+'data')
except:
    print 'plots AND data DIRECTORIES ALREADY EXIST'
# Where to place plots/data

chosenLanguage=''
# Use this flag if we want to distinguish between plots from several corpora

if '-l' in sys.argv:
    # Flag for filtering by language
    i=(sys.argv).index('-l')
    chosenLanguage=sys.argv[i+1]
    print 'ADDED LANG FLAG',chosenLanguage
    time.sleep(1)

#######
def writeTop(counter,f):
######
    '''
    Convenience function to write list of tuples
    (invariably from sorted dictionary)
    '''
    import csv
    counter.reverse()
    outFile=csv.writer(open(f,'w'),delimiter='\t')
    outFile.writerow(['name','value'])
    for l in counter:
        outFile.writerow([l[0],str(l[1])])
#############
def main():
#############
    for source in ['tw','fb']:
        print source,'TOPICS'
        #####################
        # Topics
        topicSums={}


        for k,v in data[source]['topics'].items():
#            print k,v.sum()
            topicSums[k]=v.sum()
        topicSums=sorted(topicSums.items(), key=operator.itemgetter(1))

        maxs={}
        sortedMaxs=[]
        for a,b in data[source]['topics'].items():
            if not a==u'NaN':
                maxs[a]=b.sum()
                sortedMaxs=sorted(maxs.iteritems(), key=operator.itemgetter(1))
                sortedMaxs.reverse()

        fig = plt.figure()
        ax=fig.add_subplot(111)
        ax.barh(range(len(topicSums)),[v[1] for v in topicSums],log=False,linewidth=0,alpha=0.7,color="#00aeef")
        ax.set_axis_bgcolor('#efefef')
        ax.xaxis.set_major_formatter(formatter)
        ax.xaxis.set_major_locator(plt.MaxNLocator(4))
        ax.set_yticks([i+0.5 for i in range(len(topicSums))])
        ax.set_xlabel('Number of Tweets')
        ax.set_yticklabels([v[0] for v in topicSums]);
        plt.savefig(inDirectory+'plots/'+source+'_topics_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
        # Get sum of topics, use this to plot in some kind of order
        writeTop(topicSums,inDirectory+'data/'+source+'_topics_'+chosenLanguage+'.tsv')
        #####################
        # Plot total voume time series

        print source,'TOTAL'
        try: 
            sns.set(context='poster', style='whitegrid', font='Arial', rc={'font.size': 14, 'axes.labelsize': 16, 'legend.fontsize': 14.0,'axes.titlesize': 12, 'xtick.labelsize': 14, 'ytick.labelsize': 14})
            sns.despine()
            rc={'font.size': 14, 'axes.labelsize': 16, 'legend.fontsize': 14.0, 
                'axes.titlesize': 12, 'xtick.labelsize': 14, 'ytick.labelsize': 12}
            totalSeriesFig, ax = plt.subplots()
            ax=data[source]['time'][0:-1].plot(legend=False,figsize=(14,8),style=gpBlue,lw=7)
            ax.grid(color='lightgray', alpha=0.4)
            ax.axis(ymin=0)
            plt.savefig(inDirectory+'plots/'+source+'_total_'+chosenLanguage+'.png',dpi=60)
            mpld3.save_html(totalSeriesFig, inDirectory+'plots/'+source+'_total_'+chosenLanguage+'.php', figid=chosenLanguage+"TotalSeriesFig")
        except:
            print source,'TOTAL FAILED'
            print traceback.print_exc()
        #####################
        # Plot all topics together
        try:
            allSeriesFig, ax = plt.subplots()
            for c in sortedMaxs:
                if not c[0]=='NaN':
                    a=c[0]
                    b=data[source]['topics'][a]

                    col=plotCycle.next()
#                print a,col
                    ax=b.plot(label=a,legend=True,figsize=(14,8),style=col,lw=3)
                    ax.grid(color='lightgray', alpha=0.7)
                    xlabels = ax.get_xticklabels()
                    ylabels = ax.get_yticklabels()
                    mpld3.save_html(allSeriesFig, inDirectory+'plots/'+source+'_all_'+chosenLanguage+'.php', figid=chosenLanguage+"AllSeriesFig")
        except:
            print source,'ALL TOPICS FAILED'
        ######################
# Plot individual topics

        print source,'INDIVIDUAL TOTAL'
        plt.cla() 
        for c in sortedMaxs:    
#            print '\t',c
            if not c[0]==u'NaN':
                a=c[0]
                b=data[source]['topics'][a]
                col=plotCycle.next()
#                print a,col,b.max(),b.min()
                sns.set(context='poster', style='whitegrid', font='Arial', rc={'font.size': 14, 'axes.labelsize': 16, 'legend.fontsize': 14.0,'axes.titlesize': 12, 'xtick.labelsize': 20, 'ytick.labelsize': 18})
                ax=b.plot(label=a,legend=False,figsize=(20,12),style=col,lw=7)
                xlabels = ax.get_xticklabels()
                ylabels = ax.get_yticklabels()
                plt.savefig(inDirectory+'plots/'+source+'_'+a.replace('/','_')+'_'+chosenLanguage+'.png',dpi=60)
#        print '\t',tS["webDir"]+'/charts/PLOT_'+chosenLanguage+'_'+a.replace('/','_')+'.png'
                plt.cla();

        #####################
# Plot sentiment

        print source,'SENTIMENT'
        try: 
            sentimentFig, ax = plt.subplots()
            data[source]['pos'].plot(label='Positive',legend=True, figsize=(14,8),lw=3)
            (-1.0*data[source]['neg']).plot(label='Negative',legend=True,lw=3)
            ax.grid(color='lightgray', alpha=0.7)
            xlabels = ax.get_xticklabels()
            ylabels = ax.get_yticklabels()
            mpld3.save_html(sentimentFig, inDirectory+'plots/'+source+'_sentiment_'+chosenLanguage+'.php', figid=chosenLanguage+"sentimentFig")
        except:
            print source,'SENTIMENT FAILED'
        #######################
# Plot top hashtags
        try:
            print source,'HASHTAGS'
            
            data[source]['hashtags']=sorted(data[source]['hashtags'].items(), key=operator.itemgetter(1))

            fig, ax = plt.subplots()
            ax.barh(range(10),[v[1] for v in data[source]['hashtags'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
            ax.set_axis_bgcolor('#efefef')
#ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            ax.set_yticks([i+0.5 for i in range(10)])
            ax.set_xlabel('Number of Tweets')
            ax.set_yticklabels(['#'+v[0] for v in data[source]['hashtags'][-10:]]);
            plt.savefig(inDirectory+'plots/'+source+'_hashtags_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['hashtags'][-10:],inDirectory+'data/'+source+'_hashtags_'+chosenLanguage+'.tsv')
        except:
            print source,'HASHTAGS FAILED'
        ##########################
# plot top raw URLS
        try:
            print source,'LINKS'
            
            data[source]['links']=sorted(data[source]['links'].items(), key=operator.itemgetter(1))
            fig, ax = plt.subplots()
            ax.barh(range(10),[v[1] for v in data[source]['links'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
            ax.set_axis_bgcolor('#efefef')
            ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            ax.set_yticks([i+0.5 for i in range(10)])
            ax.set_xlabel('Number of Tweets')
            ax.set_yticklabels([v[0] for v in data[source]['links'][-10:]]);
            plt.savefig(inDirectory+'plots/'+source+'_links_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['links'][-10:],inDirectory+'data/'+source+'_rawDomains_'+chosenLanguage+'.tsv')
        except:
            print 'LINKS FAILED',source

# plot top URLS
        try:
            print source,'DOMAINS'
            
            data[source]['domains']=sorted(data[source]['domains'].items(), key=operator.itemgetter(1))
            fig, ax = plt.subplots()
            ax.barh(range(10),[v[1] for v in data[source]['domains'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
            ax.set_axis_bgcolor('#efefef')
            ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            ax.set_yticks([i+0.5 for i in range(10)])
            ax.set_xlabel('Number of Tweets')
            ax.set_yticklabels([v[0] for v in data[source]['domains'][-10:]]);
            plt.savefig(inDirectory+'plots/'+source+'_domains_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['domains'][-10:],inDirectory+'data/'+source+'_domains_'+chosenLanguage+'.tsv')
        except:
            print 'DOMAINS FAILED',source
        ###########################
        # Plot top unigrams
        print source,'UNIGRAMS'
        try: 
            data[source]['unigrams']=sorted(data[source]['unigrams'].items(), key=operator.itemgetter(1))
            
            fig, ax = plt.subplots()
            ax.barh(range(10),[v[1] for v in data[source]['unigrams'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
            ax.set_axis_bgcolor('#efefef')
            ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            ax.set_yticks([i+0.5 for i in range(10)])
            ax.set_xlabel('Number of Tweets')
            ax.set_yticklabels([v[0].decode('utf-8') for v in data[source]['unigrams'][-10:]]);
            plt.savefig(inDirectory+'plots/'+source+'_unigrams_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
        except:
            print source,'UNIGRAMS FAILED'
        ###########################
#   Write top users
        print source,'USERS'
        try:
            data[source]['users']=sorted(data[source]['users'].items(), key=operator.itemgetter(1))

            fig, ax = plt.subplots()
            ax.barh(range(10),[v[1] for v in data[source]['users'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
            ax.set_axis_bgcolor('#efefef')
            ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            ax.set_yticks([i+0.5 for i in range(10)])
            ax.set_xlabel('Number of Tweets')
            ax.set_yticklabels(['@'+v[0] for v in data[source]['users'][-10:]]);
            plt.savefig(inDirectory+'plots/'+source+'_users_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['users'][-10:],inDirectory+'data/'+source+'_accounts_'+chosenLanguage+'.tsv')
        except:
            print source,'USERS FAILED'

        ##############################
# Write top mentions

        print source,'MENTIONS'
        try:        
            data[source]['mentions']=sorted(data[source]['mentions'].items(), key=operator.itemgetter(1))
            fig, ax = plt.subplots()
            ax.barh(range(10),[v[1] for v in data[source]['mentions'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
            ax.set_axis_bgcolor('#efefef')
            ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            ax.set_yticks([i+0.5 for i in range(10)])
            ax.set_xlabel('Number of Tweets')
            ax.set_yticklabels(['@'+v[0] for v in data[source]['mentions'][-10:]]);
            plt.savefig(inDirectory+'plots/'+source+'_mentions_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['mentions'][-10:],inDirectory+'data/'+source+'_mentions_'+chosenLanguage+'.tsv')
        except:
            print source,'MENTIONS FAILED'

        ###################
# Write top bigrams

        print source,'BIGRAMS'
        try:    
            data[source]['bigrams']=sorted(data[source]['bigrams'].items(), key=operator.itemgetter(1))

            fig, ax = plt.subplots()
            ax.barh(range(10),[v[1] for v in data[source]['bigrams'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
            ax.set_axis_bgcolor('#efefef')
            ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            ax.set_yticks([i+0.5 for i in range(10)])
            ax.set_xlabel('Number of Tweets')
            ax.set_yticklabels([v[0][0].decode('utf-8')+' '+v[0][1].decode('utf-8') for v in data[source]['bigrams'][-10:]]);
            plt.savefig(inDirectory+'plots/'+source+'_bigrams_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
        except:
            print source,'BIGRAMS FAILED'
        ####################
# Write top trigrams

        print source,'TRIGRAMS'
        try:
         
            data[source]['trigrams']=sorted(data[source]['trigrams'].items(), key=operator.itemgetter(1))
            
            fig, ax = plt.subplots()
            ax.barh(range(10),[v[1] for v in data[source]['trigrams'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
            ax.set_axis_bgcolor('#efefef')
            ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            ax.set_yticks([i+0.5 for i in range(10)])
            ax.set_xlabel('Number of Tweets')
            ax.set_yticklabels([v[0][0].decode('utf-8')+' '+v[0][1].decode('utf-8')+' '+v[0][2].decode('utf-8') for v in data[source]['trigrams'][-10:]]);
            plt.savefig(inDirectory+'plots/'+source+'_trigrams_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
        except:
            print source,'TRIGRAMS FAILED'
        print '--------'
#TODO plot of both sources together
    plt.cla()
    try:
        print '<ALL> COMBINED'
        data['fb']['time'][0:-1].plot(label='FB',legend=True)
        data['tw']['time'][0:-1].plot(label='TW',legend=True)
        (data['tw']['time'][0:-1]+data['fb']['time']).plot(label='ALL',legend=True)
        plt.savefig(inDirectory+'plots/total_combined.png',bbox_inches='tight',dpi=200)
    except:
        print 'COMBINED PLOT FAILED'
if __name__=='__main__':
    main()
