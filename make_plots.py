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

import argparse
parser=argparse.ArgumentParser()


parser.add_argument("input",help='specify input directory') # Compulsory
parser.add_argument("-c","--cities",help='specify file of city names for snapping')
parser.add_argument("-H","--hack",help='specify topic hack',action="store_true")
parser.add_argument("--clean",help='flag to delete existing daily files and counter objects in input directory',action='store_true')
parser.add_argument("-C","--country",help='specify 2 letter uppercase ISO codes of country',nargs='+')
parser.add_argument("-L","--language",help='specify 2 letter lowercase codes of languages',nargs='+')
args=parser.parse_args()

inDirectory=args.input
inFileName=inDirectory+'counters.dat'
#inFileName=inDirectory+'counters_BR.dat'

chosenCountry=args.country

chosenLanguages=args.language

chosenLanguagesCountries=[]
if chosenLanguages:
    chosenLanguagesCountries.extend([c.lower() for c in chosenLanguages])
if chosenCountry:
    chosenLanguagesCountries.extend([c.upper() for c in chosenCountry])
if len(chosenLanguagesCountries)==0:chosenLanguagesCountries=None

print chosenLanguagesCountries

if chosenLanguagesCountries:
    inFileName=inDirectory+'/counters_'+'_'.join(chosenLanguagesCountries)+'.dat'
# Have a separate counter file for each country

if args.clean:
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

#chosenLanguage='english'
suffix=''
if chosenLanguagesCountries:
    suffix='_'.join(chosenLanguagesCountries)
# Use this flag if we want to distinguish between plots from several corpora

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
        plt.savefig(inDirectory+'plots/'+source+'_topics_'+suffix+'.png', bbox_inches='tight',dpi=200)
        # Get sum of topics, use this to plot in some kind of order
        writeTop(topicSums,inDirectory+'data/'+source+'_topics_'+suffix+'.tsv')
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
            plt.savefig(inDirectory+'plots/'+source+'_total_'+suffix+'.png',dpi=60)
            mpld3.save_html(totalSeriesFig, inDirectory+'plots/'+source+'_total_'+suffix+'.php', figid=suffix+"TotalSeriesFig")
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
                    mpld3.save_html(allSeriesFig, inDirectory+'plots/'+source+'_all_'+suffix+'.php', figid=suffix+"AllSeriesFig")
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
                plt.savefig(inDirectory+'plots/'+source+'_'+a.replace('/','_')+'_'+suffix+'.png',dpi=60)
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
            mpld3.save_html(sentimentFig, inDirectory+'plots/'+source+'_sentiment_'+suffix+'.php', figid=suffix+"sentimentFig")
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
            plt.savefig(inDirectory+'plots/'+source+'_hashtags_'+suffix+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['hashtags'][-10:],inDirectory+'data/'+source+'_hashtags_'+suffix+'.tsv')
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
            plt.savefig(inDirectory+'plots/'+source+'_links_'+suffix+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['links'][-10:],inDirectory+'data/'+source+'_rawDomains_'+suffix+'.tsv')
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
            plt.savefig(inDirectory+'plots/'+source+'_domains_'+suffix+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['domains'][-10:],inDirectory+'data/'+source+'_domains_'+suffix+'.tsv')
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
            plt.savefig(inDirectory+'plots/'+source+'_unigrams_'+suffix+'.png', bbox_inches='tight',dpi=200)
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
            plt.savefig(inDirectory+'plots/'+source+'_users_'+suffix+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['users'][-10:],inDirectory+'data/'+source+'_accounts_'+suffix+'.tsv')
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
            plt.savefig(inDirectory+'plots/'+source+'_mentions_'+suffix+'.png', bbox_inches='tight',dpi=200)
            writeTop(data[source]['mentions'][-10:],inDirectory+'data/'+source+'_mentions_'+suffix+'.tsv')
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
            plt.savefig(inDirectory+'plots/'+source+'_bigrams_'+suffix+'.png', bbox_inches='tight',dpi=200)
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
            plt.savefig(inDirectory+'plots/'+source+'_trigrams_'+suffix+'.png', bbox_inches='tight',dpi=200)
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
        plt.savefig(inDirectory+'plots/total_combined_'+suffix+'.png',bbox_inches='tight',dpi=200)
    except:
        print 'COMBINED PLOT FAILED'
if __name__=='__main__':
    main()
