'''
Script to read in counters*.dat produced by process_ds_files.py
Produces plots of time series and bar charts of top mentions etc
Replaces plot_stuff_RCN.py and time_volume_plot_efficient.ipynb
'''
import matplotlib
matplotlib.use('Agg')

import cPickle as pickle
import pandas as pd
from utils import *
import mpld3
import seaborn as sns
import matplotlib.pyplot as plt

import sys
sys.path.append('/mnt/home/ubuntu/projects/tools/')

from gp_colours import *
from matplotlib.ticker import ScalarFormatter
formatter = ScalarFormatter()
formatter.set_scientific(False)

import itertools
plotColours=['#1B6E44','#6D1431','#FF5500','#5D6263','#009D97','#c84699','#71be45','#3F8CBC','#FFC200']
plotCycle=itertools.cycle(plotColours)

inFile=open('../data/baseline3/counters.dat','r')
data=pickle.load(inFile)

chosenLanguage='english'

if '-l' in sys.argv:
    # Flag for filtering by country
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
    #####################
    # Topics
    topicSums={}
    for k,v in data['topics'].items():
        print k,v.sum()
        topicSums[k]=v.sum()
    topicSums=sorted(topicSums.items(), key=operator.itemgetter(1))

    maxs={}
    sortedMaxs=[]
    for a,b in data['topics'].items():
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
    plt.savefig('plots/hashtags_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
    # Get sum of topics, use this to plot in some kind of order

    #####################
    # Plot total voume time series

    sns.set(context='poster', style='whitegrid', font='Arial', rc={'font.size': 14, 'axes.labelsize': 16, 'legend.fontsize': 14.0,'axes.titlesize': 12, 'xtick.labelsize': 14, 'ytick.labelsize': 14})
    sns.despine()
    rc={'font.size': 14, 'axes.labelsize': 16, 'legend.fontsize': 14.0, 
        'axes.titlesize': 12, 'xtick.labelsize': 14, 'ytick.labelsize': 12}
    totalSeriesFig, ax = plt.subplots()
    ax=data['time'].plot(legend=False,figsize=(14,8),style=gpBlue,lw=7)
    ax.grid(color='lightgray', alpha=0.4)
    ax.axis(ymin=0)
    plt.savefig('plots/PLOT_'+chosenLanguage+'_Total.png',dpi=60)
    mpld3.save_html(totalSeriesFig, 'plots/PLOT_'+chosenLanguage+'_Total.php', figid=chosenLanguage+"TotalSeriesFig")

    #####################
    # Plot all topics together

    allSeriesFig, ax = plt.subplots()
    for c in sortedMaxs:
        if not c[0]=='NaN':
            a=c[0]
            b=data['topics'][a]

            col=plotCycle.next()
            print a,col
            ax=b.plot(label=a,legend=True,figsize=(14,8),style=col,lw=3)
            ax.grid(color='lightgray', alpha=0.7)
            xlabels = ax.get_xticklabels()
            ylabels = ax.get_yticklabels()
            mpld3.save_html(allSeriesFig, 'plots/PLOT_'+chosenLanguage+'_All.php', figid=chosenLanguage+"AllSeriesFig")

    ######################
# Plot individual topics

    for c in sortedMaxs:    
        if not c[0]==u'NaN':
            a=c[0]
            b=data['topics'][a]
            col=plotCycle.next()
            print a,col,b.max(),b.min()
            sns.set(context='poster', style='whitegrid', font='Arial', rc={'font.size': 14, 'axes.labelsize': 16, 'legend.fontsize': 14.0,'axes.titlesize': 12, 'xtick.labelsize': 20, 'ytick.labelsize': 18})
            ax=b.plot(label=a,legend=False,figsize=(20,12),style=col,lw=7)
            xlabels = ax.get_xticklabels()
            ylabels = ax.get_yticklabels()
            plt.savefig('plots/PLOT_'+chosenLanguage+'_'+a.replace('/','_')+'.png',dpi=60)
#        print '\t',tS["webDir"]+'/charts/PLOT_'+chosenLanguage+'_'+a.replace('/','_')+'.png'
            plt.cla();

    #####################
# Plot sentiment

    sentimentFig, ax = plt.subplots()
    data['pos'].plot(label='Positive',legend=True, figsize=(14,8),lw=3)
    (-1.0*data['neg']).plot(label='Negative',legend=True,lw=3)
    ax.grid(color='lightgray', alpha=0.7)
    xlabels = ax.get_xticklabels()
    ylabels = ax.get_yticklabels()
    mpld3.save_html(sentimentFig, 'plots/PLOT_Sentiment_'+chosenLanguage+'.php', figid=chosenLanguage+"sentimentFig")

    #######################
# Plot top hashtags

    data['hashtags']=sorted(data['hashtags'].items(), key=operator.itemgetter(1))

    fig, ax = plt.subplots()
    ax.barh(range(10),[v[1] for v in data['hashtags'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
    ax.set_axis_bgcolor('#efefef')
#ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(plt.MaxNLocator(4))
    ax.set_yticks([i+0.5 for i in range(10)])
    ax.set_xlabel('Number of Tweets')
    ax.set_yticklabels(['#'+v[0] for v in data['hashtags'][-10:]]);
    plt.savefig('plots/hashtags_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)

    ##########################
# plot top URLS

    data['domains']=sorted(data['domains'].items(), key=operator.itemgetter(1))

    fig, ax = plt.subplots()
    ax.barh(range(10),[v[1] for v in data['domains'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
    ax.set_axis_bgcolor('#efefef')
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(plt.MaxNLocator(4))
    ax.set_yticks([i+0.5 for i in range(10)])
    ax.set_xlabel('Number of Tweets')
    ax.set_yticklabels([v[0] for v in data['domains'][-10:]]);
    plt.savefig('plots/hashtags_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)

    ###########################
    # Plot top unigrams
    data['unigrams']=sorted(data['unigrams'].items(), key=operator.itemgetter(1))
    
    fig, ax = plt.subplots()
    ax.barh(range(10),[v[1] for v in data['unigrams'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
    ax.set_axis_bgcolor('#efefef')
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(plt.MaxNLocator(4))
    ax.set_yticks([i+0.5 for i in range(10)])
    ax.set_xlabel('Number of Tweets')
    ax.set_yticklabels([v[0].decode('utf-8') for v in data['unigrams'][-10:]]);
    plt.savefig('plots/hashtags_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)

    ###########################
#   Write top users
    data['users']=sorted(data['users'].items(), key=operator.itemgetter(1))

    fig, ax = plt.subplots()
    ax.barh(range(10),[v[1] for v in data['users'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
    ax.set_axis_bgcolor('#efefef')
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(plt.MaxNLocator(4))
    ax.set_yticks([i+0.5 for i in range(10)])
    ax.set_xlabel('Number of Tweets')
    ax.set_yticklabels(['@'+v[0] for v in data['users'][-10:]]);
    plt.savefig('plots/users_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
# TODO write these to file

    writeTop(data['users'][-10:],'data/users_test.tsv')
    
    ##############################
# Write top mentions

    data['mentions']=sorted(data['mentions'].items(), key=operator.itemgetter(1))
    fig, ax = plt.subplots()
    ax.barh(range(10),[v[1] for v in data['mentions'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
    ax.set_axis_bgcolor('#efefef')
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(plt.MaxNLocator(4))
    ax.set_yticks([i+0.5 for i in range(10)])
    ax.set_xlabel('Number of Tweets')
    ax.set_yticklabels(['@'+v[0] for v in data['mentions'][-10:]]);
    plt.savefig('plots/mentions_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)
    writeTop(data['mentions'][-10:],'data/mentions_test.tsv')

    ###################
# Write top bigrams
    data['bigrams']=sorted(data['bigrams'].items(), key=operator.itemgetter(1))

    fig, ax = plt.subplots()
    ax.barh(range(10),[v[1] for v in data['bigrams'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
    ax.set_axis_bgcolor('#efefef')
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(plt.MaxNLocator(4))
    ax.set_yticks([i+0.5 for i in range(10)])
    ax.set_xlabel('Number of Tweets')
    ax.set_yticklabels([v[0][0].decode('utf-8')+' '+v[0][1].decode('utf-8') for v in data['bigrams'][-10:]]);
    plt.savefig('plots/bigrams_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)

    ####################
# Write top trigrams

    data['trigrams']=sorted(data['trigrams'].items(), key=operator.itemgetter(1))
    
    fig, ax = plt.subplots()
    ax.barh(range(10),[v[1] for v in data['trigrams'][-10:]],log=False,linewidth=0,alpha=0.7,color="#00aeef")
    ax.set_axis_bgcolor('#efefef')
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_locator(plt.MaxNLocator(4))
    ax.set_yticks([i+0.5 for i in range(10)])
    ax.set_xlabel('Number of Tweets')
    ax.set_yticklabels([v[0][0].decode('utf-8')+' '+v[0][1].decode('utf-8')+' '+v[0][2].decode('utf-8') for v in data['trigrams'][-10:]]);
    plt.savefig('plots/trigrams_'+chosenLanguage+'.png', bbox_inches='tight',dpi=200)

if __name__=='__main__':
    main()
