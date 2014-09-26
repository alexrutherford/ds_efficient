'''A bunch of simple convenience functions'''
import operator
stopWords=['a','the','of']

#############
def printTop(d,n=10):
#############
    ''' Takes dictionary and returns top n 
    items according to value''' 
    sortedD=sorted(d.iteritems(), key=operator.itemgetter(1))
    for e in sortedD[-1*n:]:
        print e
    print len(d.keys())
#############
def hasGeolocation(t):
#############
    '''Test if tweet JSON has GP geolocation field. Returns Boolean'''
    if 'ungp' in t.keys():
        if 'geolocation' in t['ungp'].keys():
            return True
    return False

