'''A bunch of simple convenience functions'''
import operator
import time,csv,traceback
from geopy import distance

stopWords=['a','the','of']

posRe=r':-\)|:\)'
negRe=r':-\(|:\('
cleanRe=r'\n|\r\n'

#############
def getClosestCity(cities,tCoords,tol=150):
#############
    '''Takes tuple of coordinates, cycles through cities 
    in global variable <cities>, reads their coords from
    global variable <coords> and returns closest
    ------
    returns tuple of coords of closest city,city name
    OR None, if no city within tolerance'''
    dist=999999
    closest='ZZZZ'
    cCoords=[]
    for c in cities:
        cDist=distance.distance(tCoords,c[1:])
        if cDist<tol:
            dist=cDist
            closest=c[0]
            cCoords=c[1:]
    if dist<tol:
        return cCoords,closest
    else:
        return None,None
#############
def getCities(citiesFileName,geo):
#############
    '''
    Reads in a text file of city names
    Geolocates each city and returns a list of
    tuples; (cityname,cityLat,cityLong)
    TODO add some kind of context so the correct global city is chosen
    '''
    cities=[]
    nCityError=0

    inFile=csv.reader(open(citiesFileName,'r'),delimiter='\t')
    for l in inFile:
        try:
            city=l[0].decode('utf-8')
            coords=geo.geoLocate(city)
            coords=coords[0][1:3]
            cities.append((city,coords[0],coords[1]))
        except:
            nCityError+=1
    return cities,nCityError
#############
def getISODate(dummyTime):
#############
    '''Takes Twitter timestamp
    ------
    returns iso format timestamp -> YYY-MM-DD hh:mm:ss
    '''
    # Get from this format: Thu, 02 Jan 2014 16:26:15 +0000...
    timeStruct=time.strptime(dummyTime,'%a, %d %b %Y %H:%M:%S +0000')
    return str(timeStruct[0])+'-'+str(timeStruct[1])+'-'+str(timeStruct[2])+' '+str(timeStruct[3])+':'+str(timeStruct[4])+':'+str(timeStruct[5])
    # ...into this format mm/DD/YYYYYYY-MM-DD hh:mm:ss
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

