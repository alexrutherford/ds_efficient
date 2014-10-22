Set of python scripts to parse large DataSift streaming corpora including several sources. Example deployment at [UN Global Pulse climate monitor](http://unglobalpulse.net/climate/)

1. ```python process_ds_files.py```
Cycles through all files from DataSift ```DataSift*json``` and places messages in daily files. Also counts mentions, hashtags and other attributes. Serialises data in ```counters.dat```. Deleted tweets that appear in the stream written to ```deletions.csv```.

2. ```python process_deletions.py```
Considers tweet IDs in ```deletions.csv```, removes from daily files and adjusts counters.

3. ```python get_top_tweets.py```
Looks through daily files produced as output from ```process_ds_files.py``` and counts tweets from last n days. Writes out IDs to file for embedding.

4. ```query_dump.ipynb```
Reads in serialised data from ```counters.dat``` and produces plots interatively

5. ```make_plots.py```
Reads in serialised data from ```counters.dat``` and produces plots and data files for web pages

#Usage
* Location of data specified in ```dataDirectory``` and set with ```-d``` flag. If several corpora exist (typically corresponding to different languages) in directories ```data_dir/corp1``` and ```data/corp2``` run with ```python process_ds_files.py -d data_dir/```.  All files of macthing ```DataSift*json``` within these directories and its subdirectories will be considered. This produces all output files in ```corp1``` and ```corp2```.

* To restrict to content geo-located to a particular country only; pass in 2 letter ISO country code ```python process_ds_files.py -C UK```

* To produce a map of content snapped to particular cities for use in DC.js dashboard supply a list of cities ```python process_ds_files.py -c cities.csv```

#Output Files
```process_ds_files.py``` produces a set of files for each corpora (each directory within ```dataDirectory```)

1. Set of daily files of form ```YYYY_MM_DD.json```, holds all messages from that day
2. Pickle file holding all counters and time series ```counters.dat```
3. Input file to CartoDB ```carto.txt```
4. Input file to DC.js ```dc.csv```
5. Deletions file, all streaming messages later deleted ```deletions.csv```

#Dependencies
* [Pandas](http://pandas.pydata.org/)
* [Gensim](http://radimrehurek.com/gensim/)
* [UNGP geolocation](https://github.com/UNGlobalPulse/PLNY)
* [UNGP gender](https://github.com/UNGlobalPulse/PLNY)
* [NLTK](http://www.nltk.org/)
* [Langid](https://github.com/saffsd/langid.py)
* [GeoPy](https://pypi.python.org/pypi/geopy/1.3.0)

#TODOs

* Find a more consistent way to count over topics (and sub topics)

