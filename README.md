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
Reads in serialised data from ```counters.dat``` and produces plots and data files for web pages. Pass in directory with pickle file with ```-d <dataDirectory>```

#Usage
* Location of data specified by ```dataDirectory``` as a compulsory argument to ```process_ds_files.py```. If several corpora exist (typically corresponding to different languages) then invoke separately for each one (only overhead for running multiple times is parsing of geolocation world pickle file).  All directories matching ```20[0-9]{2,2}-[0-9]{2,2}``` will be examined and all files within matching ```DataSift*json``` will be considered. This produces all output files in ```dataDirectory```.

* To restrict to content geo-located to particular countries only; pass in a list of 2 letter ISO country code ```python process_ds_files.py -C GB ID FR```

* To produce a map of content snapped to particular cities for use in DC.js dashboard supply a list of cities ```python process_ds_files.py -c cities.csv```

* To clean existing output files pass ```--clean``` flag.

#Output Files
```process_ds_files.py``` produces a set of files for each corpora (in ```dataDirectory```)

1. Set of daily files of form ```YYYY_MM_DD[_languages][_countries].json```, holds all messages from that day
2. Pickle file holding all counters and time series ```counters[_languages][_countries].dat```
3. Input file to CartoDB ```carto[_languages][_countries].txt```
4. Input file to DC.js ```dc[_languages][_countries].csv```
5. Deletions file, all streaming messages later deleted ```deletions[_languages][_countries].csv``` (only non-empty for streaming data, not historical queries)

```make_plots.py``` produces all input plots for dashboard in png/mlpd3 format

```get_top_tweets.py``` reads daily files from last N days and produces list of top tweets by ID ready for embedding.

#Example Usage
```python process_ds_files.py data/ -C BR PT -c cities.csv```
```python process_deletions.py -d data/ -C BR -L pt```
```python make_plots.py -d data/ -C BR --clean ```
```python get_top_tweets.py -d data -n 7 -C BR --clean```

#Dependencies
* [Pandas](http://pandas.pydata.org/)
* [Gensim](http://radimrehurek.com/gensim/)
* [UNGP geolocation](https://github.com/UNGlobalPulse/PLNY)
* [UNGP gender](https://github.com/UNGlobalPulse/PLNY)
* [NLTK](http://www.nltk.org/)
* [Langid](https://github.com/saffsd/langid.py)
* [GeoPy](https://pypi.python.org/pypi/geopy/1.3.0)
* [Matplotlib](http://matplotlib.org/)
* [MplD3](https://pypi.python.org/pypi/mpld3/0.2)

#TODOs

* Find a more consistent way to count over topics (and sub topics)
* Add in gender time series for each topic
* Count ngram time series in a separate process
* Convert topic counting (amd resampling) to using temp Series not DataFrame
* Convert topic collocations to time series rather than counts
