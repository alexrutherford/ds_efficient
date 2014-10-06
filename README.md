Set of python scripts to parse large DataSift streaming corpora

Run in following order, with data by language in ```../data/<language>/``` set in ```dataDirectory```

1. ```python process_ds_files.py```
Cycles through all files from DataSift ```DataSift*json``` and places tweets in daily files. Also counts mentions, hashtags and other attributes. Serialises data in ```counters.dat```. Deleted tweets that appear in the stream written to ```deletions.csv```.

2. ```python process_deletions.py```
Considers tweet IDs in ```deletions.csv```, removes from daily files and adjusts counters.

#Dependencies
* Pandas
* Gensim
* UNGP geolocation
* UNGP gender
* NLTK
