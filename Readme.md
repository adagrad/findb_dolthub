# Financial Research Data Base


The purpose of this database is to __enable research__. 

There is not much free available financial to support a research driven project. On the other hand a lot of APIs exist 
to obtain information of any individual asset. What is clearly missing is a broader access to the data in contrast to 
very specific API calls. To perform a research task the tool of choice usually is SQL. However, since a lot of 
researchers will use python, pandas and jupyterlab there is also an 
[API server available on github](https://github.com/adagrad/findb_dolthub) which directly streams pandas DataFrames 
as simple as this: 

```python
import pandas as pd
pd.read_parquet("http://127.0.0.1:9876/api/ohlcv/yfinance/MSFT,AAPL?__axis=0&__as=parquet")
```

FinDB should enable individuals todo research without having to build their own individual database. Because this is not 
only tedious but also hits the same servers unnecessarily often for the same data. Save the planet! This is an attempt 
to introduce an open and crowed maintained database which can be used to do research across different assets classes, 
countries and sectors. In case you miss some specific data please consider contributing to this project. Make sure every
contribution includes a GitHub action as well to keep the data updated (see *Data Lifecycle and Frequency*).

Now, if you really only need the go ahead and just [install dolt](https://docs.dolthub.com/introduction/installation)
and clone the dolt database form the url: 
[dolt clone adagrad/findb](https://www.dolthub.com/repositories/adagrad/findb).

If you also want to use the API, the simplest would be to just use the hosted 
[docker image](https://github.com/adagrad/findb_dolthub/pkgs/container/finget) from the github container registry.


## Project State
The project is pre-alpha and developed on [github.com/adagrad/findb_dolthub](https://github.com/adagrad/findb_dolthub)
and [dolthub.com/adagrad/findb](https://www.dolthub.com/repositories/adagrad/findb). 

Any PRs are very welcome, is it new data sources or API extensions. Currently, you will be able to find a pretty 
big collection of tickers from exchange traded asses all over the world. Along with these ticker symbols goes along a 
collection of end of day open, high, low, close data. 


## Data Lifecycle and Frequency

The purpose of this database is explicitly not to provide most recent up-to-date information. The idea is to support 
various data sources and keep them updated. Open source, for free forever, no limitations! While the requirement is not
to be as up-to-date as possible but still closely enough up to date to train statistical models or perform some 
advanced machine learning.

Adding a new data source means you also have to add a GitHub action to keep the data updated while not abusing the task
executors! This way the database will be growing on a regular basis just with a rather low frequency like every month.  


## Contribution
Help with extending data sources and/or with maintaining existing sources and jobs is highly appreciated!
Contribute like you do with any other GitHub project by forking, and the submission of pull requests. Just make sure 
you follow the pattern. 

## Credits
Inspired by [JerBouma/FinanceDatabase](https://github.com/JerBouma/FinanceDatabase/) but using GitHub actions to keep
data updated and use an SQL compliant database. No api client needed, just do SQL. 

A customized version of [Benny-/Yahoo-ticker-symbol-downloader](https://github.com/Benny-/Yahoo-ticker-symbol-downloader) 
is used to get the list of "all" symbols. [ranaroussi/yfinance](https://github.com/ranaroussi/yfinance) is used for the
sector and industry information. And a lot of custom and direct _requests_ calls.

## Fetch Data Locally

While as already pointed out it is not recommended to hit the servers of various sources more often as actually needed (
hence use the already existing data). Here are some commands to execute and test the commands locally.

### Run docker container with commands
```bash
docker build -t finget .
docker run --env-file .env -v`pwd`:/data -w /data -it finget <command> <args>
# i.e docker run --env-file .env -v /tmp/foo/:/data -w /data -it finget yfinance symbol "--time 300 --dolt-load"
# i.e sudo rm -rf /tmp/foo/* && docker run --env-file .env -v /tmp/foo/:/data -w /data -it finget yfinance quote "-w exchange='NYQ' --time 300 --dolt-load"
# i.e sudo rm -rf /tmp/foo/* && docker run --env-file .env -v /tmp/foo/:/data -w /data -it --entrypoint /entrypoint_merge.sh finget "yfinance/quote/ffcab0f2b210e9c135ee"
```

### Integration to dolthub

The idea is to fetch as few data as possible over wire and just pull/push the deltas. This is exactly what git does 
and dolthub wants to bring this paradigm to data as well. The project is still very early but the idea behind it is 
really promising and should be supported! Updating your local database is as easy as `git pull` which just becomes 
`dolt pull`.

### Use a locally synced database
By default you do not need to have a full database synced locally but just query the rest endpoint of the hosted 
version on Dolthub. However, you of course can also use a locally hosted database by passing the command line argument
`--repo-database "mysql+pymysql://root:@localhost/findb/main"`.

## Development

### Adding Data
todo document adding data
Every table needs to match the plugin command names
like the command `python main.py yfinance symbol` can only load data into the table `yfinance_symbol`

### Add GitHub Action
#### Docker Image and GHCR (GitHub Container Registry) for GitHub Actions
```bash
docker build -t finget .
# provide a developer setting access token with everything granted under packages as password
docker login ghcr.io -u adagrad
docker images | grep finget
docker tag <image> ghcr.io/adagrad/finget:latest  
docker push ghcr.io/adagrad/finget:latest
```

