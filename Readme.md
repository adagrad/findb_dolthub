

### Integration to dolthub

dolthub is ... 
you can get deltas of the data as easy as `dolt pull`

Every table needs to match the plugin command names
like the command `python main.py yfinance symbol` can only load data into the table `yfinance_symbol`


### Docker Image and github registry for github actions
```bash
docker build -t finget .
# provide a developer setting access token with everything granted under packages as password
docker login ghcr.io -u adagrad
docker images | grep finget
docker tag <image> ghcr.io/adagrad/finget:latest  
docker push ghcr.io/adagrad/finget:latest
```