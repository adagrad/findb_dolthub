
### run docker container
```bash
docker build -t finget .
docker run --env-file .env -v`pwd`:/data -w /data -it finget <command> <args>
# i.e docker run --env-file .env -v /tmp/foo/:/data -w /data -it finget yfinance symbol "--time 300 --dolt-load"
# i.e docker run --env-file .env -v /tmp/foo/:/data -w /data -it finget yfinance quote "-w exchange='NYQ' --time 300 --dolt-load"
# i.e docker run --env-file .env -v /tmp/foo/:/data -w /data -it --entrypoint /entrypoint_merge.sh finget "yfinance/quote/ffcab0f2b210e9c135ee"
```

### Integration to dolthub

dolthub is ... 
you can get deltas of the data as easy as `dolt pull`

Every table needs to match the plugin command names
like the command `python main.py yfinance symbol` can only load data into the table `yfinance_symbol`

## Development
### GitHub Action
#### Docker Image and GHCR (GitHub Container Registry) for GitHub Actions
```bash
docker build -t finget .
# provide a developer setting access token with everything granted under packages as password
docker login ghcr.io -u adagrad
docker images | grep finget
docker tag <image> ghcr.io/adagrad/finget:latest  
docker push ghcr.io/adagrad/finget:latest
```

Make sure after changes to the github actions relevant files and containers to 
also tag the sources with the `v1` tag: 

```bash
git tag v1 -f && git push --tags -f
```