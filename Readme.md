

### Integration to dolthub

dolthub is ... 
you can get deltas of the data as easy as `dolt pull`

Every table needs to match the plugin command names
like the command `python main.py yfinance symbol` can only load data into the table `yfinance_symbol`

## Development
### GitHub Action
#### Docker Image and GHCR (GitHub Conteiner Registry) for GitHub Actions
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