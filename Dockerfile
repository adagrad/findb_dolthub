# docker build . -t finget
FROM python:3.8
LABEL org.opencontainers.image.source=https://github.com/adagrad/findb_dolthub

ENV PATH="/opt/:${PATH}"

RUN apt-get update && apt-get install -y tor
COPY torrc.default /etc/tor/torrc.default

ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN apt-get install -y default-mysql-client

COPY fin.meta.db.sqlite /
COPY yfsymbols.csv* /
COPY entrypoint*.sh /
ADD app /opt/

EXPOSE 9050
EXPOSE 9051
EXPOSE 3306

WORKDIR /data/

ENTRYPOINT ["/entrypoint.sh"]
