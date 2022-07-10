# docker build . -t finget
FROM python:3.8
LABEL org.opencontainers.image.source=https://github.com/adagrad/findb_dolthub

RUN apt-get update && apt-get install -y tor

ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash

COPY entrypoint.sh /entrypoint.sh
COPY entrypoint_merge.sh /entrypoint_merge.sh
COPY torrc.default /etc/tor/torrc.default
ADD pull /opt/

EXPOSE 9050
EXPOSE 9051

ENV PATH="/opt/:${PATH}"

WORKDIR /opt/
ENTRYPOINT ["/entrypoint.sh"]
