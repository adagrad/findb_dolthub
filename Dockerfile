# docker build . -t finget
FROM python:3.8

RUN apt-get update && apt-get install -y tor

ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash

RUN dolt config --global --add user.email adagrad@protonmail.com
RUN dolt config --global --add user.name "adagrad"

COPY entrypoint.sh /entrypoint.sh
ADD pull /opt/
COPY torrc.default /etc/tor/torrc.default

EXPOSE 9050
EXPOSE 9051

ENV PATH="/opt/:${PATH}"

WORKDIR /opt/
ENTRYPOINT ["/entrypoint.sh"]
