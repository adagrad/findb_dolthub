FROM python:3.8

ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash

RUN dolt config --global --add user.email adagrad@protonmail.com
RUN dolt config --global --add user.name "adagrad"

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
