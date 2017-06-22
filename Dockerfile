FROM python:3.6

RUN echo "deb http://repo.mongodb.org/apt/debian jessie/mongodb-org/3.4 main" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list
RUN apt-get update
RUN apt-get install -yf --allow-unauthenticated --fix-missing mongodb-org
RUN apt-get install -y vim

RUN mkdir -p /data/db

COPY requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt
COPY . /tmp/
RUN pip install /tmp/

