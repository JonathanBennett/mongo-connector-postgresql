FROM python:3.6

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6
RUN echo "deb http://repo.mongodb.org/apt/debian jessie/mongodb-org/3.4 main" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list
RUN echo "deb http://ftp.debian.org/debian jessie-backports main" >> /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y mongodb-org
RUN apt-get install -y vim

RUN mkdir -p /data/db

COPY requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt
RUN pip install sqlalchemy_utils
COPY . /tmp/
RUN pip install /tmp/
