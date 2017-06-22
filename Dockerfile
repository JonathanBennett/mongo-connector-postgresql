FROM python:3.6

RUN echo "deb http://repo.mongodb.org/apt/debian jessie/mongodb-org/3.4 main" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list
RUN apt-get update
RUN apt-get install -yf --allow-unauthenticated --fix-missing mongodb-org
RUN apt-get install -y vim
RUN mkdir /data
RUN mkdir /data/db

RUN pip install urllib3==1.10.4 && \
    pip install requests==2.7.0 && \
    pip install mongo-connector==2.5 && \
    pip install python-dateutil && \
    pip install psycopg2 && \
    pip install sqlalchemy_utils && \
    pip install git+https://github.com/pajachiet/mongo-connector-postgresql.git

