FROM ubuntu:16.04
MAINTAINER Grant Heffernan <grant@mapzen.com>

# env
ENV DEBIAN_FRONTEND noninteractive

# install dependencies
RUN apt-get update && apt-get install -y default-jdk python python-pip maven
RUN pip install --upgrade pip
RUN pip install boto3 argparse

# install scripts
ADD ./scripts /scripts

# install java code
ADD ./src /datastore/src
ADD ./pom.xml /datastore/pom.xml

# compile java
RUN cd /datastore && mvn install 2>&1 1>/dev/null && mvn clean package
RUN ln -s /datastore/target/datastore-histogram-tile-writer \
      /usr/local/bin/datastore-histogram-tile-writer

# set working dir
RUN mkdir /work
WORKDIR /work

# cleanup
RUN apt-get clean && \
      rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
