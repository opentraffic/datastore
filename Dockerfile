FROM ubuntu:17.04
MAINTAINER Grant Heffernan <grant@mapzen.com>

# env
ENV DEBIAN_FRONTEND noninteractive

# install dependencies
RUN apt-get update && apt-get install -y default-jdk python3 python3-pip maven
RUN pip3 install --upgrade pip
RUN pip3 install boto3 argparse

# install scripts
ADD ./scripts /scripts

# install java code
ADD ./src /datastore/src
ADD ./pom.xml /datastore/pom.xml

# compile java
RUN cd /datastore && mvn install 2>&1 1>/dev/null && mvn clean package
RUN ln -s /datastore/target/datastore-histogram-tile-writer \
      /usr/local/bin/datastore-histogram-tile-writer

# create output dir and set working dir
RUN mkdir /output
RUN mkdir /work
WORKDIR /work

# cleanup
RUN apt-get clean && \
      rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
