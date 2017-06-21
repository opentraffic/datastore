FROM ubuntu:16.04
MAINTAINER Grant Heffernan <grant@mapzen.com>

# env
ENV DEBIAN_FRONTEND noninteractive

# install dependencies
RUN apt-get update && apt-get install -y default-jdk python python-pip maven
RUN pip install --upgrade pip
RUN pip install boto3 argparse

# install code
ADD ./scripts /scripts
ADD ./pom.xml /pom.xml
ADD ./src /src

# compile java
RUN cd / && mvn install

# set working dir
RUN mkdir /work
WORKDIR /work

# cleanup
RUN apt-get clean && \
      rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
