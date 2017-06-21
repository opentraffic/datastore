FROM python:2.7.13
MAINTAINER Grant Heffernan <grant@mapzen.com>

# env
ENV DEBIAN_FRONTEND noninteractive

# install dependencies
RUN apt-get update && apt-get install -y openjdk-7-jre
RUN pip install boto3 argparse

RUN mkdir /work
WORKDIR /work

# install code
ADD ./scripts /scripts

# cleanup
RUN apt-get clean && \
      rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
