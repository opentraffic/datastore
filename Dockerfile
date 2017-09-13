FROM ubuntu:16.04
MAINTAINER Grant Heffernan <grant@mapzen.com>

# env
ENV DEBIAN_FRONTEND noninteractive

# install dependencies
RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:valhalla-core/opentraffic
RUN apt-get update && apt-get install -y default-jdk python python-pip python3 python3-pip maven protobuf-compiler python-protobuf flatbuffers
RUN pip install --upgrade pip
RUN pip install boto3 argparse flatbuffers
RUN pip3 install --upgrade pip
RUN pip3 install boto3 argparse flatbuffers

# install scripts
ADD ./scripts /scripts

# install proto and flatbuffer definitions
ADD ./proto/speedtile.proto /proto/
ADD ./osmlr-tile-spec/tile.proto /proto/
ADD ./osmlr-tile-spec/segment.proto /proto/
ADD ./src/main/fbs/histogram-tile.fbs /flatbuffer/schema.fbs

# install java code
ADD ./src /datastore/src
ADD ./pom.xml /datastore/pom.xml

# compile java
RUN cd /datastore && mvn install 2>&1 1>/dev/null && mvn clean package
RUN ln -s /datastore/target/datastore-histogram-tile-writer \
      /usr/local/bin/datastore-histogram-tile-writer

# generate python
RUN protoc --python_out /scripts --proto_path /proto /proto/*.proto
RUN sed -ie "s/namespace.*/namespace dsfb;/g" /flatbuffer/schema.fbs && flatc --python -o /scripts /flatbuffer/schema.fbs

# create output dir and set working dir
RUN mkdir /output
RUN mkdir /work
WORKDIR /work

# cleanup
RUN apt-get clean && \
      rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
