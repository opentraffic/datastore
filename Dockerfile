FROM ubuntu:16.04
MAINTAINER Grant Heffernan <grant@mapzen.com>

# env
ENV DEBIAN_FRONTEND noninteractive

ENV STORE_BIND_ADDR ${STORE_BIND_ADDR:-"0.0.0.0"}
ENV STORE_LISTEN_PORT ${STORE_LISTEN_PORT:-"8003"}
ENV KAFKA_HOST ${KAFKA_HOST:-"0.0.0.0"}
ENV KAFKA_PORT ${KAFKA_PORT:-"9092"}
ENV FRONTEND_TOPIC ${FRONTEND_TOPIC:-"otdatastore-frontend"}

# install dependencies
RUN apt-get update && apt-get install -y python python-pip
RUN pip install kafka-python

# install code
ADD ./py /datastore
RUN cd /datastore/frontend && python setup.py install

# cleanup
RUN apt-get clean && \
      rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

EXPOSE ${STORE_LISTEN_PORT}

# start the datastore service
CMD python -u /usr/local/bin/opentraffic-datastore-frontend -l ${STORE_BIND_ADDR}:${STORE_LISTEN_PORT} -b ${KAFKA_HOST}:${KAFKA_PORT} -t ${FRONTEND_TOPIC}
