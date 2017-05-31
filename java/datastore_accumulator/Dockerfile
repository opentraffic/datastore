FROM ubuntu:16.04
MAINTAINER Grant Heffernan <grant@mapzen.com>

# env
ENV DEBIAN_FRONTEND noninteractive

ENV KAFKA_HOST ${KAFKA_HOST:-"0.0.0.0"}
ENV KAFKA_PORT ${KAFKA_PORT:-"9092"}
ENV FRONTEND_TOPIC ${FRONTEND_TOPIC:-"otdatastore-frontend"}
ENV BACKEND_TOPIC ${BACKEND_TOPIC:-"otdatastore-backend"}

# install dependencies
RUN apt-get update && apt-get install -y openjdk-8-jdk-headless

# install code
ADD . /datastore
RUN cd /datastore && ./gradlew fatJar

# cleanup
RUN apt-get clean && \
      rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# start the datastore service
CMD java -cp /datastore/build/libs/datastore_accumulator-all-1.0-SNAPSHOT.jar io.opentraffic.datastore.Accumulator -b ${KAFKA_HOST}:${KAFKA_PORT} -i ${FRONTEND_TOPIC} -i ${BACKEND_TOPIC}
