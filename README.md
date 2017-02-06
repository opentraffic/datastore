# Open Traffic Datastore

Open Traffic Datastore is part of OTv2, the new Open Traffic platform under development. It will take the place of OTv1's Data Pool and the back-end API portions of the [Traffic Engine Application](https://github.com/opentraffic/traffic-engine-app).

The Datastore ingests input from distributed [Reporter](https://github.com/opentraffic/reporter) instances, powers an API for querying and visualization, and creates processed data products.

#### Docker build

    sudo docker build -t opentraffic/datastore .

#### Docker Compose

    PGDATA=. DATAPATH=. sudo -E docker-compose up

