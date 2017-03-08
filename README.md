# Open Traffic Datastore

Open Traffic Datastore is part of OTv2, the new Open Traffic platform under development. It will take the place of OTv1's Data Pool.

The Datastore ingests input from distributed [Reporter](https://github.com/opentraffic/reporter) instances. Its contents can be queried using the [Open Traffic API](https://github.com/opentraffic/api). The Datastore is also used to created processed data products.

#### Docker build

    sudo docker build -t opentraffic/datastore .

#### Docker Compose

    PGDATA=. DATAPATH=. sudo -E docker-compose up

