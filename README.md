# Open Traffic Datastore

Open Traffic Datastore is part of OTv2, the new Open Traffic platform under development. It will take the place of OTv1's Data Pool.

The Datastore ingests input from distributed [Reporter](https://github.com/opentraffic/reporter) instances. Its contents can be queried using the [Open Traffic API](https://github.com/opentraffic/api). The Datastore is also used to created processed data products.

The java component here is intended to be run whenever the reporter uploads a CSV file containing segment travel data to an S3 bucket. This program then processes it to create output files in FlatBuffers and ORC in an output S3 bucket or buckets.

#### Docker build

    sudo docker build -t opentraffic/datastore .

#### Docker Compose

    PGDATA=. DATAPATH=. sudo -E docker-compose up

#### Building with Maven

    mvn clean package
