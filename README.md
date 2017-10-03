# Open Traffic Datastore

Open Traffic Datastore is part of OTv2, the new Open Traffic platform under development. It will take the place of OTv1's Data Pool.

The Datastore ingests input from distributed [Reporter](https://github.com/opentraffic/reporter) instances. The Datastore is also used to create processed data products.

The java component here is intended to be run whenever the reporter uploads a CSV file containing segment travel data to an S3 bucket. This program then processes it to create output files in FlatBuffers and ORC in an output S3 bucket or buckets.

#### Docker build

This repository and resultant Docker images are built and published via CircleCI. If you need to build
locally:

    sudo docker build -t opentraffic/datastore .

#### Using the image

The image is available via DockerHub: `docker pull opentraffic/datastore:latest`.

#### Building with Maven

    mvn clean package
    
#### Public Data Extracts

Documentation for the Public Data Extract tiles can be found [here](./public_data_extracts.md)

#### Coverage Map

Documentation for the Coverage Map can be found [here](./coverage_map.md)