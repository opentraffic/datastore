# Open Traffic Datastore

Open Traffic Datastore is part of the [OTv2 platform](https://github.com/opentraffic/otv2-platform). It takes the place of OTv1's Data Pool.

The Datastore ingests input from distributed [Reporter](https://github.com/opentraffic/reporter) instances, creating internal "histogram tile files." The Datastore is also used to create public data extracts from the histogram tile files.

Datastore jobs can be run within Docker containers on [AWS Batch](https://aws.amazon.com/batch/), with Reporter inputs, histogram tile files, and public data extracts all stored on [Amazon S3](https://aws.amazon.com/s3/). Or run the jobs and store the files on your own choice of infrastructure.

[Analyst UI](https://github.com/opentraffic/analyst-ui), an app that runs in a web browser, can be used to fetch and parse Datastore's public data extracts.

## How to use

Read more about:

- [Datastore's series of batch jobs](docs/data_generation.md)
- [public data extract formats](docs/public_data_extracts.md)
- [GeoJSON coverage map format](docs/coverage_map.md)

To parse and visualize public data extracts:

- Use the simple JavaScript web app contained in `./speed-tile-visualizer`
- Use the full [Analyst UI](https://github.com/opentraffic/analyst-ui)

## How to develop

### Docker build

This repository and resultant Docker images are built and published via CircleCI. If you need to build
locally:

    sudo docker build -t opentraffic/datastore .

### Using the image

The image is available via DockerHub:

    docker pull opentraffic/datastore:latest

### Building with Maven

    mvn clean package