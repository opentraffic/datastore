# Overview: Tile Creation

There are a series of scripts we use to generate all of the internal and external representations of the open traffic data. Most of these data are tiles with the added dimension of time, with the one exception being the [coverage map](coverage_map.md) which is an all encompassing single GeoJSON file. The creation of this data can be most easily described as a top down topology. At each stage of the topology the processing will take multiple upstream tiles and turn them into a downstream tile. Note: these processes currently rely on access to the AWS APIs for things like storage and scheduling of jobs. Currently the pipeline is setup to handle only the `AUTO` mode of transport. If we wanted to handle other modes of transport we would need to separate the datastreams with respect to the buckets that contain the various pieces at each stage of the pipeline. Concretely, if we wanted at a motorbike dataset, we'd need to make reporter, histogram, speed tile and reference tile buckets. These buckets are accessed by the various stages described in the following sections.

## Stage 1: Histogram Generation

We have a script called `submit-work-service.py` which is run continuously on an EC2 instance. It wakes up every so often and schedules work in batch to turn files from the reporter into flatbuffer histograms. Basically it goes to the s3 bucket where the reporters are dropping data. It lists all the s3 prefixes (time tile) it sees and schedules a job for each of those in AWS Batch. Each job is for a single tile and single hour (out of all hours since the beginning of the unix epoch).

The usage for `submit-work-service.py` is controlled by environment variables, the following is an example using all environment variables:
```
    DATASTORE_ENV=dev SLEEP_BETWEEN_RUNS=120 ./submit-work-service.py
```
 As the Batch service picks up those jobs and schedules them they complete each job by running the script `work.py`. This script picks up any existing histogram for this time tile and all the reporter outputs at that s3 prefix and runs the java flatbuffer generation with those as inputs. The final output is a flatbuffer which is pushed back up to s3.

The usage for `work.py` is controlled via arguments to the program as you can see by passing `--help`:
```
    usage: work.py [-h] [--s3-reporter-bucket S3_REPORTER_BUCKET]
                   [--s3-datastore-bucket S3_DATASTORE_BUCKET]
                   [--s3-reporter-prefix S3_REPORTER_PREFIX]

    optional arguments:
      -h, --help            show this help message and exit
      --s3-reporter-bucket S3_REPORTER_BUCKET
                            Bucket (e.g. reporter-work-prod) in which the data we
                            wish to process is located
      --s3-datastore-bucket S3_DATASTORE_BUCKET
                            Bucket (e.g. datastore-output-prod) into which we will
                            place transformed data
      --s3-reporter-prefix S3_REPORTER_PREFIX
                            S3 prefix under which tiles will be found, should look
                            like epochsecond_epochsecond/level/tile_index
```
These hourly histogram tiles are the basis for the next step. Histogram tile generation is not idempotent because the inputs are deleted as they are received.

## Stage 2: Speed Tile Generation

We generate speed tiles in a similar manner as the histograms. Basically we run a script called `submit-speed-tile-work-service.py` weekly that will at the end of each week submit a bunch of speed tile jobs into the AWS Batch service. Each job targets a given week of the year. The determination of which week gets submitted is currently set to look at what is in the speed tile data and what is in the histogram data. It figures out which week of speed tiles is the latest available and if the histogram data has coverage for the week after that it submits jobs for this week. If not it exits. You can also run this script and specify the target week directly if new historical data needs to be folded in (ie the histograms have taken updates since the last run of speed tile generation for a given week).

The usage of `submit-speed-tile-work-service.py` is controlled by environment variables, the following is an example using all environment variables:
```
    DATASTORE_ENV=dev TARGET_BBOX=-21,86,39,162 TARGET_WEEK=2017/12 TARGET_LEVEL=1 ./submit-speed-tile-work-service.py
```
The speed tile job submission makes a job for each 4 degree (level 0) tile and the speed tile batch workers (which run `speed-tile-work.py`) will do the level 0 tile and the 16 level 1 tiles underneath it. Each speed tile contains a weeks worth of hourly data which it fetches from the histogram s3 bucket. When the week is finished and the speed tiles are generated they are pushed up to s3 and a job is scheduled in batch to recompute the reference tiles for the year of data up to and including the week of the newly generated speed tile. If you were making speed tile for a week we already had coverage or a week in the past the reference tile will be regenerated if it includes this new data occurred within the past year (this is a TODO).

For more about the speed tile contents, see [this doc](public_data_extracts.md).

The usage for `speed-tile-work.py` is controlled via arguments to the program as you can see by passing `--help`:
```
    usage: speed-tile-work.py [-h] --environment ENVIRONMENT --tile-level
                              TILE_LEVEL --tile-index TILE_INDEX --week WEEK
                              [--concurrency CONCURRENCY]
                              [--max-tile-level MAX_TILE_LEVEL]
    
    optional arguments:
      -h, --help            show this help message and exit
      --environment ENVIRONMENT
                            The environment prod or dev to use when computing
                            bucket names and batch job queues and definitions
      --tile-level TILE_LEVEL
                            The tile level used to get the input data from the
                            histogram bucket
      --tile-index TILE_INDEX
                            The tile index used to get the input data from the
                            histogram bucket
      --week WEEK           The week used to get the input data from the histogram
                            bucket
      --concurrency CONCURRENCY
                            The week used to get the input data from the histogram
                            bucket
      --max-tile-level MAX_TILE_LEVEL
                            The max tile level to generate speed tiles for. Must
                            be at least 0 and can go up to 2
      --osmlr-version OSMLR_VERSION
                            The version of osmlr to fetch when creating speed
                            tiles
```

Speed tile generation is idempotent, you may run it as many times as you like without affecting the veracity of the data.

## Stage 3: Reference Tile Generation

The reference tiles are generated at the end of each run of a given speed tile is finished. The AWS Batch worker uses `reference-tile-work.py` to compute the reference tile. It takes as input one or more weeks of speed tiles (for a given tile). By default we attempt to get speed tile data for the year (so 52 weeks). When finished the tile is pushed up to s3 with some meta tags specifying the range of data (min and max epoch UTC timestamp). This means that a reference tile has no set time range and that as we get updates the reference tiles will move ahead in time. These ranges will be used when coverage map generation happens.

For more about the reference tile contents, see [this doc](public_data_extracts.md#reference-speed-tiles).

The usage for `reference-tile-work.py` is controlled via arguments to the program as you can see by passing `--help`:

```
    usage: ref-tile-work.py [-h] [--environment ENVIRONMENT] --end-week END_WEEK
                            [--weeks WEEKS] --tile-level TILE_LEVEL --tile-index
                            TILE_INDEX [--verbose]
    
    Generate ref speed tiles
    
    optional arguments:
      -h, --help            show this help message and exit
      --environment ENVIRONMENT
                            The environment prod or dev to use when computing
                            bucket names and batch job queues and definitions
                            (default: None)
      --end-week END_WEEK   The last week you want to use (default: None)
      --weeks WEEKS         How many weeks up to and including this end week to
                            make use of (default: 52)
      --tile-level TILE_LEVEL
                            The level to target (default: None)
      --tile-index TILE_INDEX
                            The tile id to target (default: None)
      --verbose, -v         Turn on verbose output i.e. DEBUG level logging
                            (default: False)
```
## Stage 4: Coverage Map Generation

The coverage map is generated using `make_coverage_map.py` and takes as input all the reference tiles' metadata (the min and max time ranges) and computes a GeoJSON file as output. This lives in the same bucket as the reference tiles.

For more about the coverage map contents, see [this doc](coverage_map.md).

The usage for `make_coverage_map.py` is controlled via arguments to the program as you can see by passing `--help`:

```
    usage: make_coverage_map.py [-h] --ref-speed-bucket REF_SPEED_BUCKET
                                [--output-file OUTPUT_FILE] [--upload-results]
    
    Generate Coverage Map
    
    optional arguments:
      -h, --help            show this help message and exit
      --ref-speed-bucket REF_SPEED_BUCKET
                            AWS Ref Bucket location. (default: None)
      --output-file OUTPUT_FILE
                            geojson output file (default: None)
      --upload-results      Upload the results to the AWS Bucket (default: False)
```
