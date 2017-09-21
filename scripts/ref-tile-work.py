#!/usr/bin/env python
import argparse
import math
import os
import sys
import boto3
import gzip
import logging
import datetime
import time
import StringIO
from Queue import Queue
from threading import Thread

log = logging.getLogger('make_ref_speeds')
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s'))
log.addHandler(handler)

try:
  import speedtile_pb2
except ImportError:
  print 'You need to generate protobuffer source via: protoc --python_out . --proto_path ../proto ../proto/*.proto'
  sys.exit(1)

valhalla_tiles = [{'level': 2, 'size': 0.25}, {'level': 1, 'size': 1.0}, {'level': 0, 'size': 4.0}]

def url_suffix(tile_level, tile_index):
  tile_set = filter(lambda x: x['level'] == tile_level, valhalla_tiles)[0]
  max_index = int(360.0/tile_set['size'] * 180.0/tile_set['size']) - 1
  num_dirs = int(math.ceil(len(str(max_index)) / 3.0))
  suffix = list(str(tile_index).zfill(3 * num_dirs))
  for i in range(0, num_dirs):
    suffix.insert(i * 3 + i, '/')
  suffix = '/' + str(tile_level) + ''.join(suffix) 
  return suffix

def get_subtile_count(filename):
  spdtile = speedtile_pb2.SpeedTile()
  with gzip.open(filename, 'rb') as f:
    spdtile.ParseFromString(f.read())
  count = int(math.ceil(spdtile.subtiles[0].totalSegments / float(spdtile.subtiles[0].subtileSegments)))
  del spdtile
  return count

class Worker(Thread):
  def __init__(self, tasks, results, adder):
    Thread.__init__(self)
    self.tasks = tasks
    self.results = results
    self.adder = adder
    self.daemon = True
    self.start()
  def run(self):
    while True:
      func, args, kargs = self.tasks.get()
      kargs['adder'] = self.adder
      try: 
        result = func(*args, **kargs)
        if result is not None:
          self.results.put(result)
      except Exception as e: log.error(e)
      self.tasks.task_done()

class ThreadPool:
  def __init__(self, num_threads):
    self.tasks = Queue(0)
    self.results = Queue(0)
    for _ in range(num_threads): Worker(self.tasks, self.results, self.add_task)
  def add_task(self, func, *args, **kargs):
    self.tasks.put((func, args, kargs))
  def wait_completion(self):
    self.tasks.join()
    return list(self.results.queue)

def work(speed_bucket, week_tile, part, **kargs):
  session = boto3.session.Session()
  client = session.client('s3')
  #make sure we have a spot for this
  key = week_tile % part
  try:
    os.makedirs('/'.join(key.split('/')[:-1]))
  except:
    pass
  #try to get it
  try:
    client.download_file(speed_bucket, key, key)
    log.info('Downloaded %s from %s' % (key, speed_bucket))
    #queue up more if there is more
    if part == 0:
      parts = get_subtile_count(key)
      for p in range(1, parts):
        kargs['adder'](work, speed_bucket, week_tile, p)
    return key
  except:
    pass

def download(tile_level, tile_index, end_week, weeks, speed_bucket):
  #TODO: check if the bucket is valid and if not pretend its a dir to scan for speed tiles
  log.info('Downloading speed information for the time range')
  pool = ThreadPool(10)
  end = datetime.datetime.strptime(end_week + '/1','%Y/%W/%w').date()
  suffix = url_suffix(tile_level, tile_index) + '.spd.%d.gz'
  for week in range(0, weeks):
    key = (end - datetime.timedelta(weeks=week)).strftime("%Y/%W") + suffix
    pool.add_task(work, speed_bucket, key, 0)
  return pool.wait_completion()

def createAvgSpeedList(fileNames):
  log.info('Getting speed information for time range')

  #TODO: when this is getting data over a range of time that includes an update to OSMLR definitions
  #this process will waste time computing values for segments that are marked deleted, this is probably
  #a good thing as it means that people using previous version of osmlr will still have reference speeds

  #each segment has its own list of speeds, we dont know how many segments for the avg speed list to start with
  segments = []
  segmentHours=[]
  minSeconds = None
  maxSeconds = None

  #need to loop thru all of the speed tiles for a given tile id
  for fileName in fileNames:
    log.info('Processing ' + fileName)
    spdtile = speedtile_pb2.SpeedTile()
    with gzip.open(fileName, 'rb') as f:
      spdtile.ParseFromString(f.read())
    subtileCount = 1;
    #we now need to retrieve all of the speeds for each segment in this tile
    for subtile in spdtile.subtiles:
      log.debug('>>>>> subtileCount per file=' + str(subtileCount))
      subtileCount += 1
      if minSeconds is None or minSeconds > subtile.rangeStart:
        minSeconds = subtile.rangeStart
      if maxSeconds is None or maxSeconds < subtile.rangeEnd:
        maxSeconds = subtile.rangeEnd

      #make sure that there are enough lists in the list for all segments
      missing = spdtile.subtiles[0].totalSegments - len(segments)
      log.debug('spdtile.subtiles[0].totalSegments=' + str(spdtile.subtiles[0].totalSegments) + ' | len(segments)=' + str(len(segments)) + ' | missing=' + str(missing))
      if missing > 0:
        segments.extend([ [] for i in range(0, missing) ])
        segmentHours.extend([ {} for i in range(0, missing) ])
      
      entries = subtile.unitSize / subtile.entrySize
      for i, speed in enumerate(subtile.speeds):
        segmentIndex = subtile.startSegmentIndex + int(math.floor(i/entries))
        hour = i % entries;
        log.debug('SPEED: i=' + str(i) + ' | hour=' + str(hour) + ' | segmentIndex=' + str(segmentIndex) + ' | segmentId=' + str((segmentIndex<<25)|(subtile.index<<3)|subtile.level) + ' | speed=' + str(speed))
        if speed > 0 and speed <= 160:
          segments[segmentIndex].append(speed)
          if hour in segmentHours[segmentIndex]:
            segmentHours[segmentIndex][hour]['total'] += speed
            segmentHours[segmentIndex][hour]['count'] += 1
          else:
            segmentHours[segmentIndex][hour] = {'total': speed, 'count': 1.0}
        elif speed > 160:
          log.error('INVALID SPEED: i=' + str(i) + ' | segmentIndex=' + str(segmentIndex) + ' | segmentId=' + str((segmentIndex<<25)|(subtile.index<<3)|subtile.level) + ' | speed=' + str(speed))

  #sort each list of speeds per segment
  for i, segment in enumerate(segments):
    segment.sort()
    log.debug('SORTED SPEEDS: segmentIndex=' + str(i) + ' | speeds=' + str(segment))

  return segments, segmentHours, minSeconds, maxSeconds

def createRefSpeedTile(speedListPerSegment, speedListPerHourPerSegment, tile_level, tile_index, minSeconds, maxSeconds):
  log.info('Creating reference speed tile')

  tile = speedtile_pb2.SpeedTile()
  st = tile.subtiles.add()
  st.level = tile_level
  st.index = tile_index
  st.startSegmentIndex = 0
  st.totalSegments = len(speedListPerSegment)
  st.subtileSegments = len(speedListPerSegment)
  #time stuff
  st.rangeStart = minSeconds
  st.rangeEnd = maxSeconds
  st.unitSize = 604800 #a weeks worth of data
  st.entrySize = 3600 #note that this only holds true for the average per hour speeds, the references speeds have no bearing on unit and entry size since they are one speed per segment
  st.description = 'Week reference speeds averaged over ' + time.strftime('%Y.%m.%d %H:%M:%S', time.gmtime(minSeconds)) + ' - ' + time.strftime('%Y.%m.%d %H:%M:%S', time.gmtime(maxSeconds))

  #bucketize avg speeds into 20%, 40%, 60% and 80% reference speed buckets
  #for each segment
  #for segment in speedListPerSegment:
  for segment in speedListPerSegment:
    size = len(segment)
    st.referenceSpeeds20.append(segment[int(size * .2)] if size > 0 else 0) #0 here represents no data
    st.referenceSpeeds40.append(segment[int(size * .4)] if size > 0 else 0) #0 here represents no data
    st.referenceSpeeds60.append(segment[int(size * .6)] if size > 0 else 0) #0 here represents no data
    st.referenceSpeeds80.append(segment[int(size * .8)] if size > 0 else 0) #0 here represents no data

  # write out the average speeds for each hour for each segment
  for segment in speedListPerHourPerSegment:
    for hour in range(0,168):
      st.speeds.append(int(round(segment[hour]['total'] / segment[hour]['count'])) if hour in segment else 0)
  return tile

def writeTile(tile, bucket):
  #compress
  log.info('Compressing reference speed tile')
  zipped = StringIO.StringIO()
  f_out = gzip.GzipFile(mode='wb',fileobj=zipped)
  f_out.write(tile.SerializeToString())

  #maybe local maybe s3
  key = (url_suffix(tile.subtiles[0].level, tile.subtiles[0].index) + '.ref.gz').strip('/')
  if bucket is not None:
    #push up with custom metatags.  All user custom meta data can only be string and will have a prefix of x-amz-meta-
    log.info('Uploading reference speed tile to %s as %s' % (bucket, key))
    client = boto3.client("s3")
    client.put_object(ACL='public-read', Bucket=bucket, ContentType='application/octet-stream', ContentEncoding='gzip', Body=zipped.getvalue(), Key=key,
      Metadata={'rangeStart':str(tile.subtiles[0].rangeStart), 'rangeEnd':str(tile.subtiles[0].rangeEnd)})
  else:
    fileName = (key).split('/')[-1]
    log.info('Writing reference speed tile to %s' % fileName)
    with open(fileName, 'wb') as f:
      f.write(zipped.getvalue())

# Read in protobuf files from the datastore output in AWS to read in the lengths, speeds & next segment ids and generate the segment speed files in proto output format
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Generate ref speed tiles', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--speed-bucket', type=str, help='AWS bucket location (i.e., where to get the speed tiles), if its not a valid bucket it will be treated as a local path', required=True)
  parser.add_argument('--ref-speed-bucket', type=str, help='AWS Bucket (e.g., ref-speedtiles-prod) into which we will place the ref tile')
  parser.add_argument('--end-week', type=str, help='The last week you want to use', required=True)
  parser.add_argument('--weeks', type=int, help='How many weeks up to and including this end week to make use of', default=52)
  parser.add_argument('--tile-level', type=int, help='The level to target', required=True)
  parser.add_argument('--tile-index', type=int, help='The tile id to target', required=True)
  parser.add_argument('--verbose', '-v', help='Turn on verbose output i.e. DEBUG level logging', action='store_true')

  # parse the arguments
  args = parser.parse_args()

  if log.level == logging.NOTSET:
    log.setLevel(logging.DEBUG if args.verbose else logging.WARN)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s'))
    log.addHandler(handler)

  if args.verbose:
    log.debug('speed-bucket ' + args.speed_bucket)
    log.debug('end-week ' + args.end_week)
    log.debug('weeks ' + args.weeks)
    log.debug('tile-level=' + str(args.tile_level))
    log.debug('tile-index=' + str(args.tile_index))

  #get the data
  fileNames = download(args.tile_level, args.tile_index, args.end_week, args.weeks, args.speed_bucket)
  if not fileNames:
    log.info('No data was found')
    sys.exit(0)
  
  #read the data
  speedListPerSegment, speedListPerHourPerSegment, minSeconds, maxSeconds = createAvgSpeedList(fileNames)

  #turn the data into a tile
  tile = createRefSpeedTile(speedListPerSegment, speedListPerHourPerSegment, args.tile_level, args.tile_index, minSeconds, maxSeconds)

  #store the ref tile
  writeTile(tile, args.ref_speed_bucket)
