#!/usr/bin/env python

'''
If you're running this from this directory you can start the server with the following command:
./datastore_service.py localhost:8003

sample url looks like this:
http://localhost:8003/store?json={"segments": [{"segment_id": 345678,"prev_segment_id": 356789,"start_time": 98765,"end_time": 98777,"length":555}, {"segment_id": 345780,"start_time": 98767,"end_time": 98779,"length":678}, {"segment_id": 345795,"prev_segment_id": 656784,"start_time": 98725,"end_time": 98778,"length":479}, {"segment_id": 545678,"prev_segment_id": 556789,"start_time": 98735,"end_time": 98747,"length":1234}],"provider": 123456,"mode": "auto"}
'''

import sys
import json
import multiprocessing
import threading
from Queue import Queue
import socket
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from cgi import urlparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import time

actions = set(['store'])

# this is where thread local storage lives
thread_local = threading.local()

#use a thread pool instead of just frittering off new threads for every request
class ThreadPoolMixIn(ThreadingMixIn):
  allow_reuse_address = True  # seems to fix socket.error on server restart

  def serve_forever(self):
    # set up the threadpool
    pool_size = int(os.environ.get('THREAD_POOL_MULTIPLIER', 1)) * multiprocessing.cpu_count()
    self.requests = Queue(pool_size)
    for x in range(pool_size):
      t = threading.Thread(target = self.process_request_thread)
      t.setDaemon(1)
      t.start()
    # server main loop
    while True:
      self.handle_request()
    self.server_close()

  def make_thread_locals(self):
    # connect to the db
    try:
      self.credentials = (os.environ['POSTGRES_DB'], os.environ['POSTGRES_USER'], os.environ['POSTGRES_HOST'], 
                          os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_PORT'])
      sql_conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'" % self.credentials)
      sql_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      setattr(thread_local, 'sql_conn', sql_conn)
      cursor = sql_conn.cursor()
    except Exception as e:
      raise Exception('Failed to connect to database: ' + repr(e))
    
    #show some info
    try:
        cursor.execute('select pg_backend_pid();')
        sys.stdout.write('init: %s(%d) connected has connection %s(%d)' % (threading.current_thread().getName(), id(threading.current_thread()), cursor.fetchone()[0], id(sql_conn)) + os.linesep)
        sys.stdout.flush()
    except Exception as e:
      raise Exception('Failed to identify database connection: ' + repr(e))

    # create prepared statement and see if its there
    try:
      prepare_statement = 'PREPARE report AS INSERT INTO segments (segment_id,prev_segment_id,mode,' \
                          'start_time,start_time_dow,start_time_hour,end_time,end_time_dow,' \
                          'end_time_hour,length,speed,provider) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12);'
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepared statement: ' + repr(e))

  def process_request_thread(self):
    self.make_thread_locals()
    while True:
      request, client_address = self.requests.get()
      ThreadingMixIn.process_request_thread(self, request, client_address)

  def handle_request(self):
    try:
      request, client_address = self.get_request()
    except socket.error:
      return
    if self.verify_request(request, client_address):
      self.requests.put((request, client_address))

#enable threaded server
class ThreadedHTTPServer(ThreadPoolMixIn, HTTPServer):
  pass

#custom handler for getting routes
class StoreHandler(BaseHTTPRequestHandler):

  #boiler plate parsing
  def parse_segments(self, post):
    #split the query from the path
    try:
      split = urlparse.urlsplit(self.path)
    except:
      raise Exception('Try a url that looks like /action?query_string')
    #path has the action in it
    try:
      if split.path.split('/')[-1] not in actions:
        raise
    except:
      raise Exception('Try a valid action: ' + str([k for k in actions]))
    #handle POST
    if post:
      body = self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')
      return json.loads(body)
    #handle GET
    else:
      params = urlparse.parse_qs(split.query)
      if 'json' in params:      
        return json.loads(params['json'][0])
    raise Exception('No json provided')

  #parse the request because we dont get this for free!
  def handle_request(self, post):
    #get the reporter data
    segments = self.parse_segments(post)

    try:   
      # get the provider. 
      provider = segments['provider']
      mode = segments['mode']

      # get the segments and loop over to get the rest of the data.
      for segment in segments['segments']:
        segment_id = segment['segment_id']
        prev_segment_id = segment.get('prev_segment_id', None)
        start_time = segment['start_time']
        start_time_dow = time.strftime('%w', time.gmtime(start_time))
        start_time_hour = time.strftime('%H', time.gmtime(start_time))
        end_time = segment['end_time']
        end_time_dow = time.strftime("%w", time.gmtime(end_time))
        end_time_hour = time.strftime("%H", time.gmtime(end_time))
        length = segment['length']

        seconds = end_time - start_time
        if seconds <= 0:
          speed = 0.0
        else:
          #kph
          speed = round((length / (seconds * 1.0))*3.6,2)

        # send it to the cursor.
        thread_local.sql_conn.cursor().execute("execute report (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
          (segment_id, prev_segment_id, mode, start_time, start_time_dow, start_time_hour, 
           end_time, end_time_dow, end_time_hour, length, speed, provider))

    except Exception as e:
      return 400, str(e)

    #hand it back
    return 200, 'ok'

  #send an answer
  def answer(self, code, body):
    response = json.dumps({'response': body })
    self.send_response(code)

    #set some basic info
    self.send_header('Access-Control-Allow-Origin','*')
    self.send_header('Content-type', 'application/json;charset=utf-8')
    self.send_header('Content-length', len(response))
    self.end_headers()

    #hand it back
    self.wfile.write(response)

  #handle the request
  def do(self, post):
    try:
      code, body = self.handle_request(post)
      self.answer(code, body)
    except Exception as e:
      self.answer(400, str(e))

  def do_GET(self):
    self.do(False)
  def do_POST(self):
    self.do(True)

def initialize_db():
  #try to connect forever...
  credentials = (os.environ['POSTGRES_DB'], os.environ['POSTGRES_USER'], os.environ['POSTGRES_HOST'], 
                 os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_PORT'])
  while True:
    try:
      with psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'" % credentials) as sql_conn:
        # check and see if db exists.
        # NOTE: this will have to change for redshift.
        cursor = sql_conn.cursor()
        cursor.execute("select exists(select relname from pg_class where relname = 'segments' and relkind='r');")
        if cursor.fetchone()[0] == False:
          sys.stdout.write('Creating tables.' + os.linesep)
          sys.stdout.flush()
          cursor.execute('CREATE TABLE segments(segment_id bigint, prev_segment_id bigint, ' \
                         'mode text,start_time integer,start_time_dow smallint, start_time_hour smallint, ' \
                         'end_time integer, end_time_dow smallint, end_time_hour smallint, length integer, ' \
                         'speed float, provider text); ' \
                         'CREATE INDEX index_ids ON segments (segment_id);' \
                         'CREATE INDEX index_ids_dates ON segments (segment_id,start_time,end_time);' \
                         'CREATE INDEX index_ids_dow ON segments (segment_id,start_time_dow,end_time_dow);' \
                         'CREATE INDEX index_ids_hours ON segments (segment_id,start_time_hour,end_time_hour);' \
                         'CREATE INDEX index_ids_hours_dow ON segments (segment_id,start_time_dow,start_time_hour,end_time_dow,end_time_hour);' \
                         'CREATE INDEX index_ids_dates_hours ON segments (segment_id,start_time,start_time_hour,end_time,end_time_hour);' \
                         'CREATE INDEX index_ids_dates_dow ON segments (segment_id,start_time,start_time_dow,end_time,end_time_dow);' \
                         'CREATE INDEX index_ids_dates_hours_dow ON segments (segment_id,start_time,start_time_dow,start_time_hour,end_time,end_time_dow,end_time_hour);')
          sys.stdout.write('Done.' + os.linesep)
          sys.stdout.flush()
      break
    except Exception as e:
      sys.stdout.write('Could not find or create table: ' + repr(e) + os.linesep)
      sys.stdout.flush()
      time.sleep(5)

#program entry point
if __name__ == '__main__':

  #parse out the address to bind on
  try:
    address = sys.argv[1].split('/')[-1].split(':')
    address[1] = int(address[1])
    address = tuple(address)
    os.environ['POSTGRES_DB']
    os.environ['POSTGRES_USER']
    os.environ['POSTGRES_HOST']
    os.environ['POSTGRES_PASSWORD']
    os.environ['POSTGRES_PORT']
  except Exception as e:
    sys.stderr.write('Bad address or environment: {0}\n'.format(e))
    sys.stderr.flush()
    sys.exit(1)

  #create the database and default tables.
  initialize_db()
  
  #setup the server
  StoreHandler.protocol_version = 'HTTP/1.0'
  httpd = ThreadedHTTPServer(address, StoreHandler)

  #wait until interrupt
  try:
    httpd.serve_forever()
  except KeyboardInterrupt:
    httpd.server_close()
