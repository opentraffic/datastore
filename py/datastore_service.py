#!/usr/bin/env python

'''
If you're running this from this directory you can start the server with the following command:
./datastore_service.py localhost:8003

sample url looks like this:
http://localhost:8003/store?json={%22segments%22:[{%22mode%22:%20%22auto%22,%22segment_id%22:%20345678,%22prev_segment_id%22:%20356789,%22begin_time%22:%2098765,%22end_time%22:%2098777,%22length%22:555},{%22mode%22:%20%22bus%22,%22segment_id%22:%20345780,%22begin_time%22:%2098767,%22end_time%22:%2098779,%22length%22:678},{%22mode%22:%20%22auto%22,%22segment_id%22:%20345795,%22prev_segment_id%22:%20656784,%22begin_time%22:%2098725,%22end_time%22:%2098778,%22length%22:479},{%22mode%22:%20%22auto%22,%22segment_id%22:%20545678,%22prev_segment_id%22:%20556789,%22begin_time%22:%2098735,%22end_time%22:%2098747,%22length%22:1234}],%22provider%22:%20123456}
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
import os
import time

actions = set(['store'])

#use a thread pool instead of just frittering off new threads for every request
class ThreadPoolMixIn(ThreadingMixIn):
  allow_reuse_address = True  # seems to fix socket.error on server restart

  def serve_forever(self):
    # set up the threadpool
    self.requests = Queue(multiprocessing.cpu_count())
    for x in range(multiprocessing.cpu_count()):
      t = threading.Thread(target = self.process_request_thread)
      t.setDaemon(1)
      t.start()
    # server main loop
    while True:
      self.handle_request()
    self.server_close()

  def make_thread_locals(self):
    credentials = (os.environ['POSTGRES_DB'], os.environ['POSTGRES_USER'], os.environ['POSTGRES_HOST'], 
                   os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_PORT'])
    try:
      sql_conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'" % credentials)
    except Exception as e:
      raise Exception('Failed to connect to database with: %s' % repr(e))

    sys.stdout.write("Connected to db\n")
    sys.stdout.flush()

    try:
      # check and see if prepared statement exists...if not, create it
      cursor = sql_conn.cursor()
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'report');")

      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE report AS INSERT INTO segments (segment_id,prev_segment_id,mode," \
                              "start_time,end_time,length,provider) VALUES ($1,$2,$3,$4,$5,$6,$7);"
          cursor.execute(prepare_statement)
          sql_conn.commit()
          sys.stdout.write("Created prepare statement.\n")
          sys.stdout.flush()
        except Exception as e:
          raise Exception("Can't create prepare statement: %s" % repr(e))
    except Exception as e:
      raise Exception("Can't check for prepare statement: %s" % repr(e))
    self.sql_conn = sql_conn

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

      # get the segments and loop over to get the rest of the data.
      for segment in segments['segments']:
        segment_id = segment['segment_id']
        prev_segment_id = segment.get('prev_segment_id', None)
        mode = segment['mode']
        start_time = segment['begin_time']
        end_time = segment['end_time']
        length = segment['length']

        # send it to the cursor.
        self.server.sql_conn.cursor().execute("execute report (%s,%s,%s,%s,%s,%s,%s)",
          (segment_id, prev_segment_id, mode, start_time, end_time, length, provider))
      
      # write all the data to the db.
      self.server.sql_conn.commit()                
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
      sql_conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'" % credentials)
      break
    except Exception as e:
      # repeat until you connect.
      time.sleep(5)

  # check and see if db exists.
  cursor = sql_conn.cursor()
  # this will have to change for redshift.
  try:
    cursor.execute("select exists(select relname from pg_class where relname = 'segments' and relkind='r');")

    if cursor.fetchone()[0] == False:
      sys.stdout.write("Creating tables.\n")
      sys.stdout.flush()
      try:
        # may need some more indexes here.
        cursor.execute("CREATE TABLE segments(segment_id integer, prev_segment_id integer, " \
                       "mode text,start_time integer,end_time integer, length integer, provider text);")
        sql_conn.commit()
        sys.stdout.write("Done.\n")
        sys.stdout.flush()
      except Exception as e:
        sys.stdout.write("Can't create tables: {0}\n".format(e))
        sys.stdout.flush()
        sys.exit(1)
  except Exception as e:
    sys.stdout.write("Can't check for tables.: {0}\n".format(e))
    sys.stdout.flush()
    sys.exit(1)

  sql_conn.close()

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
