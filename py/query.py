#!/usr/bin/env python

'''
If you're running this from this directory you can start the server with the following command:
./query.py localhost:8005

sample url looks like this:
http://localhost:8005/query_by_ids?json={"segments":[345678,345780,345795]}'''

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
import calendar
import datetime

actions = set(['query_by_ids','query_by_range'])

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
      raise Exception('Failed to connect to database.')

    sys.stdout.write("Connected to db\n")
    sys.stdout.flush()

    try:
      # check and see if prepare statements exists...if not, create them
      cursor = sql_conn.cursor()
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'query_by_id');")

      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE query_by_id AS SELECT segment_id, prev_segment_id, start_time, " \
                              "start_time_dow, start_time_hour, end_time, speed, length FROM segments where " \
                              "segment_id = ANY ($1) order by segment_id, prev_segment_id;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")

      cursor.execute("select exists(select name from pg_prepared_statements where name = 'query_by_range');")

      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE query_by_range AS SELECT segment_id, prev_segment_id, start_time, " \
                              "start_time_dow, start_time_hour, end_time, speed, length FROM segments where " \
                              "start_time >= $1 and start_time < $2 order by segment_id, prev_segment_id;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")
    except Exception as e:
      raise Exception("Can't check for prepare statements")
    self.sql_conn = sql_conn
    sys.stdout.write("Created prepare statements.\n")
    sys.stdout.flush()

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
    #get the query data
    segments = self.parse_segments(post)

    try:   
      # get the provider. 
      list_of_ids = segments['segments'] if 'segments' in segments else None
      start_date_time = segments.get('start_date_time', None)
      end_date_time = segments.get('end_date_time', None)
      cursor = self.server.sql_conn.cursor()

      # send it to the cursor.

      if None in (start_date_time, end_date_time):
        cursor.execute("execute query_by_id (%s)",(list_of_ids,))
      else:
        s_date_time = calendar.timegm(time.strptime(start_date_time,"%Y-%m-%d"))
        # add a day so that we include the end day in the range.
        d = datetime.datetime.strptime(end_date_time,"%Y-%m-%d") + datetime.timedelta(days=1)
        e_date_time = calendar.timegm(d.timetuple())

        cursor.execute("execute query_by_range (%s, %s)",(s_date_time,e_date_time))
      rows = cursor.fetchall()      
      segments = {'segments':[]}

      for row in rows:
        d = dict()
        d['segment_id'] = row[0]
        d['prev_segment_id'] = row[1]
        d['start_time'] = row[2]
        d['start_time_dow'] = row[3]
        d['start_time_hour'] = row[4]
        d['end_time'] = row[5]
        d['speed'] = row[6]
        d['length'] = row[7]

        segments['segments'].append(d)

      results = json.dumps(segments)

    except Exception as e:
      # must commit if failure
      #self.server.sql_conn.commit()             
      return 400, str(e)

    #hand it back
    return 200, json.loads(results)

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
      sys.stdout.write("No tables exist!\n".format(e))
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
