#!/usr/bin/env python

'''
If you're running this from this directory you can start the server with the following command:
./query.py localhost:8004

sample url looks like this:
http://localhost:8004/query?segment_ids=19320,67128184
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00
http://localhost:8004/query?segment_ids=19320,67128184&dow=0
http://localhost:8004/query?segment_ids=19320,67128184&hours=11,12,3
http://localhost:8004/query?segment_ids=19320,67128184&hours=11,12,3&dow=0,1,2,3,4,5,6
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00&hours=11,12,3,0
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00&dow=0,1,2,3,4,5,6
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00&hours=11,12,3,0&dow=0,1,2,3,4,5,6
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
import calendar
import datetime
import urllib
from distutils.util import strtobool

actions = set(['query'])

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

      # id only query
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'q_ids');")
      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE q_ids AS SELECT segment_id, avg(speed) as average_speed FROM " \
                              " segments where segment_id = ANY ($1) group by segment_id;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")

      # id, date, hours, and dow query
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'q_ids_date_hours_dow');")
      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE q_ids_date_hours_dow AS SELECT segment_id, avg(speed) as average_speed, " \
                              "start_time_dow as dow, start_time_hour as hour, count(segment_id) as observation_count " \
                              "FROM segments where segment_id = ANY ($1) and " \
                              "((start_time >= $2 and start_time <= $3) and (end_time >= $2 and end_time <= $3)) and " \
                              "(start_time_hour = ANY ($4) and end_time_hour = ANY ($4)) and " \
                              "(start_time_dow = ANY ($5) and end_time_dow = ANY ($5)) " \
                              "group by segment_id, start_time_dow, start_time_hour;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")

      # id, date, and hours query
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'q_ids_date_hours');")
      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE q_ids_date_hours AS SELECT segment_id, avg(speed) as average_speed, " \
                              "start_time_hour as hour FROM segments where " \
                              "segment_id = ANY ($1) and " \
                              "((start_time >= $2 and start_time <= $3) and (end_time >= $2 and end_time <= $3)) and " \
                              "(start_time_hour = ANY ($4) and end_time_hour = ANY ($4)) " \
                              "group by segment_id, start_time_hour;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")

      # id, date, and dow query
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'q_ids_date_dow');")
      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE q_ids_date_dow AS SELECT segment_id, avg(speed) as average_speed, " \
                              "start_time_dow as dow FROM segments where " \
                              "segment_id = ANY ($1) and " \
                              "((start_time >= $2 and start_time <= $3) and (end_time >= $2 and end_time <= $3)) and " \
                              "(start_time_dow = ANY ($4) and end_time_dow = ANY ($4)) " \
                              "group by segment_id, start_time_dow;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")

      # id, hours, and dow query
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'q_ids_hours_dow');")
      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE q_ids_hours_dow AS SELECT segment_id, avg(speed) as average_speed, " \
                              "start_time_dow as dow, start_time_hour as hour, count(segment_id) as observation_count " \
                              "FROM segments where segment_id = ANY ($1) and " \
                              "(start_time_hour = ANY ($2) and end_time_hour = ANY ($2)) and " \
                              "(start_time_dow = ANY ($3) and end_time_dow = ANY ($3)) " \
                              "group by segment_id, start_time_dow, start_time_hour;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")

      # id and date
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'q_ids_date');")
      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE q_ids_date AS SELECT segment_id, avg(speed) as average_speed " \
                              "FROM segments where " \
                              "segment_id = ANY ($1) and " \
                              "((start_time >= $2 and start_time <= $3) and (end_time >= $2 and end_time <= $3)) " \
                              "group by segment_id;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")

      # id and hours query
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'q_ids_hours');")
      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE q_ids_hours AS SELECT segment_id, avg(speed) as average_speed, " \
                              "start_time_hour as hour FROM segments where " \
                              "segment_id = ANY ($1) and " \
                              "(start_time_hour = ANY ($2) and end_time_hour = ANY ($2)) " \
                              "group by segment_id, start_time_hour;"
          cursor.execute(prepare_statement)
          sql_conn.commit()
        except Exception as e:
          raise Exception("Can't create prepare statements")

      # id and dow query
      cursor.execute("select exists(select name from pg_prepared_statements where name = 'q_ids_dow');")
      if cursor.fetchone()[0] == False:
        try:
          prepare_statement = "PREPARE q_ids_dow AS SELECT segment_id, avg(speed) as average_speed, " \
                              "start_time_dow as dow FROM segments where " \
                              "segment_id = ANY ($1) and " \
                              "(start_time_dow = ANY ($2) and end_time_dow = ANY ($2)) " \
                              "group by segment_id, start_time_dow;"
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
class QueryHandler(BaseHTTPRequestHandler):

  #boiler plate parsing
  def parse_url(self, post):
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
      params = urlparse.parse_qs(body)
      return params
    #handle GET
    else:
      params = urlparse.parse_qs(split.query)
      return params

  #parse the request because we dont get this for free!
  def handle_request(self, post):
    #get the query data
    params = self.parse_url(post)

    try:   
      # get the kvs
      ids = s_date_time = e_date_time = hours = dow = None
      list_of_ids = params['segment_ids'] if 'segment_ids' in params else None
      list_of_dow = params['dow'] if 'dow' in params else None
      list_of_hours = params['hours'] if 'hours' in params else None
      start_date_time = params.get('start_date_time', None)
      end_date_time = params.get('end_date_time', None)
      include_observation_counts = params.get('include_observation_counts', None)
      cursor = self.server.sql_conn.cursor()

      #ids will come in as csv string.  we must split and cast to list
      #so that the cursor can bind the list.
      if list_of_ids:
        ids = [ int(i) for i in list_of_ids[0].split(',')]
      else:
        # for now return error...ids are required.
        return 400, "Please provide a list of ids."

      if start_date_time and not end_date_time:
        return 400, "Please provide an end_date_time."
      elif end_date_time and not start_date_time:
        return 400, "Please provide a start_date_time."

      if start_date_time:
        s_date_time = calendar.timegm(time.strptime(start_date_time[0],"%Y-%m-%dT%H:%M:%S"))

      if end_date_time:
        e_date_time = calendar.timegm(time.strptime(end_date_time[0],"%Y-%m-%dT%H:%M:%S"))

      if list_of_dow:
        dow = [ int(i) for i in list_of_dow[0].split(',')]

      if list_of_hours:
        hours = [ int(i) for i in list_of_hours[0].split(',')]

      #observation counts for authorized users.
      try:
        if include_observation_counts:
          include_observation_counts = bool(strtobool(str(include_observation_counts[0])))
        else:
          include_observation_counts = False
      #invalid value entered.
      except:
        include_observation_counts = False

      columns = ['segment_id', 'average_speed']
      # id only query
      if all(parameters is None for parameters in (s_date_time, e_date_time, hours, dow)):
        cursor.execute("execute q_ids (%s)",(ids,))

      # id, date, hours, and dow query
      elif all(parameters is not None for parameters in (s_date_time, e_date_time, hours, dow)):
        cursor.execute("execute q_ids_date_hours_dow (%s, %s, %s, %s, %s)",
                      ((ids,),s_date_time,e_date_time,(hours,),(dow,)))
        if include_observation_counts:
          columns = ['segment_id', 'average_speed', 'dow', 'hour', 'observation_count']
        else:
          columns = ['segment_id', 'average_speed', 'dow', 'hour']

      # id, date, and hours query
      elif all(parameters is not None for parameters in (s_date_time, e_date_time, hours)):
        cursor.execute("execute q_ids_date_hours (%s, %s, %s, %s)",
                      ((ids,),s_date_time,e_date_time,(hours,)))
        columns = ['segment_id', 'average_speed', 'hour']

      # id, date, and dow query
      elif all(parameters is not None for parameters in (s_date_time, e_date_time, dow)):
        cursor.execute("execute q_ids_date_dow (%s, %s, %s, %s)",
                      ((ids,),s_date_time,e_date_time,(dow,)))
        columns = ['segment_id', 'average_speed', 'dow']

      # id, hours, and dow query
      elif all(parameters is not None for parameters in (hours, dow)):
        cursor.execute("execute q_ids_hours_dow (%s, %s, %s)",
                      ((ids,),(hours,),(dow,)))
        if include_observation_counts:
          columns = ['segment_id', 'average_speed', 'dow', 'hour', 'observation_count']
        else:
          columns = ['segment_id', 'average_speed', 'dow', 'hour']

      # id and date query
      elif all(parameters is not None for parameters in (s_date_time, e_date_time)):
        cursor.execute("execute q_ids_date (%s, %s, %s)",
                      ((ids,),s_date_time,e_date_time))

      # id and hours query
      elif hours is not None:
        cursor.execute("execute q_ids_hours (%s, %s)",((ids,),(hours,)))
        columns = ['segment_id', 'average_speed', 'hour']

      # id and dow query
      elif dow is not None:
        cursor.execute("execute q_ids_dow (%s, %s)",((ids,),(dow,)))
        columns = ['segment_id', 'average_speed', 'dow']

      rows = cursor.fetchall()
      results = {'segments':[]}
      for row in rows:
        segment = dict(zip(columns, row))
        results['segments'].append(segment)

    except Exception as e:
      # must commit if failure
      self.server.sql_conn.commit()
      return 400, str(e)

    #hand it back
    return 200, results

  #send an answer
  def answer(self, code, body):

    response = json.dumps(body, separators=(',', ':')) if type(body) == dict else json.dumps({'response': body})

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
  QueryHandler.protocol_version = 'HTTP/1.0'
  httpd = ThreadedHTTPServer(address, QueryHandler)

  #wait until interrupt
  try:
    httpd.serve_forever()
  except KeyboardInterrupt:
    httpd.server_close()
