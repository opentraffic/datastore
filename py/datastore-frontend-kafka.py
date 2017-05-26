#!/usr/bin/env python

'''
If you're running this from this directory you can start the server with the
following command:
./datastore_service.py -l localhost:8003 -b kafka_host:kafka_port -t topic

sample url looks like this:
http://localhost:8003/store?json={"segments": [{"segment_id": 345678,
 "prev_segment_id": 356789,"start_time": 98765,"end_time": 98777,"length":555},
 {"segment_id": 345780,"start_time": 98767,"end_time": 98779,"length":678},
 {"segment_id": 345795,"prev_segment_id": 656784,"start_time": 98725,
 "end_time": 98778,"length":479}, {"segment_id": 545678,"prev_segment_id":
 556789,"start_time": 98735,"end_time": 98747,"length":1234}],"provider":
 123456,"mode": "auto"}
'''

import json
import multiprocessing
import threading
from Queue import Queue
import socket
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from cgi import urlparse
import os
import segment_pb2
from kafka import KafkaProducer

actions = set(['store'])


# use a thread pool instead of just frittering off new threads for every
# request
class ThreadPoolMixIn(ThreadingMixIn):
    allow_reuse_address = True  # seems to fix socket.error on server restart

    def serve_forever(self):
        # set up the threadpool
        if 'THREAD_POOL_COUNT' in os.environ:
            pool_size = int(os.environ.get('THREAD_POOL_COUNT'))
        else:
            pool_size = int(os.environ.get('THREAD_POOL_MULTIPLIER', 1)) * \
                        multiprocessing.cpu_count()
        self.requests = Queue(pool_size)
        for x in range(pool_size):
            t = threading.Thread(target=self.process_request_thread)
            t.setDaemon(1)
            t.start()
        # server main loop
        while True:
            self.handle_request()
        self.server_close()

    def process_request_thread(self):
        while True:
            request, client_address = self.requests.get()
            ThreadingMixIn.process_request_thread(
              self, request, client_address)

    def handle_request(self):
        try:
            request, client_address = self.get_request()
        except socket.error:
            return
        if self.verify_request(request, client_address):
            self.requests.put((request, client_address))


# enable threaded server
class ThreadedHTTPServer(ThreadPoolMixIn, HTTPServer):
    pass


# custom handler for getting routes
class StoreHandler(BaseHTTPRequestHandler):

    def __init__(self, producer, topic, req, client_addr, server):
        self.producer = producer
        self.topic = topic
        BaseHTTPRequestHandler.__init__(self, req, client_addr, server)

    # boiler plate parsing
    def parse_segments(self, post):
        # split the query from the path
        try:
            split = urlparse.urlsplit(self.path)
        except:
            raise RuntimeError('Try a url that looks like /action?query_string')
        # path has the action in it
        try:
            if split.path.split('/')[-1] not in actions:
                raise
        except:
            raise RuntimeError('Try a valid action: ' + str(actions))

        # handle GET
        json_req = None
        params = urlparse.parse_qs(split.query)
        if 'json' in params:
            json_req = json.loads(params['json'][0])
            del params['json']

        # handle POST
        if post:
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length).decode('utf-8')
            json_req = json.loads(body)

        # do we have something
        if json_req is None:
            raise RuntimeError('No json provided')

        # mix in the query parameters
        for k, v in params.iteritems():
            if k in json_req:
                continue
            if len(v) == 1:
                json_req[k] = v[0]
            elif len(v) > 1:
                json_req[k] = v

        return json_req

    # parse the request because we dont get this for free!
    def handle_request(self, post):
        # get the reporter data
        segments = self.parse_segments(post)

        # pre-fetch value for hourly bucketing
        bucket_size = segment_pb2.TimeBucket.Size.Value('HOURLY')
        bucket_length = 60

        # reject some queries
        if os.environ.get('SECRET_KEY') != segments.get('secret_key'):
            return 401, 'Unauthorized'

        try:
            # get the provider.
            provider = str(segments['provider'])
            mode = segments['mode']

            # TODO: make this actually parse the input and raise an appropriate
            # error if the vehicle type doesn't exist.
            assert mode == 'auto'
            mode = segment_pb2.VehicleType.Value('AUTO')

            # get the segments and loop over to get the rest of the data.
            for segment in segments['segments']:

                segment_id = segment['segment_id']
                next_segment_id = segment.get('next_segment_id', None)
                start_time = int(segment['start_time'])
                end_time = int(segment['end_time'])
                length = int(segment['length'])

                duration = end_time - start_time
                if duration < 0:
                    raise ValueError(
                        "Start time must not be before end time, but %d < %d" %
                        (start_time, end_time))
                # maximum length should be 1000m, but we will give a little
                # bit of leeway. length _must_ be positive, though.
                if length < 0 or length > 2048:
                    raise ValueError(
                        "Segment length must be a positive number, not %d" %
                        length)

                # send to producer
                key_bytes = bytes(str(segment_id))
                m = segment_pb2.Measurement()
                m.vehicle_type = mode
                m.segment_id = segment_id
                if next_segment_id is not None:
                    m.next_segment_id = next_segment_id
                m.length = length
                m.time_bucket.size = bucket_size
                m.time_bucket.index = end_time / bucket_length
                m.duration = duration
                m.count = 1
                m.provider = provider
                val_bytes = m.SerializeToString()
                self.producer.send(self.topic, key=key_bytes, value=val_bytes)

        except StandardError as e:
            return 400, str(e)

        # hand it back
        return 200, 'ok'

    # send an answer
    def answer(self, code, body):
        response = json.dumps({'response': body})
        self.send_response(code)

        # set some basic info
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-type', 'application/json;charset=utf-8')
        self.send_header('Content-length', len(response))
        self.end_headers()

        # hand it back
        self.wfile.write(response)

    # handle the request
    def do(self, post):
        try:
            code, body = self.handle_request(post)
            self.answer(code, body)
        except StandardError as e:
            self.answer(400, str(e))

    def do_GET(self):
        self.do(False)

    def do_POST(self):
        self.do(True)


# program entry point
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
      description='OTv2 HTTP datastore frontend to put data into Kafka')
    parser.add_argument('-l', '--listen', dest='listen', required=True,
                        help='Address:port to listen on')
    parser.add_argument('-b', '--kafka-bootstrap', dest='bootstrap', nargs='+',
                        required=True, help='Kafka bootstrap server(s)')
    parser.add_argument('-t', '--topic', dest='topic', required=True,
                        help='Topic to publish parsed data on')

    args = parser.parse_args()

    # setup the server
    StoreHandler.protocol_version = 'HTTP/1.0'
    producer = KafkaProducer(bootstrap_servers=args.bootstrap)

    # parse listen address
    host, port = args.listen.split(':')
    port = int(port)

    # TODO: set up thread to periodically flush the producer?

    def make_new_handler(req, client_addr, server):
        return StoreHandler(producer, args.topic, req, client_addr, server)
    httpd = ThreadedHTTPServer((host, port), make_new_handler)

    # wait until interrupt
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        producer.flush()
        httpd.server_close()
