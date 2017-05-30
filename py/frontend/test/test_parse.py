from otdatastore.parse import parse_segments
from otdatastore import segment_pb2
import unittest


# this is the invalid GraphId from Valhalla. We'll just borrow that, since the
# default default for protocol buffers is zero, which is a valid GraphId and
# so might cause confusion.
INVALID_SEGMENT_ID = 0x3ffffffffff


class TextParseSegments(unittest.TestCase):

    def test_parse_empty(self):
        # empty list is ok, but produces no results
        segments = list(parse_segments({
            'mode':'auto',
            'provider':'foo',
            'segments': [],
        }))
        self.assertEquals([], segments)

    def test_parse_missing_provider(self):
        # provider must be present
        def func():
            segments = list(parse_segments({
                'mode':'auto',
                'segments': [],
            }))
        self.assertRaises(StandardError, func)

    def test_parse_missing_mode(self):
        # mode must be present
        def func():
            segments = list(parse_segments({
                'provider': 'foo',
                'segments': [],
            }))
        self.assertRaises(StandardError, func)

    def test_parse_alternative_mode(self):
        # mode must be 'auto' at present
        def func():
            segments = list(parse_segments({
                'provider': 'foo',
                'mode': 'truck',
                'segments': [],
            }))
        self.assertRaises(StandardError, func)

    def test_parse_success(self):
        # parse a known segment and check that the result is what we expected
        # it to be.
        segments = list(parse_segments({
            'mode':'auto',
            'provider':'foo',
            'segments': [
                {
                    'segment_id': 12345,
                    'next_segment_id': 12346,
                    'start_time': 1496166202,
                    'end_time': 1496166337,
                    'length': 256,
                }
            ],
        }))

        self.assertEquals(1, len(segments))
        seg = segments[0]
        auto_mode = segment_pb2.VehicleType.Value('AUTO')
        hourly = segment_pb2.TimeBucket.Size.Value('HOURLY')

        self.assertEquals(auto_mode, seg.vehicle_type)
        self.assertEquals('foo', seg.provider)
        self.assertEquals(12345, seg.segment_id)
        self.assertEquals(12346, seg.next_segment_id)
        self.assertEquals(256, seg.length)
        self.assertEquals(hourly, seg.time_bucket.size)
        # 415601 = 1496166337 / 3600 - the hourly bucket for the end time
        self.assertEquals(415601, seg.time_bucket.index)

    def test_parse_next_segment_id_optional(self):
        # check that the next segment ID can be omitted, and what the value
        # is in that case.
        segments = list(parse_segments({
            'mode':'auto',
            'provider':'foo',
            'segments': [
                {
                    'segment_id': 12345,
                    'start_time': 1496166202,
                    'end_time': 1496166337,
                    'length': 256,
                }
            ],
        }))

        self.assertEquals(1, len(segments))
        seg = segments[0]
        auto_mode = segment_pb2.VehicleType.Value('AUTO')
        hourly = segment_pb2.TimeBucket.Size.Value('HOURLY')

        self.assertEquals(auto_mode, seg.vehicle_type)
        self.assertEquals('foo', seg.provider)
        self.assertEquals(12345, seg.segment_id)
        self.assertEquals(INVALID_SEGMENT_ID, seg.next_segment_id)
        self.assertEquals(256, seg.length)
        self.assertEquals(hourly, seg.time_bucket.size)
        # 415601 = 1496166337 / 3600 - the hourly bucket for the end time
        self.assertEquals(415601, seg.time_bucket.index)
