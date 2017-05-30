from otdatastore import segment_pb2

def parse_segments(segments):
    # pre-fetch value for hourly bucketing
    # TODO: make configurable.
    bucket_size = segment_pb2.TimeBucket.Size.Value('HOURLY')
    bucket_length = 3600

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

        yield m
