package io.opentraffic.datastore.sink;

import io.opentraffic.datastore.BucketSize;
import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;
import io.opentraffic.datastore.VehicleType;
import io.opentraffic.datastore.flatbuffer.Entry;
import io.opentraffic.datastore.flatbuffer.Histogram;
import io.opentraffic.datastore.flatbuffer.Segment;
import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.*;
import java.util.ArrayList;

/**
 * Created by matt on 07/06/17.
 */
public class FlatBuffersSinkTest {

    // Little helper function to encode the measurements in a tile, write it out to an
    // in-memory buffer and read the result back as a Histogram object.
    private Histogram roundTrip(ArrayList<Measurement> measurements) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        FlatBufferSink sink = new FlatBufferSink(bos);

        try {
            sink.write(measurements);

        } catch (IOException ex) {
            fail(ex.getLocalizedMessage());
        }

        Histogram h = Histogram.getRootAsHistogram(ByteBuffer.wrap(bos.toByteArray()));

        return h;
    }

    @Test
    public void testFlatBuffersWriteOK() {
        final int segmentId = 1;
        final int nextSegmentId = 1235;
        final int length = 1000;
        final byte durationBucket = 60;
        final TimeBucket timeBucket = new TimeBucket(BucketSize.HOURLY, 415787);
        final int count = 1;

        ArrayList<Measurement> measurements = new ArrayList<>();
        measurements.add(new Measurement(
                VehicleType.AUTO, (long)segmentId, (long)nextSegmentId, length,
                timeBucket, durationBucket, count, null));

        Histogram h = roundTrip(measurements);

        assertEquals(VehicleType.AUTO, VehicleType.values()[(int)h.vehicleType()]);
        assertEquals(segmentId + 1, h.segmentsLength());
        Segment s = h.segments(segmentId);
        assertEquals(segmentId, s.segmentId());
        assertEquals(1, s.nextSegmentIdsLength());
        assertEquals(nextSegmentId, s.nextSegmentIds(0));
        assertEquals(1, s.entriesLength());
        Entry e = s.entries(0);
        assertEquals(59, e.dayHour());
        assertEquals(0, e.nextSegmentIdx());
        assertEquals(durationBucket, e.speedBucket());
        assertEquals(count, e.count());
    }

    // similar test, but check for two segments
    @Test
    public void testFlatBuffersTwo() {
        TimeBucket timeBucket = new TimeBucket(BucketSize.HOURLY, 415787);
        ArrayList<Measurement> measurements = new ArrayList<>();

        measurements.add(new Measurement(
                VehicleType.AUTO, 3L, 1L, 990,
                timeBucket, (byte)60, 2, null));
        measurements.add(new Measurement(
                VehicleType.AUTO, 3L, 5L, 999,
                timeBucket, (byte)60, 12, null));
        measurements.add(new Measurement(
                VehicleType.AUTO, 5L, 3L, 1001,
                timeBucket, (byte)65, 7, null));

        Histogram h = roundTrip(measurements);

        assertEquals(VehicleType.AUTO, VehicleType.values()[(int)h.vehicleType()]);
        // should have 6 segments because the maximum segment ID is 5 and it's a
        // dense packed array (starting at zero).
        assertEquals(6, h.segmentsLength());

        {
            Segment s = h.segments(3);
            assertEquals(3, s.segmentId());

            // sample data has 2 next segment IDs: [1, 5], and they should be in the
            // same order here.
            assertEquals(2, s.nextSegmentIdsLength());
            assertEquals(1, s.nextSegmentIds(0));
            assertEquals(5, s.nextSegmentIds(1));
            assertEquals(2, s.entriesLength());

            {
                Entry e0 = s.entries(0);
                assertEquals(59, e0.dayHour());
                assertEquals(0, e0.nextSegmentIdx());
                assertEquals(60, e0.speedBucket());
                assertEquals(2, e0.count());
            }

            {
                Entry e1 = s.entries(1);
                assertEquals(59, e1.dayHour());
                assertEquals(1, e1.nextSegmentIdx());
                assertEquals(60, e1.speedBucket());
                assertEquals(12, e1.count());
            }
        }

        {
            Segment s = h.segments(5);
            assertEquals(5, s.segmentId());

            assertEquals(1, s.nextSegmentIdsLength());
            assertEquals(3, s.nextSegmentIds(0));
            assertEquals(1, s.entriesLength());

            Entry e = s.entries(0);
            assertEquals(59, e.dayHour());
            assertEquals(0, e.nextSegmentIdx());
            assertEquals(65, e.speedBucket());
            assertEquals(7, e.count());
        }
    }
}
