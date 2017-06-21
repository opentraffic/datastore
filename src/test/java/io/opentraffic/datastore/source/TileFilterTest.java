package io.opentraffic.datastore.source;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import io.opentraffic.datastore.BucketSize;
import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;
import io.opentraffic.datastore.VehicleType;

public class TileFilterTest {
    @Test
    public void testTileFilter() {
        final long segment_id = 13L;
        final long tile_id = (1234L << 3L) | 1L;
        final long segment_in_tile = (segment_id << 25) | tile_id;
        final long segment_not_in_tile = segment_in_tile + (1L << 3L);

        ArrayList<Measurement> measurements = new ArrayList<>();
        measurements.add(new Measurement(
                VehicleType.AUTO, segment_in_tile, 1, 1, 1,
                new TimeBucket(BucketSize.HOURLY, 1), (byte)1, 1, null));
        measurements.add(new Measurement(
                VehicleType.AUTO, segment_not_in_tile, 1, 1, 1,
                new TimeBucket(BucketSize.HOURLY, 1), (byte)1, 1, null));

        Iterable<Measurement> iterable = new TileFilter(tile_id, measurements);

        int count = 0;
        for (Measurement m : iterable) {
            // check that it stripped off the tile ID and level from the segment ID
            assertEquals(segment_id, m.segmentId & ((1L << 25L) - 1L));
            count++;
        }
        assertEquals(1, count);
    }
}
