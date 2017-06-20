package io.opentraffic.datastore.source;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import io.opentraffic.datastore.BucketSize;
import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;
import io.opentraffic.datastore.VehicleType;

public class TimeFilterTest {
    @Test
    public void testTimeFilter() {
        ArrayList<Measurement> measurements = new ArrayList<>();
        measurements.add(new Measurement(
                VehicleType.AUTO, 1, 1, 1, 1,
                new TimeBucket(BucketSize.HOURLY, 1), (byte)1, 1, null));
        measurements.add(new Measurement(
                VehicleType.AUTO, 1, 1, 1, 1,
                new TimeBucket(BucketSize.HOURLY, 2), (byte)1, 1, null));
        measurements.add(new Measurement(
                VehicleType.AUTO, 1, 1, 1, 1,
                new TimeBucket(BucketSize.HOURLY, 3), (byte)1, 1, null));

        TimeBucket bucket = new TimeBucket(BucketSize.HOURLY, 2);
        Iterable<Measurement> iterable = new TimeFilter(bucket, measurements);

        int count = 0;
        for (Measurement m : iterable) {
            assertEquals(bucket, m.timeBucket);
            count++;
        }
        assertEquals(1, count);
    }
}
