package io.opentraffic.datastore;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by matt on 07/06/17.
 */
public class MeasurementBuckets {
    ArrayList<Measurement> m_measurements;

    public MeasurementBuckets(Iterable<Measurement> source) {

        TreeMap<Measurement, Integer> buckets = new TreeMap<>();

        for (Measurement m : source) {
            // strip count and provider, which we don't want to bucket by. keep the count so we
            // can add it back on later.
            Measurement bucketMeasurement = new Measurement(
                    m.vehicleType, m.segmentId, m.nextSegmentId, m.length, m.queueLength, m.timeBucket,
                    m.durationBucket, 0, null);

            buckets.merge(bucketMeasurement, m.count, (a, b) -> nullZero(a) + nullZero(b));
        }

        this.m_measurements = new ArrayList<Measurement>(buckets.size());
        for (Map.Entry<Measurement, Integer> entry : buckets.entrySet()) {
            Measurement k = entry.getKey();
            this.m_measurements.add(new Measurement(
                    k.vehicleType, k.segmentId, k.nextSegmentId, k.length, k.queueLength, k.timeBucket,
                    k.durationBucket, nullZero(entry.getValue()), null));
        }

        assert(isSorted(this.m_measurements));
    }

    public ArrayList<Measurement> getMeasurements() {
        return m_measurements;
    }

    private static int nullZero(Integer i) {
        return i == null ? 0 : i.intValue();
    }

    private static boolean isSorted(ArrayList<Measurement> c) {
        final int size = c.size();
        for (int i = 1; i < size; i++) {
            if (c.get(i).compareTo(c.get(i-1)) <= 0) {
                return false;
            }
        }
        return true;
    }
}
