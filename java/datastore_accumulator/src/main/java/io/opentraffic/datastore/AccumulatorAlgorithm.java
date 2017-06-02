package io.opentraffic.datastore;

import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.function.Predicate;

/**
 * Created by matt on 02/06/17.
 */
public class AccumulatorAlgorithm {
    public static final Segment.TimeBucket.Size ONLY_SUPPORTED_BUCKET_SIZE = Segment.TimeBucket.Size.HOURLY;

    private final long m_npriv;
    private long m_max_time_bucket;

    public AccumulatorAlgorithm(long npriv) {
        this.m_npriv = npriv;
        this.m_max_time_bucket = 0;
    }

    public Segment.Measurement apply(Segment.Measurement value, KeyValueStore<AccumulatorProcessor.Key, AccumulatorProcessor.Values> store) {
        // check time bucket is okay
        if (!timeBucketOkay(value.getTimeBucket())) {
            // TODO: warn?
            return null;
        }

        // extract information from measurement into local keys and values to be looked up in the store.
        AccumulatorProcessor.Key key = new AccumulatorProcessor.Key(value.getVehicleType(), value.getSegmentId(), value.getNextSegmentId(), value.getLength());
        final long time_bucket = getTimeBucketIndex(value.getTimeBucket());
        AccumulatorProcessor.Value val = new AccumulatorProcessor.Value(time_bucket, value.getDuration(), value.getCount(), value.getProvider());

        // keep the last-seen time bucket index. we will use that to clean out the data store periodically
        if (time_bucket > this.m_max_time_bucket) {
            this.m_max_time_bucket = time_bucket;
        }

        // fetch existing values and append this new one.
        AccumulatorProcessor.Values values = store.get(key);
        if (values == null) {
            values = new AccumulatorProcessor.Values();
        }
        if (values.values == null) {
            values.values = new ArrayList<>();
        }
        // clear out any values which were in a previous (now expired) time bucket
        values.values.removeIf(new Predicate<AccumulatorProcessor.Value>() {
            @Override
            public boolean test(AccumulatorProcessor.Value value) {
                return value.time_bucket < time_bucket;
            }
        });
        values.values.add(val);

        // check if we can emit a new measurement, clearing out this key
        if (values.values.size() >= this.m_npriv) {
            // sum over values
            Segment.Measurement sum = sumValues(key, values.values);

            // delete key
            store.delete(key);

            // return measurement to emit
            return sum;

        } else {
            // update the key with the additional value
            store.put(key, values);

            return null;
        }
    }

    private boolean timeBucketOkay(Segment.TimeBucket timeBucket) {
        return timeBucket.getSize() == ONLY_SUPPORTED_BUCKET_SIZE;
    }

    private Segment.Measurement sumValues(AccumulatorProcessor.Key k, ArrayList<AccumulatorProcessor.Value> values) {
        // this shouldn't ever happen, as we've just added at least one Value to the array. however, just to be on the safe side...
        if (values.isEmpty()) {
            throw new RuntimeException("List of values is not allowed to be empty in sumValues.");
        }

        // check that all the time bucket values are the same - this should be ensured by the "removeIf" loop in the process code. but better to check!
        long time_bucket = values.get(0).time_bucket;
        int count = 0;
        int duration_sum = 0;
        String provider = values.get(0).provider;
        for (AccumulatorProcessor.Value v : values) {
            if (v.time_bucket != time_bucket) {
                throw new RuntimeException("Expected all time bucket indexes to be " + time_bucket + ", but got value with bucket index " + v.time_bucket);
            }
            count += v.count;
            duration_sum += v.duration * v.count;
            // we only keep provider if it's the same for all measurements.
            if (v.provider != provider) {
                provider = null;
            }
        }

        // build the protobuf message
        Segment.Measurement.Builder b = Segment.Measurement.newBuilder();
        // only set vehicle type if it's different from the default
        if (k.vtype != b.getVehicleType()) {
            b.setVehicleType(k.vtype);
        }
        b.setSegmentId(k.segment_id);
        // only set next segment ID if it's different from the default
        if (k.next_segment_id != b.getNextSegmentId()) {
            b.setNextSegmentId(k.next_segment_id);
        }
        b.setLength(k.length);

        b.setTimeBucket(Segment.TimeBucket.newBuilder()
                .setSize(ONLY_SUPPORTED_BUCKET_SIZE)
                .setIndex(time_bucket)
                .build());
        b.setCount(count);
        b.setDuration(duration_sum / count);
        if (provider != null) {
            b.setProvider(provider);
        }
        return b.build();
    }

    private long getTimeBucketIndex(Segment.TimeBucket timeBucket) {
        return timeBucket.getIndex();
    }

    public long getMaxTimeBucket() {
        return m_max_time_bucket;
    }
}
