package io.opentraffic.datastore;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Created by matt on 31/05/17.
 */
public class AccumulatorProcessor implements Processor<String, Segment.Measurement> {
    public static final String NAME = "io.opentraffic.datastore.AccumulatorState";
    public static final Segment.TimeBucket.Size ONLY_SUPPORTED_BUCKET_SIZE = Segment.TimeBucket.Size.HOURLY;

    private static class KeySerde implements Serde<Key> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<Key> serializer() {
            return new Serializer<Key>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {
                }

                @Override
                public byte[] serialize(String topic, Key data) {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutput out = null;
                    try {
                        out = new ObjectOutputStream(bos);
                        out.writeObject(data);
                        out.flush();
                        return bos.toByteArray();
                    } catch (IOException ex) {
                        throw new RuntimeException("Unable to serialize Key: ", ex);
                    }
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public Deserializer<Key> deserializer() {
            return new Deserializer<Key>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {
                }

                @Override
                public Key deserialize(String topic, byte[] data) {
                    ByteArrayInputStream bis = new ByteArrayInputStream(data);
                    ObjectInput in = null;
                    try {
                        in = new ObjectInputStream(bis);
                        Key k = (Key)in.readObject();
                        return k;
                    } catch (IOException ex) {
                        throw new RuntimeException("Unable to deserialize Key: ", ex);
                    } catch (ClassNotFoundException ex) {
                        throw new RuntimeException("Unable to deserialize Key: ", ex);
                    }
                }

                @Override
                public void close() {
                }
            };
        }
    }

    private static class ValuesSerde implements Serde<Values> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<Values> serializer() {
            return new Serializer<Values>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {
                }

                @Override
                public byte[] serialize(String topic, Values data) {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutput out = null;
                    try {
                        out = new ObjectOutputStream(bos);
                        out.writeObject(data);
                        out.flush();
                        return bos.toByteArray();
                    } catch (IOException ex) {
                        throw new RuntimeException("Unable to serialize Values: ", ex);
                    }
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public Deserializer<Values> deserializer() {
            return new Deserializer<Values>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {
                }

                @Override
                public Values deserialize(String topic, byte[] data) {
                    ByteArrayInputStream bis = new ByteArrayInputStream(data);
                    ObjectInput in = null;
                    try {
                        in = new ObjectInputStream(bis);
                        Values v = (Values)in.readObject();
                        return v;
                    } catch (IOException ex) {
                        throw new RuntimeException("Unable to deserialize Key: ", ex);
                    } catch (ClassNotFoundException ex) {
                        throw new RuntimeException("Unable to deserialize Key: ", ex);
                    }
                }

                @Override
                public void close() {
                }
            };
        }
    }

    public static final class Key implements Serializable, Comparable<Key> {
        public final Segment.VehicleType vtype;
        public final long segment_id;
        public final long next_segment_id;
        public final int length;

        public Key(Segment.VehicleType vtype, long segment_id, long next_segment_id, int length) {
            this.vtype = vtype;
            this.segment_id = segment_id;
            this.next_segment_id = next_segment_id;
            this.length = length;
        }

        @Override
        public int compareTo(Key other) {
            if (this.vtype != other.vtype) {
                return this.vtype.compareTo(other.vtype);

            } else if (this.segment_id != other.segment_id) {
                return Long.compare(this.segment_id, other.segment_id);

            } else if (this.next_segment_id != other.next_segment_id) {
                return Long.compare(this.next_segment_id, other.next_segment_id);

            } else {
                return Integer.compare(this.length, other.length);
            }
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("Key(");
            b.append(this.vtype.toString()); b.append(", ");
            b.append(this.segment_id); b.append(", ");
            b.append(this.next_segment_id); b.append(", ");
            b.append(this.length);
            b.append(")");
            return b.toString();
        }
    }

    public static final class Value implements Serializable {
        public final long time_bucket;
        public final long duration;
        public final int count;
        public final String provider;

        public Value(long time_bucket, long duration, int count, String provider) {
            this.time_bucket = time_bucket;
            this.duration = duration;
            this.count = count;
            this.provider = provider;
        }
    }

    public static class Values implements Serializable {
        ArrayList<Value> values;
    }

    public static StateStoreSupplier createStore() {
        return Stores.create(NAME)
                .withKeys(new KeySerde())
                .withValues(new ValuesSerde())
                .inMemory().build();
    }

    private static KeyValueStore<Key, Values> getStore(ProcessorContext context) {
        return (KeyValueStore<Key, Values>) context.getStateStore(NAME);
    }

    private ProcessorContext m_context;
    private KeyValueStore<Key, Values> m_store;
    private final long m_npriv;
    private final boolean m_verbose;
    private long m_max_time_bucket;

    public AccumulatorProcessor(long npriv, boolean verbose) {
        this.m_npriv = npriv;
        this.m_verbose = verbose;
        this.m_max_time_bucket = 0;
    }

    @Override
    public void init(ProcessorContext context) {
        this.m_context = context;
        this.m_store = getStore(context);
    }

    @Override
    public void process(String partitionKey, Segment.Measurement value) {
        // check time bucket is okay
        if (!timeBucketOkay(value.getTimeBucket())) {
            // TODO: warn?
            return;
        }

        // extract information from measurement into local keys and values to be looked up in the store.
        Key key = new Key(value.getVehicleType(), value.getSegmentId(), value.getNextSegmentId(), value.getLength());
        final long time_bucket = getTimeBucketIndex(value.getTimeBucket());
        Value val = new Value(time_bucket, value.getDuration(), value.getCount(), value.getProvider());

        // keep the last-seen time bucket index. we will use that to clean out the data store periodically
        if (time_bucket > this.m_max_time_bucket) {
            this.m_max_time_bucket = time_bucket;
        }

        // fetch existing values and append this new one.
        Values values = this.m_store.get(key);
        if (values == null) {
            values = new Values();
        }
        if (values.values == null) {
            values.values = new ArrayList<>();
        }
        // clear out any values which were in a previous (now expired) time bucket
        values.values.removeIf(new Predicate<Value>() {
            @Override
            public boolean test(Value value) {
                return value.time_bucket < time_bucket;
            }
        });
        values.values.add(val);

        // check if we can emit a new measurement, clearing out this key
        if (values.values.size() >= this.m_npriv) {
            // sum over values
            Segment.Measurement sum = sumValues(key, values.values);

            // emit new measurement for this
            if (this.m_verbose) {
                System.out.println("OUTPUT: " + sum);
            }
            this.m_context.forward(partitionKey, sum);

            // delete key
            this.m_store.delete(key);

        } else {
            if (this.m_verbose) {
                System.out.println("ACCUMULATE[" + values.values.size() + "/" + this.m_npriv + "]: " + key);
            }
            // update the key with the additional value
            this.m_store.put(key, values);
        }
    }

    private boolean timeBucketOkay(Segment.TimeBucket timeBucket) {
        return timeBucket.getSize() == ONLY_SUPPORTED_BUCKET_SIZE;
    }

    private Segment.Measurement sumValues(Key k, ArrayList<Value> values) {
        // this shouldn't ever happen, as we've just added at least one Value to the array. however, just to be on the safe side...
        if (values.isEmpty()) {
            throw new RuntimeException("List of values is not allowed to be empty in sumValues.");
        }

        // check that all the time bucket values are the same - this should be ensured by the "removeIf" loop in the process code. but better to check!
        long time_bucket = values.get(0).time_bucket;
        int count = 0;
        int duration_sum = 0;
        String provider = values.get(0).provider;
        for (Value v : values) {
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

    @Override
    public void punctuate(long timestamp) {
        ArrayList<Key> keysToDelete = new ArrayList<>();
        KeyValueIterator<Key, Values> iterator = this.m_store.all();
        while (iterator.hasNext()) {
            KeyValue<Key, Values> keyValue = iterator.next();

            if (keyValue.value == null) {
                // delete any key with a null value
                keysToDelete.add(keyValue.key);

            } else if (keyValue.value.values == null) {
                // delete any key where the values are unset - although this should theoretically never happen
                keysToDelete.add(keyValue.key);

            } else if (keyValue.value.values.isEmpty()) {
                // delete any key with an empty set of values
                keysToDelete.add(keyValue.key);

            } else {
                // delete any key where all the time bucket values are in the past
                boolean delete = true;
                for (Value val : keyValue.value.values) {
                    if (val.time_bucket >= this.m_max_time_bucket) {
                        delete = false;
                        break;
                    }
                }
                if (delete) {
                    keysToDelete.add(keyValue.key);
                }
            }
        }

        for (Key k : keysToDelete) {
            this.m_store.delete(k);
        }
    }

    @Override
    public void close() {

    }
}
