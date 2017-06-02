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

import io.opentraffic.datastore.internal.Datastore;

/**
 * Created by matt on 31/05/17.
 */
public class AccumulatorProcessor implements Processor<String, Segment.Measurement> {
    public static final String NAME = "io.opentraffic.datastore.AccumulatorState";

    public static final class Key implements Comparable<Key> {
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

        @Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + length;
			result = prime * result + (int) (next_segment_id ^ (next_segment_id >>> 32));
			result = prime * result + (int) (segment_id ^ (segment_id >>> 32));
			result = prime * result + ((vtype == null) ? 0 : vtype.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Key other = (Key) obj;
			if (length != other.length)
				return false;
			if (next_segment_id != other.next_segment_id)
				return false;
			if (segment_id != other.segment_id)
				return false;
			if (vtype != other.vtype)
				return false;
			return true;
		}
    }

    public static final class Value {
        public final long time_bucket;
        public final int duration;
        public final int count;
        public final String provider;

        public Value(long time_bucket, int duration, int count, String provider) {
            this.time_bucket = time_bucket;
            this.duration = duration;
            this.count = count;
            this.provider = provider;
        }
    }

    public static class Values {
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
    private final AccumulatorAlgorithm m_algorithm;

    public AccumulatorProcessor(long npriv, boolean verbose) {
        this.m_npriv = npriv;
        this.m_verbose = verbose;
        this.m_algorithm = new AccumulatorAlgorithm(npriv);
    }

    @Override
    public void init(ProcessorContext context) {
        this.m_context = context;
        this.m_store = getStore(context);
    }

    @Override
    public void process(String partitionKey, Segment.Measurement value) {
        Segment.Measurement measurement = this.m_algorithm.apply(value, this.m_store);

        if (measurement != null) {
            // emit new measurement for this
            if (this.m_verbose) {
                System.out.println("OUTPUT: " + measurement);
            }
            this.m_context.forward(partitionKey, measurement);

        } else {
            if (this.m_verbose) {
                System.out.println("ACCUMULATE: " + value);
            }
        }

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
                    if (val.time_bucket >= this.m_algorithm.getMaxTimeBucket()) {
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
