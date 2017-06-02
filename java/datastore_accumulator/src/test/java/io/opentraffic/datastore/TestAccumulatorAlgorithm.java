package io.opentraffic.datastore;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by matt on 02/06/17.
 */
public class TestAccumulatorAlgorithm {

    private static class KeyValueStore implements org.apache.kafka.streams.state.KeyValueStore<AccumulatorProcessor.Key, AccumulatorProcessor.Values> {

        private HashMap<AccumulatorProcessor.Key, AccumulatorProcessor.Values> m_store;

        public KeyValueStore() {
        	this.m_store = new HashMap<>();
        }
        
        public Set<AccumulatorProcessor.Key> keySet() {
        	return this.m_store.keySet();
        }
        
        @Override
        public void put(AccumulatorProcessor.Key key, AccumulatorProcessor.Values value) {
            this.m_store.put(key, value);
        }

        @Override
        public AccumulatorProcessor.Values putIfAbsent(AccumulatorProcessor.Key key, AccumulatorProcessor.Values value) {
            return this.m_store.putIfAbsent(key, value);
        }

        @Override
        public void putAll(List<KeyValue<AccumulatorProcessor.Key, AccumulatorProcessor.Values>> entries) {
            for (KeyValue<AccumulatorProcessor.Key, AccumulatorProcessor.Values> entry : entries) {
                this.m_store.put(entry.key, entry.value);
            }
        }

        @Override
        public AccumulatorProcessor.Values delete(AccumulatorProcessor.Key key) {
            return this.m_store.remove(key);
        }

        @Override
        public String name() {
            return "fake_name_here";
        }

        @Override
        public void init(ProcessorContext context, StateStore root) {
            throw new RuntimeException("TestAccumulatorAlgorithm.KeyValueStore.init not implemented");
        }

        @Override
        public void flush() {
            throw new RuntimeException("TestAccumulatorAlgorithm.KeyValueStore.flush not implemented");
        }

        @Override
        public void close() {
            throw new RuntimeException("TestAccumulatorAlgorithm.KeyValueStore.close not implemented");
        }

        @Override
        public boolean persistent() {
            throw new RuntimeException("TestAccumulatorAlgorithm.KeyValueStore.persistent not implemented");
        }

        @Override
        public boolean isOpen() {
            throw new RuntimeException("TestAccumulatorAlgorithm.KeyValueStore.isOpen not implemented");
        }

        @Override
        public AccumulatorProcessor.Values get(AccumulatorProcessor.Key key) {
            return this.m_store.get(key);
        }

        @Override
        public KeyValueIterator<AccumulatorProcessor.Key, AccumulatorProcessor.Values> range(AccumulatorProcessor.Key from, AccumulatorProcessor.Key to) {
            throw new RuntimeException("TestAccumulatorAlgorithm.KeyValueStore.range not implemented");
        }

        @Override
        public KeyValueIterator<AccumulatorProcessor.Key, AccumulatorProcessor.Values> all() {
            throw new RuntimeException("TestAccumulatorAlgorithm.KeyValueStore.all not implemented");
        }

        @Override
        public long approximateNumEntries() {
            return this.m_store.size();
        }
    }

    @Test
    public void requiresMultipleMeasurements() {
        AccumulatorAlgorithm alg = new AccumulatorAlgorithm(3);
        Segment.Measurement measurement =
                Segment.Measurement.newBuilder()
                .setSegmentId(1234L)
                .setNextSegmentId(1235L)
                .setLength(1000)
                .setDuration(60)
                .setTimeBucket(
                        Segment.TimeBucket.newBuilder()
                        .setSize(AccumulatorAlgorithm.ONLY_SUPPORTED_BUCKET_SIZE)
                        .setIndex(415671)
                        .build())
                .setCount(1)
                .build();
        KeyValueStore store = new KeyValueStore();

        Segment.Measurement output = alg.apply(measurement, store);

        assertEquals(null, output);
        assertEquals(1, store.approximateNumEntries());
        AccumulatorProcessor.Key key = store.keySet().iterator().next();
        assertEquals(1, store.get(key).values.size());
        
        // add another identical measurement - still shouldn't output anything
        output = alg.apply(measurement, store);
        
        assertEquals(null, output);
        assertEquals(1, store.approximateNumEntries());
        assertEquals(2, store.get(key).values.size());
        
        // a 3rd measurement should make it output the average of the three, which should be basically
        // the same as the original measurement, but with count=3
        output = alg.apply(measurement, store);
        
        assertNotNull(output);
        assertEquals(measurement.getVehicleType(), output.getVehicleType());
        assertEquals(measurement.getSegmentId(), output.getSegmentId());
        assertEquals(measurement.getNextSegmentId(), output.getNextSegmentId());
        assertEquals(measurement.getLength(), output.getLength());
        // this is the average, but the average of 3 identical things should be the same as each thing.
        assertEquals(measurement.getDuration(), output.getDuration());
        // major difference is that the count should now be 3.
        assertEquals(3, output.getCount());
        
        // check that the store is also cleared
        assertEquals(0, store.approximateNumEntries());
        assertEquals(null, store.get(key));
    }
}
