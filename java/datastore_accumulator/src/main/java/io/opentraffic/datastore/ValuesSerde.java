package io.opentraffic.datastore;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opentraffic.datastore.internal.Datastore;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by matt on 02/06/17.
 */
class ValuesSerde implements Serde<AccumulatorProcessor.Values> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<AccumulatorProcessor.Values> serializer() {
        return new Serializer<AccumulatorProcessor.Values>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, AccumulatorProcessor.Values data) {
                Datastore.Values.Builder valuesBuilder = Datastore.Values.newBuilder();

                if (data != null && data.values != null) {
                    for (AccumulatorProcessor.Value value : data.values) {
                        Datastore.Value.Builder builder = Datastore.Value.newBuilder();
                        builder.setTimeBucket(
                                Segment.TimeBucket.newBuilder()
                                        .setSize(AccumulatorProcessor.ONLY_SUPPORTED_BUCKET_SIZE)
                                        .setIndex(value.time_bucket)
                                        .build());
                        builder.setDuration(value.duration);
                        builder.setCount(value.count);
                        if (value.provider != null) {
                            builder.setProvider(value.provider);
                        }
                        valuesBuilder.addValues(builder.build());
                    }
                }

                return valuesBuilder.build().toByteArray();
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public Deserializer<AccumulatorProcessor.Values> deserializer() {
        return new Deserializer<AccumulatorProcessor.Values>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public AccumulatorProcessor.Values deserialize(String topic, byte[] data) {
                Datastore.Values pbfValues = null;
                try {
                    pbfValues = Datastore.Values.parseFrom(data);
                } catch (InvalidProtocolBufferException ex) {
                    throw new RuntimeException("Unable to deserialize Key", ex);
                }

                AccumulatorProcessor.Values values = new AccumulatorProcessor.Values();
                final int numValues = pbfValues.getValuesCount();
                if (numValues > 0) {
                    values.values = new ArrayList<>(numValues);
                    for (int i = 0; i < numValues; i++) {
                        Datastore.Value pv = pbfValues.getValues(i);
                        String provider = null;
                        if (pv.hasProvider()) {
                            provider = pv.getProvider();
                        }
                        AccumulatorProcessor.Value v = new AccumulatorProcessor.Value(
                                pv.getTimeBucket().getIndex(),
                                pv.getDuration(),
                                pv.getCount(),
                                provider
                        );
                        values.values.add(i, v);
                    }
                }

                return values;
            }

            @Override
            public void close() {
            }
        };
    }
}
