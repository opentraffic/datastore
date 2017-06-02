package io.opentraffic.datastore;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opentraffic.datastore.internal.Datastore;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * Created by matt on 02/06/17.
 */
class KeySerde implements Serde<AccumulatorProcessor.Key> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<AccumulatorProcessor.Key> serializer() {
        return new Serializer<AccumulatorProcessor.Key>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, AccumulatorProcessor.Key data) {
                Datastore.Key.Builder builder = Datastore.Key.newBuilder();
                if (data.vtype != builder.getVtype()) {
                    builder.setVtype(data.vtype);
                }
                builder.setSegmentId(data.segment_id);
                if (data.next_segment_id != builder.getNextSegmentId()) {
                    builder.setNextSegmentId(data.next_segment_id);
                }
                builder.setLength(data.length);
                return builder.build().toByteArray();
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public Deserializer<AccumulatorProcessor.Key> deserializer() {
        return new Deserializer<AccumulatorProcessor.Key>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public AccumulatorProcessor.Key deserialize(String topic, byte[] data) {
                try {
                    Datastore.Key pbfKey = Datastore.Key.parseFrom(data);
                    return new AccumulatorProcessor.Key(
                            pbfKey.getVtype(),
                            pbfKey.getSegmentId(),
                            pbfKey.getNextSegmentId(),
                            pbfKey.getLength()
                    );
                } catch (InvalidProtocolBufferException ex) {
                    throw new RuntimeException("Unable to deserialize Key", ex);
                }
            }

            @Override
            public void close() {
            }
        };
    }
}
