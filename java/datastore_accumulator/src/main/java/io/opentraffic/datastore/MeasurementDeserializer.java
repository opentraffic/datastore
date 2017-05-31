package io.opentraffic.datastore;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import io.opentraffic.datastore.Segment.Measurement;

import java.util.Map;

/**
 * Parses a measurement from bytes to a class, implementing the interface which
 * Kafka wants for this.
 */
public class MeasurementDeserializer implements Deserializer<Measurement> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Measurement deserialize(String topic, byte[] data) {
        try {
            return Measurement.parseFrom(data);
        } catch (InvalidProtocolBufferException ex) {
            // TODO: figure out what is the appropriate course of action here - should it re-throw as something else, or return null?
            throw new RuntimeException("Failed to deserialize Measurement: ", ex);
        }
    }

    @Override
    public void close() {
    }
}
