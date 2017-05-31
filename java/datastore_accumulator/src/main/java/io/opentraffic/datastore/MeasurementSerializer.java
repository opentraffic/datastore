package io.opentraffic.datastore;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by matt on 31/05/17.
 */
public class MeasurementSerializer implements Serializer<Segment.Measurement> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Segment.Measurement data) {
        return data.toByteArray();
    }

    @Override
    public void close() {
    }
}
