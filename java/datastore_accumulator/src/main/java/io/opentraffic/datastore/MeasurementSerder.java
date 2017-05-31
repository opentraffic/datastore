package io.opentraffic.datastore;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by matt on 31/05/17.
 */
public class MeasurementSerder implements Serde<Segment.Measurement> {

    public MeasurementSerder() {
        m_serializer = new MeasurementSerializer();
        m_deserializer = new MeasurementDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Segment.Measurement> serializer() {
        return m_serializer;
    }

    @Override
    public Deserializer<Segment.Measurement> deserializer() {
        return m_deserializer;
    }

    private MeasurementSerializer m_serializer;
    private MeasurementDeserializer m_deserializer;
}
