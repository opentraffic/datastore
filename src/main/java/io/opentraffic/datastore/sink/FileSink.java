package io.opentraffic.datastore.sink;

import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.MeasurementBuckets;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by matt on 06/06/17.
 */
public interface FileSink {
    // write a _sorted_ set of measurements to the sink
    void write(ArrayList<Measurement> measurements) throws IOException;
}
