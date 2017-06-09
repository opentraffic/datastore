package io.opentraffic.datastore.sink;

import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.sink.FileSink;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Just prints the measurements to stdout. Useful for debugging.
 */
public class PrintSink implements FileSink {
    @Override
    public void write(ArrayList<Measurement> measurements) {
        for (Measurement m : measurements) {
            System.out.println(m);
        }
    }
}
