package io.opentraffic.datastore.sink;

import java.util.ArrayList;

import io.opentraffic.datastore.Measurement;

/**
 * Just prints the measurements to stdout. Useful for debugging.
 */
public class PrintSink {
    public static void write(ArrayList<Measurement> measurements) {
        for (Measurement m : measurements) {
            System.out.println(m);
        }
    }
}
