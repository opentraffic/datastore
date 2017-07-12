package io.opentraffic.datastore.source;

import java.io.Closeable;

import io.opentraffic.datastore.Measurement;

public abstract class Source implements Iterable<Measurement>, Closeable{

}
