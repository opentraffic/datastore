package io.opentraffic.datastore.source;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;

/**
 * FileSource abstracts over input files.
 */
public interface FileSource extends Iterable<InputStream> {
}
