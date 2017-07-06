package io.opentraffic.datastore.source;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;

import io.opentraffic.datastore.DurationBucket;
import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;
import io.opentraffic.datastore.VehicleType;
import io.opentraffic.datastore.flatbuffer.Entry;
import io.opentraffic.datastore.flatbuffer.Histogram;
import io.opentraffic.datastore.flatbuffer.Segment;

public class FlatbufferSource implements Iterable<Measurement> {
  private final TimeBucket bucket;
  private final long tile;
  private final Histogram histogram;
  private int index;
  ArrayList<Measurement> measurements;
  
  FlatbufferSource(File file, TimeBucket timeBucket, long tileId) throws IOException {
    bucket = timeBucket;
    tile = tileId;
    histogram = Histogram.getRootAsHistogram(ByteBuffer.wrap(IOUtils.toByteArray(file.toURI())));
    index = 0;
    measurements = new ArrayList<Measurement>();
  }

  @Override
  public Iterator<Measurement> iterator() {
    return new Iterator<Measurement>() {

      @Override
      public boolean hasNext() {
        //go get more measurements (note that a segment can have zero entries, so it's best to loop here until data is available)
        while (index < histogram.segmentsLength() && measurements.size() == 0) {
          Segment segment = histogram.segments(index);
          index += 1;
          //convert segment into a Measurement object
          long segmentId = segment.segmentId();
          VehicleType vtype = VehicleType.values()[histogram.vehicleType()];
          for (int i = 0; i < segment.entriesLength(); i++) {
            Entry e = segment.entries(i);
            long nextSegmentId = segment.nextSegmentIds(e.nextSegmentIdx());
            int duration = DurationBucket.unquantise((byte)e.durationBucket());
            int count = (int)e.count();
            Measurement m = new Measurement(vtype, segmentId, nextSegmentId, 255, e.queue(), duration, count,
                null, e.epochHour() * 3600, (e.epochHour()  + 1) * 3600);
            measurements.add(m);
          }
        }
        //if we have more
        return measurements.size() > 0;
      }

      @Override
      public Measurement next() {
        //give back one
        return measurements.remove(0);
      }
      
    };
  }

}
