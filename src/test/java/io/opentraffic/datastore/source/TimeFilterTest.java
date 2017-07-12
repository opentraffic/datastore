package io.opentraffic.datastore.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import io.opentraffic.datastore.BucketSize;
import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;

public class TimeFilterTest {
  @Test
  public void testTimeFilter() throws ParseException, IOException {
    StringBuilder measurements = new StringBuilder();
    measurements.append("segment_id,next_segment_id,duration,count,length,queue_length,minimum_timestamp,maximum_timestamp,source,vehicle_type\n");
    measurements.append(Long.toString(0L << 25L) + ",1,1,1,1,1,0,1,foo,AUTO\n");
    measurements.append(Long.toString(1L << 25L) + ",1,1,1,1,1,3600,3601,foo,AUTO\n");
    measurements.append(Long.toString(2L << 25L) + ",1,1,1,1,1,7200,7201,foo,AUTO\n");

    Options options = new Options();
    MeasurementSource.AddOptions(options);
    CommandLineParser cliParser = new DefaultParser();
    CommandLine cmd = cliParser.parse(options, new String[0]);
    
    MeasurementSource parser = new MeasurementSource(cmd, measurements.toString(), new TimeBucket(BucketSize.HOURLY, 1), 0);
    TimeBucket bucket = new TimeBucket(BucketSize.HOURLY, 1);
    int count = 0;
    for (Measurement m : parser) {
      assertTrue(bucket.intersects(m.minTimestamp, m.maxTimestamp));
      count++;
    }
    parser.close();
    assertEquals(1, count);
  }
}
