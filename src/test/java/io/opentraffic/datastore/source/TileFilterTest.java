package io.opentraffic.datastore.source;

import static org.junit.Assert.assertEquals;

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

public class TileFilterTest {
  @Test
  public void testTileFilter() throws ParseException, IOException {
    final long segment_id = 13L;
    final long tile_id = (1234L << 3L) | 1L;
    final long segment_in_tile = (segment_id << 25) | tile_id;
    final long segment_not_in_tile = segment_in_tile + (1L << 3L);

    StringBuilder measurements = new StringBuilder();
    measurements.append("segment_id,next_segment_id,duration,count,length,queue_length,minimum_timestamp,maximum_timestamp,source,vehicle_type\n");
    measurements.append(Long.toString(segment_in_tile) + ",1,1,1,1,1,1,1,foo,AUTO\n");
    measurements.append(Long.toString(segment_not_in_tile) + ",1,1,1,1,1,1,1,foo,AUTO\n");

    Options options = new Options();
    MeasurementSource.AddOptions(options);
    CommandLineParser cliParser = new DefaultParser();
    CommandLine cmd = cliParser.parse(options, new String[0]);
    
    MeasurementSource parser = new MeasurementSource(cmd, measurements.toString(), new TimeBucket(BucketSize.HOURLY, 0), tile_id);
    int count = 0;
    for (Measurement m : parser) {
      assertEquals(segment_in_tile, m.segmentId);
      count++;
    }
    parser.close();
    assertEquals(1, count);
  }
}
