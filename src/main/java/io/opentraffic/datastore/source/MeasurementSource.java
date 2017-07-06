package io.opentraffic.datastore.source;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import io.opentraffic.datastore.BucketSize;
import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;
import io.opentraffic.datastore.VehicleType;

/**
 * Created by matt on 06/06/17.
 */
public class MeasurementSource implements Iterable<Measurement>, Closeable {
  private final int segmentIdColumn;
  private final int nextSegmentIdColumn;
  private final int durationColumn;
  private final int countColumn;
  private final int lengthColumn;
  private final int queueLengthColumn;
  private final int minTimestampColumn;
  private final int maxTimestampColumn;
  private final int sourceColumn;
  private final int vehicleTypeColumn;
  private final TimeBucket bucket;
  private final long tile;
  private final CSVParser parser;

  public MeasurementSource(CommandLine cmd, File file, TimeBucket timeBucket, long tileId) throws IOException {
    bucket = timeBucket;
    tile = tileId;
    parser = CSVParser.parse(file, Charset.forName("UTF-8"),
        org.apache.commons.csv.CSVFormat.DEFAULT.withFirstRecordAsHeader());
    
    segmentIdColumn = Integer.parseInt(cmd.getOptionValue("segment-id-column", "0"));
    nextSegmentIdColumn = Integer.parseInt(cmd.getOptionValue("next-segment-id-column", "1"));
    durationColumn = Integer.parseInt(cmd.getOptionValue("duration-column", "2"));
    countColumn = Integer.parseInt(cmd.getOptionValue("count-column", "3"));
    lengthColumn = Integer.parseInt(cmd.getOptionValue("length-column", "4"));
    queueLengthColumn = Integer.parseInt(cmd.getOptionValue("queue-length-column", "5"));
    minTimestampColumn = Integer.parseInt(cmd.getOptionValue("min-timestamp-column", "6"));
    maxTimestampColumn = Integer.parseInt(cmd.getOptionValue("max-timestamp-column", "7"));
    sourceColumn = Integer.parseInt(cmd.getOptionValue("source-column", "8"));
    vehicleTypeColumn = Integer.parseInt(cmd.getOptionValue("mode-column", "9"));
  }
  
  public MeasurementSource(CommandLine cmd, String rawCSV, TimeBucket timeBucket, long tileId) throws IOException {
    bucket = timeBucket;
    tile = tileId;
    parser = CSVParser.parse(rawCSV,
        org.apache.commons.csv.CSVFormat.DEFAULT.withFirstRecordAsHeader());
    
    segmentIdColumn = Integer.parseInt(cmd.getOptionValue("segment-id-column", "0"));
    nextSegmentIdColumn = Integer.parseInt(cmd.getOptionValue("next-segment-id-column", "1"));
    durationColumn = Integer.parseInt(cmd.getOptionValue("duration-column", "2"));
    countColumn = Integer.parseInt(cmd.getOptionValue("count-column", "3"));
    lengthColumn = Integer.parseInt(cmd.getOptionValue("length-column", "4"));
    queueLengthColumn = Integer.parseInt(cmd.getOptionValue("queue-length-column", "5"));
    minTimestampColumn = Integer.parseInt(cmd.getOptionValue("min-timestamp-column", "6"));
    maxTimestampColumn = Integer.parseInt(cmd.getOptionValue("max-timestamp-column", "7"));
    sourceColumn = Integer.parseInt(cmd.getOptionValue("source-column", "8"));
    vehicleTypeColumn = Integer.parseInt(cmd.getOptionValue("mode-column", "9"));
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

  @Override
  public Iterator<Measurement> iterator() {
    return new Iterator<Measurement>() {
      private final Iterator<CSVRecord> itr = parser.iterator();
      private Measurement nextObject = findNext();

      @Override
      public boolean hasNext() {
        return nextObject != null;
      }

      @Override
      public Measurement next() {
        Measurement m = nextObject;
        nextObject = findNext();
        return m;
      }
    
      private Measurement parse(CSVRecord record) {
        long segmentId = parseLong(record, segmentIdColumn);
        long nextSegmentId = parseLongOptional(record, nextSegmentIdColumn, Measurement.INVALID_NEXT_SEGMENT_ID);
        int duration = parseInt(record, durationColumn);
        int count = parseInt(record, countColumn);
        int length = parseInt(record, lengthColumn);
        long minTimestamp = parseLong(record, minTimestampColumn);
        long maxTimestamp = parseLong(record, maxTimestampColumn);
        int queueLength = parseInt(record, queueLengthColumn);
        String source = parseStringOptional(record, sourceColumn, null);
        VehicleType vtype = parseVehicleType(record, vehicleTypeColumn);
        return new Measurement(vtype, segmentId, nextSegmentId, length, queueLength, duration, count,
            source, minTimestamp, maxTimestamp);
      }      
    
      private Measurement findNext() {
        while (itr.hasNext()) {
          Measurement m = parse(itr.next());
          if (m.intersects(tile, bucket)) {
            return m;
          }
        }
        return null;
      }
    };
  }

  private String parseStringOptional(CSVRecord record, int idx, String defaultValue) {
    if (idx < 0) {
      return defaultValue;
    } else {
      try {
        return record.get(idx);
      } catch (Exception e) {
        return defaultValue;
      }
    }
  }

  private TimeBucket parseTimeBucket(CSVRecord record, int idx) {
    try {
      long timestamp = parseLong(record, idx);
      Date date = new Date(timestamp * 1000L);
      long hour = date.getTime() / (1000L * 3600L);
      return new TimeBucket(BucketSize.HOURLY, hour);
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse timestamp", e);
    }
  }

  private int parseInt(CSVRecord record, int idx) {
    return Integer.parseInt(record.get(idx));
  }

  private long parseLongOptional(CSVRecord record, int idx, long defaultValue) {
    if (idx < 0) {
      return defaultValue;
    } else {
      try {
        return Long.parseLong(record.get(idx));
      } catch (Exception e) {
        return defaultValue;
      }
    }
  }

  private long parseLong(CSVRecord record, int idx) {
    return Long.parseLong(record.get(idx));
  }

  private VehicleType parseVehicleType(CSVRecord r, int idx) {
    if (idx < 0) {
      return VehicleType.AUTO;
    } else {
      return VehicleType.valueOf(r.get(idx));
    }
  }

  public static void AddOptions(Options options) {
    options.addOption(Option.builder("i").longOpt("segment-id-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for segment ID. Defaults to 0 (i.e: first column)").build());
    options.addOption(Option.builder("n").longOpt("next-segment-id-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for next segment ID. Default is -1, which sets all rows to the invalid next segment ID.")
        .build());
    options.addOption(Option.builder("d").longOpt("duration-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for duration in seconds. Defaults to 1 (i.e: second column)").build());
    options.addOption(Option.builder("c").longOpt("count-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for count. Defaults to 2 (i.e: third column)").build());
    options.addOption(Option.builder("l").longOpt("length-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for the length. Defaults to 3 (i.e: fourth column)").build());
    options.addOption(Option.builder("q").longOpt("queue-length-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for the queue length. Defaults to 4 (i.e: fifth column)").build());
    options.addOption(Option.builder("m").longOpt("min-timestamp-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for the minimum timestamp. Defaults to 5 (i.e: sixth column)").build());
    options.addOption(Option.builder("x").longOpt("max-timestamp-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for the maximum timestamp. Defaults to 6 (i.e: seventh column)").build());
    options.addOption(Option.builder("s").longOpt("source-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for source name. Default is -1, which sets all rows to a null source.").build());
    options.addOption(Option.builder("m").longOpt("mode-column").hasArg().required(false).type(Integer.class)
        .desc("Column number for mode of travel (vehicle type). Default is -1, which sets all rows to AUTO (automobile).").build());
  }

}
