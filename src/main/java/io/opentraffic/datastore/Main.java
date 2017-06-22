package io.opentraffic.datastore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import io.opentraffic.datastore.sink.FlatBufferSink;
import io.opentraffic.datastore.sink.ORCSink;
import io.opentraffic.datastore.sink.PrintSink;
import io.opentraffic.datastore.source.MeasurementParser;

/**
 * Created by matt on 06/06/17.
 */
public class Main {
  
  private final static Logger logger = Logger.getLogger(Main.class);

  public static void main(String[] args) throws Exception {

    // parse and validate program options
    Options options = createOptions();
    CommandLineParser cliParser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = cliParser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      HelpFormatter help = new HelpFormatter();
      help.printHelp("datastore-histogram-tile-writer", options);
      System.exit(1);
    }
    List<String> fileNames = cmd.getArgList();
    if (fileNames.isEmpty()) {
      throw new RuntimeException("No file names provided");
    }
    if (!cmd.hasOption("output-flatbuffers") && !cmd.hasOption("output-orc") && !cmd.hasOption("verbose")) {
      throw new RuntimeException("No data sinks provided, so nothing to do!");
    }
    TimeBucket timeBucket = new TimeBucket(BucketSize.HOURLY, Long.parseLong(cmd.getOptionValue("time-bucket")));
    long tileId = Long.parseLong(cmd.getOptionValue("tile"));

    // parse all the input into measurement buckets
    TreeMap<Measurement.Key, Measurement> measurements = new TreeMap<>();
    for (String fileName : fileNames) {
      try {
        File file = new File(fileName);
        MeasurementParser parser = new MeasurementParser(cmd, file, timeBucket, tileId);
        for (Measurement m : parser) {
          if(!measurements.containsKey(m.key))
            measurements.put(m.key, m);
          else
            measurements.get(m.key).combine(m);
        }
        parser.close();
      } catch (Exception e) {
        logger.error("Failed to parse measurements from file: " + fileName);
      }
    }
    ArrayList<Measurement> sorted = new ArrayList<Measurement>(measurements.values());

    // flatbuffer
    final String fbFile = cmd.getOptionValue("output-flatbuffers");
    if (fbFile != null) {
      try {
        File f = new File(fbFile);
        FileOutputStream fos = new FileOutputStream(f);
        FlatBufferSink.write(sorted, fos, timeBucket);
      } catch (IOException ex) {
        throw new RuntimeException("Error writing to sink", ex);
      }
    }

    // orc
    final String orcFile = cmd.getOptionValue("output-orc");
    if (orcFile != null) {
      try {
        File f = new File(orcFile);
        FileOutputStream fos = new FileOutputStream(f);
        ORCSink.write(sorted, fos, timeBucket);
      } catch (IOException ex) {
        throw new RuntimeException("Error writing to sink", ex);
      }
    }

    // stdout
    if (cmd.hasOption("verbose")) {
      PrintSink.write(sorted);
    }
  }

  public static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder("b").longOpt("time-bucket").hasArg().required(true).type(Long.class)
        .desc("The timebucket to target when creating this tile. This is which sequential hour starting from the epoch").build());
    options.addOption(Option.builder("t").longOpt("tile").hasArg().required(true).type(Long.class)
        .desc("The tile and level to target when creating this tile. Note that the level is the first 3 bits followed by the tile "
            + "index which is the next 22 bits").build());
    
    options.addOption(Option.builder("f").longOpt("output-flatbuffers").hasArg().required(false)
        .desc("If present, the location to output a FlatBuffers file to. "
            + "If the file already exists it will merged with into the rest of the output").build());
    options.addOption(Option.builder("o").longOpt("output-orc").hasArg().required(false)
        .desc("If present, the location to output an ORC file to. "
            + "If the file already exists it will merged with into the rest of the output").build());
    options.addOption(Option.builder("v").longOpt("verbose").required(false)
        .desc("If present, the textual representation of the histogram will be written to stdout.").build());

    MeasurementParser.AddOptions(options);

    return options;
  }
}
