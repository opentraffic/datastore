package io.opentraffic.datastore.source;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Created by matt on 06/06/17.
 */
public class CSVFormat {
    public final org.apache.commons.csv.CSVFormat format;

    public final int segmentIdColumn;
    public final int nextSegmentIdColumn;
    public final int durationColumn;
    public final int countColumn;
    public final int lengthColumn;
    public final int queueLengthColumn;
    public final int minTimestampColumn;
    public final int maxTimestampColumn;
    public final int sourceColumn;
    public final int vehicleTypeColumn;
    
    public static void AddOptions(Options options) {
        options.addOption(Option.builder("i")
            .longOpt("segment-id-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for segment ID. Defaults to 0 (i.e: first column)")
            .build());
        options.addOption(Option.builder("n")
            .longOpt("next-segment-id-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for next segment ID. Default is -1, which sets all rows to the invalid next segment ID.")
            .build());
        options.addOption(Option.builder("d")
            .longOpt("duration-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for duration in seconds. Defaults to 1 (i.e: second column)")
            .build());
        options.addOption(Option.builder("c")
            .longOpt("count-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for count. Defaults to 2 (i.e: third column)")
            .build());
        options.addOption(Option.builder("l")
            .longOpt("length-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for the length. Defaults to 3 (i.e: fourth column)")
            .build());
        options.addOption(Option.builder("q")
            .longOpt("queue-length-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for the queue length. Defaults to 4 (i.e: fifth column)")
            .build());
        options.addOption(Option.builder("m")
            .longOpt("min-timestamp-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for the minimum timestamp. Defaults to 5 (i.e: sixth column)")
            .build());
        options.addOption(Option.builder("x")
            .longOpt("max-timestamp-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for the maximum timestamp. Defaults to 6 (i.e: seventh column)")
            .build());
  
        options.addOption(Option.builder("s")
            .longOpt("source-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for source name. Default is -1, which sets all rows to a null source.")
            .build());
        options.addOption(Option.builder("t")
            .longOpt("vehicle-type-column")
            .hasArg()
            .required(false)
            .type(Integer.class)
            .desc("Column number for vehicle type. Default is -1, which sets all rows to AUTO.")
            .build());
    }

    public CSVFormat(org.apache.commons.csv.CSVFormat format, CommandLine cmd) {
        this.format = format;
        segmentIdColumn = Integer.parseInt(cmd.getOptionValue("segment-id-column", "0"));
        nextSegmentIdColumn = Integer.parseInt(cmd.getOptionValue("next-segment-id-column", "1"));
        durationColumn = Integer.parseInt(cmd.getOptionValue("duration-column", "2"));
        countColumn = Integer.parseInt(cmd.getOptionValue("count-column", "3"));
        lengthColumn = Integer.parseInt(cmd.getOptionValue("length-column", "4"));
        queueLengthColumn = Integer.parseInt(cmd.getOptionValue("queue-length-column", "5"));
        minTimestampColumn = Integer.parseInt(cmd.getOptionValue("min-timestamp-column", "6"));
        maxTimestampColumn = Integer.parseInt(cmd.getOptionValue("max-timestamp-column", "7"));
        sourceColumn = Integer.parseInt(cmd.getOptionValue("source-column", "-1"));
        vehicleTypeColumn = Integer.parseInt(cmd.getOptionValue("vehicle-type-column", "-1"));
    }
}
