package io.opentraffic.datastore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.opentraffic.datastore.sink.FlatBufferSink;
import io.opentraffic.datastore.sink.ORCSink;
import io.opentraffic.datastore.sink.PrintSink;
import io.opentraffic.datastore.source.CSVFormat;
import io.opentraffic.datastore.source.FileSource;
import io.opentraffic.datastore.source.InputCSVParser;

/**
 * Created by matt on 06/06/17.
 */
public class Main {

    public static void main(String[] args) throws Exception {

        //parse and validate program options
        Options options = createOptions();
        CommandLineParser cliParser = new DefaultParser();        
        CommandLine cmd = null;
        try {
            cmd = cliParser.parse(options, args);
        }
        catch (ParseException e) {
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

        //parse all the input into measurement buckets
        CSVFormat format = new CSVFormat(org.apache.commons.csv.CSVFormat.DEFAULT.withFirstRecordAsHeader(), cmd);
        FileSource source = new FileSource(fileNames);
        InputCSVParser parser = new InputCSVParser(source, format);
        MeasurementBuckets buckets = new MeasurementBuckets(parser);

        //flatbuffer
        final String fbFile = cmd.getOptionValue("output-flatbuffers");
        if (fbFile != null) {
          try {
              File f = new File(fbFile);
              FileOutputStream fos = new FileOutputStream(f);
              FlatBufferSink.write(buckets.getMeasurements(), fos);
          } catch (IOException ex) {
              throw new RuntimeException("Error writing to sink", ex);
          }
        }
        
        //orc
        final String orcFile = cmd.getOptionValue("output-orc");
        if (orcFile != null) {
          try {
              File f = new File(orcFile);
              FileOutputStream fos = new FileOutputStream(f);
              ORCSink.write(buckets.getMeasurements(), fos);
          } catch (IOException ex) {
              throw new RuntimeException("Error writing to sink", ex);
          }
        }
        
        //stdout
        if (cmd.hasOption("verbose")) {
            PrintSink.write(buckets.getMeasurements());
        }
    }

    private static Options createOptions() {
        Options options = new Options();
        options.addOption(Option.builder("f")
                .longOpt("output-flatbuffers")
                .required(false)
                .desc("If present, the location to output a FlatBuffers file to. "
                    + "If the file already exists it will merged with into the rest of the output")
                .build());
        options.addOption(Option.builder("o")
                .longOpt("output-orc")
                .required(false)
                .desc("If present, the location to output an ORC file to. "
                    + "If the file already exists it will merged with into the rest of the output")
                .build());
        options.addOption(Option.builder("v")
                .longOpt("verbose")
                .required(false)
                .desc("If present, the textual representation of the histogram will be written to stdout.")
                .build());
        
        CSVFormat.AddOptions(options);

        return options;
    }
}
