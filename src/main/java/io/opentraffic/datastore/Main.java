package io.opentraffic.datastore;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.opentraffic.datastore.sink.FileSink;
import io.opentraffic.datastore.sink.FlatBufferSink;
import io.opentraffic.datastore.sink.ORCSink;
import io.opentraffic.datastore.sink.PrintSink;
import io.opentraffic.datastore.source.*;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by matt on 06/06/17.
 */
public class Main {

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) throws Exception {

        Options options = createOptions();
        CommandLineParser cliParser = new DefaultParser();
        CommandLine cmd = cliParser.parse( options, args);

        int vehicleTypeColumn = Integer.parseInt(cmd.getOptionValue("vehicle-type-column", "-1"));
        int segmentIdColumn = Integer.parseInt(cmd.getOptionValue("segment-id-column", "0"));
        int nextSegmentIdColumn = Integer.parseInt(cmd.getOptionValue("next-segment-id-column", "1"));
        int lengthColumn = Integer.parseInt(cmd.getOptionValue("length-column", "2"));
        int timestampColumn = Integer.parseInt(cmd.getOptionValue("timestamp-column", "3"));
        int durationColumn = Integer.parseInt(cmd.getOptionValue("duration-column", "4"));
        int countColumn = Integer.parseInt(cmd.getOptionValue("count-column", "5"));
        int providerColumn = Integer.parseInt(cmd.getOptionValue("provider-column", "-1"));
        String timestampFormat = cmd.getOptionValue("timestamp-format", DEFAULT_DATE_FORMAT);

        List<String> fileNames = cmd.getArgList();
        if (fileNames.isEmpty()) {
            throw new RuntimeException("No file names provided");
        }

        FileSystem fs = cmd.hasOption("s3") ? new S3FileSystem() : new LocalFileSystem();
        FileSource source = fs.createSource(fileNames);

        List<FileSink> sinkList = new LinkedList<>();
        if (cmd.hasOption("output-flatbuffers")) {
            final String fileName = cmd.getOptionValue("output-flatbuffers");
            sinkList.add(new FlatBufferSink(fs.createOutputStream(fileName)));
        }
        if (cmd.hasOption("output-orc")) {
            final String fileName = cmd.getOptionValue("output-orc");
            sinkList.add(new ORCSink(fs.createOutputStream(fileName)));
        }
        if (cmd.hasOption("verbose")) {
            sinkList.add(new PrintSink());
        }

        if (sinkList.isEmpty()) {
            throw new RuntimeException("No data sinks provided, so nothing to do!");
        }

        CSVFormat format = new CSVFormat(
                org.apache.commons.csv.CSVFormat.DEFAULT.withFirstRecordAsHeader(),
                new SimpleDateFormat(timestampFormat),
                vehicleTypeColumn, segmentIdColumn, nextSegmentIdColumn, lengthColumn,
                timestampColumn, durationColumn, countColumn, providerColumn);
        InputCSVParser parser = new InputCSVParser(source, format);

        MeasurementBuckets buckets = new MeasurementBuckets(parser);

        for (FileSink sink : sinkList) {
            try {
                sink.write(buckets.getMeasurements());
            } catch (IOException ex) {
                throw new RuntimeException("Error writing to sink", ex);
            }
        }
    }

    private interface FileSystem {
        FileSource createSource(List<String> fileNames);
        OutputStream createOutputStream(String fileName) throws IOException;
    }

    private static class S3FileSystem implements FileSystem {

        private final AmazonS3 m_s3;

        public S3FileSystem() {
            this.m_s3 = AmazonS3ClientBuilder.defaultClient();
        }

        @Override
        public FileSource createSource(List<String> fileNames) {
            return new S3FileSource(fileNames, this.m_s3);
        }

        @Override
        public OutputStream createOutputStream(String fileName) throws IOException {
            S3FileSource.S3Location location = S3FileSource.parse(fileName);
            // AWS SDK wants to read from an input stream, but the rest of the app wants
            // to write to an output stream. So this buffer exists here to write to S3
            // once the app closes the output stream.
            return new S3BufferedOutputStream(this.m_s3, location);
        }

        private static class S3BufferedOutputStream extends ByteArrayOutputStream {
            private final AmazonS3 m_s3;
            private final S3FileSource.S3Location m_location;

            public S3BufferedOutputStream(AmazonS3 s3, S3FileSource.S3Location location) {
                this.m_s3 = s3;
                this.m_location = location;
            }

            @Override
            public void close() throws IOException {
                // TODO: should this stay the default?
                ObjectMetadata metadata = new ObjectMetadata();
                this.m_s3.putObject(
                        this.m_location.bucket,
                        this.m_location.key,
                        new ByteArrayInputStream(buf, 0, count),
                        metadata);
                super.close();
            }
        }
    }

    private static class LocalFileSystem implements FileSystem {
        @Override
        public FileSource createSource(List<String> fileNames) {
            return new LocalFileSource(fileNames);
        }

        @Override
        public OutputStream createOutputStream(String fileName) throws IOException {
            File f = new File(fileName);
            FileOutputStream fos = new FileOutputStream(f);
            return fos;
        }
    }

    private static Options createOptions() {
        Options options = new Options();
        options.addOption(
                new Option("s3", "Files given are S3 URLs, e.g: s3://bucket/key. If not given, assume they are local files."));
        options.addOption(Option.builder("f")
                .argName("output-flatbuffers")
                .hasArg()
                .required(false)
                .desc("If present, the location to output a FlatBuffers file to.")
                .build());
        options.addOption(Option.builder("o")
                .argName("output-orc")
                .hasArg()
                .required(false)
                .desc("If present, the location to output an ORC file to.")
                .build());
        options.addOption(Option.builder()
                .argName("vehicle-type-column")
                .hasArg()
                .required(false)
                .type(Integer.class)
                .desc("Column number for vehicle type. Default is -1, which sets all rows to AUTO.")
                .build());
        options.addOption(Option.builder()
                .argName("segment-id-column")
                .hasArg()
                .required(false)
                .type(Integer.class)
                .desc("Column number for segment ID. Defaults to 0 (i.e: first column)")
                .build());
        options.addOption(Option.builder()
                .argName("next-segment-id-column")
                .hasArg()
                .required(false)
                .type(Integer.class)
                .desc("Column number for next segment ID. Default is -1, which sets all rows to the invalid next segment ID.")
                .build());
        options.addOption(Option.builder()
                .argName("segment-id-column")
                .hasArg()
                .required(false)
                .type(Integer.class)
                .desc("Column number for timestamp. Defaults to 1 (i.e: second column)")
                .build());
        options.addOption(Option.builder()
                .argName("duration-column")
                .hasArg()
                .required(false)
                .type(Integer.class)
                .desc("Column number for duration in seconds. Defaults to 2 (i.e: third column)")
                .build());
        options.addOption(Option.builder()
                .argName("count-column")
                .hasArg()
                .required(false)
                .type(Integer.class)
                .desc("Column number for count. Defaults to 3 (i.e: fourth column)")
                .build());
        options.addOption(Option.builder()
                .argName("provider-column")
                .hasArg()
                .required(false)
                .type(Integer.class)
                .desc("Column number for provider name. Default is -1, which sets all rows to a null provider.")
                .build());
        options.addOption(Option.builder()
                .argName("timestamp-format")
                .hasArg()
                .required(false)
                .desc("Format of the timestamp in the CSV. Defaults to \"" + DEFAULT_DATE_FORMAT + "\".")
                .build());
        return options;
    }
}
