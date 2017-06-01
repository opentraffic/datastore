package io.opentraffic.datastore;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Created by matt on 31/05/17.
 */
public class Accumulator {

    private static CommandLine parse(String[] args) {
        Option bootstrap = new Option("b", "bootstrap", true, "Kafka bootstrap server(s).");
        bootstrap.setRequired(true);
        Option input_topic = new Option("i", "input-topic", true, "Input topic (a.k.a. frontend topic) which the accumulator will read from.");
        input_topic.setRequired(true);
        Option output_topic = new Option("o", "output-topic", true, "Output topic on which the accumulator will write.");
        output_topic.setRequired(true);

        Option npriv = new Option("n", "npriv", true, "Number of measurements needed to ensure privacy.");
        npriv.setType(Long.class);
        npriv.setRequired(false);

        Option duration = new Option("d", "duration", true, "How many milliseconds to run the accumulator for before shutting down. If unspecified, run indefinitely. This can be useful for testing purposes.");
        duration.setRequired(false);
        duration.setType(Long.class);

        Option verbose = new Option("v", "verbose", false, "If this is set, then output a lot of debugging information.");
        verbose.setRequired(false);

        Options options = new Options();
        options.addOption(bootstrap);
        options.addOption(input_topic);
        options.addOption(output_topic);
        options.addOption(npriv);
        options.addOption(duration);
        options.addOption(verbose);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter help = new HelpFormatter();
        CommandLine cli = null;

        try {
            cli = parser.parse(options, args);
        } catch (ParseException ex) {
            System.out.println(ex.getLocalizedMessage());
            help.printHelp("Accumulator", options);
            System.exit(1);
        }

        return cli;
    }

    public static void main(String[] args) throws Exception {
        CommandLine cli = parse(args);

        Properties kafka_props = new Properties();
        kafka_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafka_props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reporter");
        kafka_props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cli.getOptionValue("bootstrap"));

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("InputSource", new StringDeserializer(), new MeasurementDeserializer(), cli.getOptionValue("input-topic"));
        builder.addProcessor("Accumulator", new AccumulatorSupplier(cli), "InputSource");
        builder.addStateStore(AccumulatorProcessor.createStore(), "Accumulator");
        builder.addSink("OutputSink", cli.getOptionValue("output-topic"), new StringSerializer(), new MeasurementSerializer(),"Accumulator");

        KafkaStreams streams = new KafkaStreams(builder, kafka_props);
        streams.start();

        long duration = Long.parseLong(cli.getOptionValue("duration", Long.toString(Long.MAX_VALUE)));
        Thread.sleep(duration);

        streams.close();
    }
}
