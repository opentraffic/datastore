package io.opentraffic.datastore;

import org.apache.commons.cli.CommandLine;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * Created by matt on 31/05/17.
 */
public class AccumulatorSupplier implements ProcessorSupplier {
    public AccumulatorSupplier(CommandLine cli) {
        m_npriv = Long.parseLong(cli.getOptionValue("npriv", "3"));
    }

    @Override
    public Processor<String, Segment.Measurement> get() {
        return new AccumulatorProcessor(m_npriv);
    }

    private long m_npriv;
}
