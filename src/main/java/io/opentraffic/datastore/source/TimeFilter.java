package io.opentraffic.datastore.source;

import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;

import java.util.Iterator;

/**
 * Wrapper around a source of Measurements which filters by time. This is useful when the
 * source data might overlap multiple time buckets, but the output file should only contain
 * measurements from a single one.
 */
public class TimeFilter implements Iterable<Measurement> {

    private final TimeBucket m_bucket;
    private final Iterable<Measurement> m_wrapped;

    public TimeFilter(TimeBucket bucket, Iterable<Measurement> wrapped) {
        this.m_bucket = bucket;
        this.m_wrapped = wrapped;
    }

    @Override
    public Iterator<Measurement> iterator() {
        return new FilterIterator(this.m_bucket, this.m_wrapped.iterator());
    }

    private static final class FilterIterator implements Iterator<Measurement> {

        private final TimeBucket m_bucket;
        private final Iterator<Measurement> m_wrapped;
        private Measurement m_next;

        FilterIterator(TimeBucket bucket, Iterator<Measurement> wrapped) {
            this.m_bucket = bucket;
            this.m_wrapped = wrapped;
            this.m_next = null;
            findNext();
        }

        private void findNext() {
            Measurement found = null;
            while (this.m_wrapped.hasNext()) {
                Measurement m = this.m_wrapped.next();
                if (m.timeBucket.equals(this.m_bucket)) {
                    found = m;
                    break;
                }
            }
            this.m_next = found;
        }

        @Override
        public boolean hasNext() {
            return this.m_next != null;
        }

        @Override
        public Measurement next() {
            Measurement m = this.m_next;
            findNext();
            return m;
        }
    }
}
