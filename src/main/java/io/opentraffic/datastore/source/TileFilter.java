package io.opentraffic.datastore.source;

import java.util.Iterator;

import io.opentraffic.datastore.Measurement;

/**
 * Wrapper around a source of Measurements which filters by tile ID. This is useful if the
 * source might span multiple tiles but the output should contain only one.
 */
public class TileFilter implements Iterable<Measurement> {

    private final long m_tile_id;
    private final Iterable<Measurement> m_wrapped;

    public TileFilter(long tile_id, Iterable<Measurement> wrapped) {
        this.m_tile_id = tile_id;
        this.m_wrapped = wrapped;
    }

    @Override
    public Iterator<Measurement> iterator() {
        return new FilterIterator(this.m_tile_id, this.m_wrapped.iterator());
    }

    private static final class FilterIterator implements Iterator<Measurement> {

        // Constants which are part of Valhalla's GraphId structure. This is a packed
        // 64-bit integer using bit fields, and so takes some masking and shifting to
        // unpack.
        private static final long LEVEL_AND_TILEID_MASK = 0x1FFFFFFL;
        private static final long LEVEL_AND_TILEID_BITS = 25L;
        private static final long SEGMENTID_MASK = 0x1FFFFFL;

        private final long m_tile_id;
        private final Iterator<Measurement> m_wrapped;
        private Measurement m_next;

        FilterIterator(long tile_id, Iterator<Measurement> wrapped) {
            this.m_tile_id = tile_id;
            this.m_wrapped = wrapped;
            this.m_next = null;
            findNext();
        }

        private void findNext() {
            Measurement found = null;
            while (this.m_wrapped.hasNext()) {
                Measurement m = this.m_wrapped.next();
                if (segmentInTile(m.segmentId)) {
                    // replace the segment's GraphId with a tile-local segment ID which
                    // is 0-based and suitable for indexing into arrays.
                    final long newSegmentId =
                            (m.segmentId >> LEVEL_AND_TILEID_BITS) & SEGMENTID_MASK;
                    found = new Measurement(m.vehicleType, newSegmentId,
                            m.nextSegmentId, m.length, m.queueLength, m.timeBucket, m.durationBucket,
                            m.count, m.source);
                    break;
                }
            }
            this.m_next = found;
        }

        private boolean segmentInTile(long segmentId) {
            return (segmentId & LEVEL_AND_TILEID_MASK) == this.m_tile_id;
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
