package io.opentraffic.datastore;

/**
 * Represents a single bucket in time that measurements are put
 * into.
 *
 * A bucket has both a size, which indicates the duration of the
 * bucket and an index indicating which period it is. At this
 * point, only HOURLY buckets exist. Indexes indicate which period
 * this is relative to the UNIX epoch (1970-01-01 00:00:00 +0000)
 * in UTC.
 */
public class TimeBucket implements Comparable<TimeBucket> {
    public final BucketSize size;
    public final long index;

    public TimeBucket(BucketSize size, long index) {
        this.size = size;
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeBucket that = (TimeBucket) o;

        if (index != that.index) return false;
        return size == that.size;
    }

    @Override
    public int hashCode() {
        int result = size != null ? size.hashCode() : 0;
        result = 31 * result + (int) (index ^ (index >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TimeBucket{" +
                "size=" + size +
                ", index=" + index +
                '}';
    }

    @Override
    public int compareTo(TimeBucket other) {
        // Hourly buckets are the only kind implemented so far, but
        // other values may be possible in the future.
        assert this.size == BucketSize.HOURLY;
        assert other.size == BucketSize.HOURLY;

        return Long.compare(this.index, other.index);
    }
}
