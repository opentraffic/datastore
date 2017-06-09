package io.opentraffic.datastore;

/**
 * Created by matt on 06/06/17.
 */
public final class Measurement implements Comparable<Measurement> {
    public static final long INVALID_NEXT_SEGMENT_ID = 0x3ffffffffffL;

    // Type of the vehicle which recorded this measurement
    public final VehicleType vehicleType;

    // Segment ID that the vehicle just finished traversing.
    public final long segmentId;

    // Segment ID that the vehicle turned onto, or
    // INVALID_NEXT_SEGMENT_ID if no next segment is known.
    public final long nextSegmentId;

    // Length, in metres, from the start of `segmentId` to the
    // start of `nextSegmentId`. Note that this may include some
    // stuff past the end of `segmentId`.
    public final int length;

    // Time bucket in which the measurement was taken.
    public final TimeBucket timeBucket;

    // Duration in seconds, quantised into a bucket. See DurationBucket.
    public final byte durationBucket;

    // Number of measurements sharing this set of parameters.
    public final int count;

    // Identifier of the provider (TNC) through which this
    // measurement was processed. Note that this might be null
    // for composite measurements - always check it before
    // using it.
    public final String provider;

    public Measurement(VehicleType vehicleType, long segmentId, long nextSegmentId, int length, TimeBucket timeBucket, byte durationBucket, int count, String provider) {
        this.vehicleType = vehicleType;
        this.segmentId = segmentId;
        this.nextSegmentId = nextSegmentId;
        this.length = length;
        this.timeBucket = timeBucket;
        this.durationBucket = durationBucket;
        this.count = count;
        this.provider = provider;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Measurement that = (Measurement) o;

        if (segmentId != that.segmentId) return false;
        if (nextSegmentId != that.nextSegmentId) return false;
        if (length != that.length) return false;
        if (durationBucket != that.durationBucket) return false;
        if (count != that.count) return false;
        if (vehicleType != that.vehicleType) return false;
        if (timeBucket != null ? !timeBucket.equals(that.timeBucket) : that.timeBucket != null) return false;
        return provider != null ? provider.equals(that.provider) : that.provider == null;
    }

    @Override
    public int hashCode() {
        int result = vehicleType != null ? vehicleType.hashCode() : 0;
        result = 31 * result + (int) (segmentId ^ (segmentId >>> 32));
        result = 31 * result + (int) (nextSegmentId ^ (nextSegmentId >>> 32));
        result = 31 * result + length;
        result = 31 * result + (timeBucket != null ? timeBucket.hashCode() : 0);
        result = 31 * result + (int)durationBucket;
        result = 31 * result + count;
        result = 31 * result + (provider != null ? provider.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String stringProvider = provider == null ? "null" : ("'" + provider + "'");
        return "Measurement{" +
                "vehicleType=" + vehicleType +
                ", segmentId=" + segmentId +
                ", nextSegmentId=" + nextSegmentId +
                ", length=" + length +
                ", timeBucket=" + timeBucket +
                ", durationBucket=" + ((int)durationBucket) +
                ", count=" + count +
                ", provider=" + stringProvider +
                '}';
    }

    @Override
    public int compareTo(Measurement other) {
        int cmp = 0;

        cmp = this.vehicleType.compareTo(other.vehicleType);
        if (cmp != 0) { return cmp; }

        cmp = Long.compare(this.segmentId, other.segmentId);
        if (cmp != 0) { return cmp; }

        cmp = this.timeBucket.compareTo(other.timeBucket);
        if (cmp != 0) { return cmp; }

        cmp = Long.compare(this.nextSegmentId, other.nextSegmentId);
        if (cmp != 0) { return cmp; }

        cmp = Integer.compare(this.length, other.length);
        if (cmp != 0) { return cmp; }

        cmp = Integer.compare(this.durationBucket, other.durationBucket);
        if (cmp != 0) { return cmp; }

        cmp = Integer.compare(this.count, other.count);
        if (cmp != 0) { return cmp; }

        if (this.provider == null) {
            if (other.provider == null) {
                cmp = 0;
            } else {
                cmp = -1;
            }
        } else {
            if (other.provider == null) {
                cmp = 1;
            } else {
                cmp = this.provider.compareTo(other.provider);
            }
        }

        return cmp;
    }
}
