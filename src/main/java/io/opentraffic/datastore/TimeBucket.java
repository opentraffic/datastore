package io.opentraffic.datastore;

/**
 * Represents a single bucket in time that measurements are put into.
 *
 * A bucket has both a size, which indicates the duration of the bucket and an
 * index indicating which period it is. At this point, only HOURLY buckets
 * exist. Indexes indicate which period this is relative to the UNIX epoch
 * (1970-01-01 00:00:00 +0000) in UTC.
 */
public class TimeBucket {
  public final BucketSize size;
  public final long index;

  public TimeBucket(BucketSize size, long index) {
    this.size = size;
    this.index = index;
  }
  
  public boolean intersects(long epochHour) {
    return index == epochHour;
  }
  
  public boolean intersects(long minEpochSeconds, long maxEpochSeconds) {
    long minBucket = minEpochSeconds/3600L;
    long maxBucket = maxEpochSeconds/3600L;
    return this.index >= minBucket && this.index <= maxBucket;
  }
}
