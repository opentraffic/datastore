package io.opentraffic.datastore;

/**
 * Size of time buckets supported by the histogram tile writer.
 *
 * Currently only HOURLY, i.e: every 60 minutes. However, this
 * may change in the future.
 */
public enum BucketSize {
    HOURLY;
}
