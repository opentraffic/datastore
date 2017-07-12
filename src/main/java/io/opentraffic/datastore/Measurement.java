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

  // Ratio of the segments entire length
  public float queue;

  // Duration in seconds
  public int duration;

  // Number of measurements sharing this set of parameters.
  public int count;

  // Identifier of the source (TNC) through which this
  // measurement was processed. Note that this might be null
  // for composite measurements - always check it before
  // using it.
  public String source;
  
  // The min and max timestamps with this measurement
  // this can be used reject data from a time bucket
  // that isnt the time bucket we are interested in creating
  public long minTimestamp;
  public long maxTimestamp;

  public Measurement(VehicleType vehicleType, long segmentId, long nextSegmentId, int length, 
      int queueLength, int duration, int count, String provider, long min, long max) {
    this.vehicleType = vehicleType;
    this.segmentId = segmentId;
    this.nextSegmentId = nextSegmentId;
    this.queue = (float)queueLength / (float)length;
    this.duration = duration;
    this.count = count;
    this.source = provider;
    this.minTimestamp = min;
    this.maxTimestamp = max;
  }
  
  public void combine(Measurement m) {
    double a = count / (double)(count + m.count);
    double b = m.count / (double)(count + m.count);
    queue = (int)Math.round(queue * a + m.queue * b);
    duration = (int)Math.round(duration * a + m.duration * b);
    count += m.count;
    if(m.source != null) {
      if(source == null)
        source = m.source;    
      else if(!source.contains(m.source))
        source += "|" + m.source;
    }
    minTimestamp = Math.min(minTimestamp, m.minTimestamp);
    maxTimestamp = Math.max(maxTimestamp, m.maxTimestamp);
  }
  
  /*segment ids are defined as little endian:
   struct Fields {
     uint64_t level  : 3;   // Hierarchy level
     uint64_t tileid : 22;  // Tile Id within the hierarchy level
     uint64_t id     : 21;  // Id of the element within the tile
     uint64_t spare  : 18;
   } fields;
   */
  private static final long LEVEL_AND_TILEID_MASK = 0x1FFFFFFL;
  private static final long LEVEL_AND_TILEID_BITS = 25L;
  private static final long SEGMENTID_MASK = 0x1FFFFFL;
  public boolean intersects(long tile, TimeBucket bucket) {
    return (segmentId & LEVEL_AND_TILEID_MASK) == tile && bucket.intersects(minTimestamp, maxTimestamp);
  }
  
  public long getTileRelative() {
    return (segmentId >> LEVEL_AND_TILEID_BITS) & SEGMENTID_MASK;
  }
  
  public long getTile() {
    return segmentId & LEVEL_AND_TILEID_MASK;
  }

  @Override
  public String toString() {
    String stringProvider = source == null ? "null" : ("'" + source + "'");
    return "Measurement{" + "vehicleType=" + vehicleType + ", segmentIndex=" + getTileRelative() + ", segmentId=" + segmentId + ", nextSegmentId="
        + nextSegmentId + ", queue=" + queue + ", duration=" + duration + ", count=" + count + ", provider=" + stringProvider + '}';
  }

  @Override
  public int compareTo(Measurement o) {
    int cmp = Long.compare(segmentId, o.segmentId);
    if(cmp != 0) return cmp;
    cmp = Long.compare(nextSegmentId, o.nextSegmentId);
    if(cmp != 0) return cmp;
    return duration - o.duration;
  }
}
