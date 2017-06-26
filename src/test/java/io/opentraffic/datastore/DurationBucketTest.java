package io.opentraffic.datastore;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Created by matt on 06/06/17.
 */
public class DurationBucketTest {

  @Test
  public void testSpeedBucketZeroIsZero() {
    byte b = DurationBucket.quantise(0);
    assertEquals(0, b);
    int i = DurationBucket.unquantise((byte) 0);
    assertEquals(0, i);
  }

  @Test
  public void testSpeedBucketFirstHighBit() {
    byte b = DurationBucket.quantise(64);
    assertEquals(64, b);
    int i = DurationBucket.unquantise((byte) 64);
    assertEquals(64, i);
  }

  @Test
  public void testSpeedBucketSecondHighBit() {
    byte b = DurationBucket.quantise(192);
    assertEquals(-128, b);
    int i = DurationBucket.unquantise((byte) -128);
    assertEquals(192, i);
  }

  @Test
  public void testSpeedBucket() {
    for (int i = 64; i < 256; ++i) {
      byte b = (i > 127) ? ((byte) (256 - i)) : ((byte) i);
      int s = DurationBucket.unquantise(b);
      byte b2 = DurationBucket.quantise(s);
      assertEquals(b, b2);
    }
  }
}
