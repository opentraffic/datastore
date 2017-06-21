package io.opentraffic.datastore;

/**
 * Bucket durations into a byte by using a floating-point format
 * with the 2 high bits indicating a step size and the lower 6 bits
 * indicating the number of steps.
 */
public class DurationBucket {
    // the step sizes, which increase as the high 2 bits increase to
    // provide variable precision.
    private static int[] STEP_SIZES = {1, 2, 5, 10};

    // offset of each step, derived from the above.
    // STEP_OFFSET[i] = STEP_OFFSET[i-1] + 2^6 * STEP_SIZES[i-1]
    private static int[] STEP_OFFSETS = {0, 64, 192, 512, 1152};

    public static byte quantise(int duration) {
        // negative durations shouldn't happen, but if they do then assume
        // they're the same thing as zero.
        if (duration < 0) {
            return 0;
        }

        for (int i = 0; i < STEP_SIZES.length; i++) {
            if (duration < STEP_OFFSETS[i+1]) {
                return mergeByte(i, (duration - STEP_OFFSETS[i]) / STEP_SIZES[i]);
            }
        }

        // off the end of the array, so clamp to max measurement.
        return mergeByte(3, 63);
    }

    private static byte mergeByte(int hi, int lo) {
        assert(hi >= 0); assert(hi < 4);
        assert(lo >= 0); assert(lo < 64);

        int value = (hi << 6) | lo;
        if (value > 127) {
            return ((byte)(256 - value));
        } else {
            return ((byte)value);
        }
    }

    public static int unquantise(byte val) {
        int hi = 0, lo = 0;
        if (val < 0) {
            hi = 2 | (((-val) & 64) >> 6);
            lo = (-val) & 63;
        } else {
            hi = (val & 192) >> 6;
            lo = val & 63;
        }

        assert(hi >= 0); assert(hi < 4);
        assert(lo >= 0); assert(lo < 64);

        return STEP_OFFSETS[hi] + STEP_SIZES[hi] * lo;
    }
}
