package io.opentraffic.datastore.source;

import java.text.SimpleDateFormat;

/**
 * Created by matt on 06/06/17.
 */
public class CSVFormat {
    public final org.apache.commons.csv.CSVFormat format;
    public final SimpleDateFormat timestampFormat;

    public final int vehicleTypeColumn;
    public final int segmentIdColumn;
    public final int nextSegmentIdColumn;
    public final int lengthColumn;
    public final int timestampColumn;
    public final int durationColumn;
    public final int countColumn;
    public final int providerColumn;

    public CSVFormat(org.apache.commons.csv.CSVFormat format, SimpleDateFormat timestampFormat, int vehicleTypeColumn, int segmentIdColumn, int nextSegmentIdColumn, int lengthColumn, int timestampColumn, int durationColumn, int countColumn, int providerColumn) {
        this.format = format;
        this.timestampFormat = timestampFormat;
        this.vehicleTypeColumn = vehicleTypeColumn;
        this.segmentIdColumn = segmentIdColumn;
        this.nextSegmentIdColumn = nextSegmentIdColumn;
        this.lengthColumn = lengthColumn;
        this.timestampColumn = timestampColumn;
        this.durationColumn = durationColumn;
        this.countColumn = countColumn;
        this.providerColumn = providerColumn;
    }
}
