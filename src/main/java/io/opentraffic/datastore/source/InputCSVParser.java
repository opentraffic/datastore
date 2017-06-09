    package io.opentraffic.datastore.source;

    import io.opentraffic.datastore.*;
    import org.apache.commons.csv.CSVParser;
    import org.apache.commons.csv.CSVRecord;

    import java.io.*;
    import java.text.ParseException;
    import java.text.SimpleDateFormat;
    import java.util.Date;
    import java.util.Iterator;
    import java.util.NoSuchElementException;

    /**
     * Created by matt on 06/06/17.
     */
    public class InputCSVParser implements Iterable<Measurement> {
        private final FileSource m_source;
        private final CSVFormat m_format;

        public InputCSVParser(FileSource source, CSVFormat format) {
            this.m_source = source;
            this.m_format = format;
        }

        @Override
        public Iterator<Measurement> iterator() {
            return new InputCSVParserIterator(this.m_source.iterator(), this.m_format);
        }

        private static class ParseIterator implements Iterator<Measurement>, Closeable {
            private final CSVParser m_parser;
            private final Iterator<CSVRecord> m_iterator;
            private final CSVFormat m_format;

            public ParseIterator(InputStream input, CSVFormat format) {
                Reader reader = new InputStreamReader(input);
                try {
                    m_parser = new CSVParser(reader, format.format);
                } catch (IOException ex) {
                    throw new RuntimeException("Unable to parse CSV", ex);
                }
                this.m_iterator = m_parser.iterator();
                this.m_format = format;
            }

            @Override
            public boolean hasNext() {
                return m_iterator.hasNext();
            }

            @Override
            public Measurement next() {
                CSVRecord record = this.m_iterator.next();
                VehicleType vtype = parseVehicleType(record, this.m_format.vehicleTypeColumn);
                long segmentId = parseLong(record, this.m_format.segmentIdColumn);
                long nextSegmentId = parseLongOptional(record, this.m_format.nextSegmentIdColumn, Measurement.INVALID_NEXT_SEGMENT_ID);
                int length = parseInt(record, this.m_format.lengthColumn);
                TimeBucket timeBucket = parseTimeBucket(record, this.m_format.timestampColumn, this.m_format.timestampFormat);
                int duration = parseInt(record, this.m_format.durationColumn);
                byte durationBucket = DurationBucket.quantise(duration);
                int count = parseInt(record, this.m_format.countColumn);
                String provider = parseStringOptional(record, this.m_format.providerColumn, null);
                return new Measurement(vtype, segmentId, nextSegmentId, length, timeBucket, durationBucket, count, provider);
            }

            private String parseStringOptional(CSVRecord record, int idx, String defaultValue) {
                if (idx < 0) {
                    return defaultValue;
                } else {
                    return record.get(idx);
                }
            }

            private TimeBucket parseTimeBucket(CSVRecord record, int idx, SimpleDateFormat dateFormat) {
                String timestamp = record.get(idx);
                try {
                    Date date = dateFormat.parse(timestamp);
                    long hour = date.getTime() / (1000L * 3600L);
                    return new TimeBucket(BucketSize.HOURLY, hour);
                } catch (ParseException e) {
                    throw new RuntimeException("Unable to parse timestamp", e);
                }
            }

            private int parseInt(CSVRecord record, int idx) {
                return Integer.parseInt(record.get(idx));
            }

            private long parseLongOptional(CSVRecord record, int idx, long defaultValue) {
                if (idx < 0) {
                    return defaultValue;
                } else {
                    return Long.parseLong(record.get(idx));
                }
            }

            private long parseLong(CSVRecord record, int idx) {
                return Long.parseLong(record.get(idx));
            }

            private VehicleType parseVehicleType(CSVRecord r, int idx) {
                if (idx < 0) {
                    return VehicleType.AUTO;
                } else {
                    return VehicleType.valueOf(r.get(idx));
                }
            }



            @Override
            public void close() throws IOException {
                this.m_parser.close();
            }
        }

        private static class InputCSVParserIterator implements Iterator<Measurement> {
            private final Iterator<InputStream> m_iterator;
            private CSVFormat m_format;
            private ParseIterator m_inner;
            private boolean m_nextValid;

            public InputCSVParserIterator(Iterator<InputStream> iterator, CSVFormat format) {
                this.m_iterator = iterator;
                this.m_format = format;
                this.m_inner = null;
                this.m_nextValid = false;
                findNext();
            }

            @Override
            public boolean hasNext() {
                return this.m_nextValid && (this.m_inner != null && this.m_inner.hasNext());
            }

            @Override
            public Measurement next() {
                findNext();
                if (!this.m_nextValid) {
                    throw new NoSuchElementException();
                }
                return this.m_inner.next();
            }

            private void findNext() {
                while (this.m_inner == null || !this.m_inner.hasNext()) {
                    if (!this.m_iterator.hasNext()) {
                        this.m_nextValid = false;
                        return;
                    }

                    if (this.m_inner != null) {
                        try {
                            this.m_inner.close();
                        } catch (IOException ex) {
                            throw new RuntimeException("Unable to close old iterator", ex);
                        }
                    }

                    this.m_inner = new ParseIterator(this.m_iterator.next(), this.m_format);
                }
                this.m_nextValid = true;
            }
        }
    }
