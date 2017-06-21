package io.opentraffic.datastore.sink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.PhysicalFsWriter;

import io.opentraffic.datastore.BucketSize;
import io.opentraffic.datastore.DurationBucket;
import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;

/**
 * Created by matt on 08/06/17.
 */
public class ORCSink {

    public static final String SCHEMA_DEF = "struct<" +
            "vtype:tinyint," +
            "segment_id:int," +
            "day_hour:tinyint," +
            "next_segment_id:int," +
            "duration:int," +
            "count:int" +
            ">";

    public static void write(ArrayList<Measurement> measurements, OutputStream output) throws IOException {

        Path path = new Path("foo");
        Configuration conf = new Configuration();
        FileSystem fs = new SingleOutputFileSystem(output);

        TypeDescription schema = TypeDescription.fromString(SCHEMA_DEF);

        OrcFile.WriterOptions writerOptions =
                OrcFile.writerOptions(conf)
                .setSchema(schema);
        writerOptions.physicalWriter(new PhysicalFsWriter(fs, path, writerOptions));

        Writer writer = OrcFile.createWriter(path, writerOptions);

        VectorizedRowBatch batch = schema.createRowBatch();

        LongColumnVector vType = (LongColumnVector)batch.cols[0];
        LongColumnVector segmentId = (LongColumnVector)batch.cols[1];
        LongColumnVector dayHour = (LongColumnVector)batch.cols[2];
        LongColumnVector nextSegmentId = (LongColumnVector)batch.cols[3];
        LongColumnVector duration = (LongColumnVector)batch.cols[4];
        LongColumnVector count = (LongColumnVector)batch.cols[5];

        for (Measurement m : measurements) {
            int row = batch.size++;

            vType.vector[row] = m.vehicleType.ordinal();
            segmentId.vector[row] = m.segmentId;
            dayHour.vector[row] = convertTimeBucketToWeek(m.timeBucket);
            nextSegmentId.vector[row] = m.nextSegmentId;
            duration.vector[row] = DurationBucket.unquantise(m.durationBucket);
            count.vector[row] = m.count;

            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }

        // this should end up calling close() on fos, so we don't have to.
        writer.close();
    }

    private static int convertTimeBucketToWeek(TimeBucket timeBucket) {
        assert(timeBucket.size == BucketSize.HOURLY);
        // guaranteed the result will be smaller than 24*7, so narrowing conversion to
        // int is not a problem.
        return (int)((timeBucket.index - 96L) % (24L * 7L));
    }

    private static class SingleOutputFileSystem extends FileSystem {

        private final OutputStream m_output;

        public SingleOutputFileSystem(OutputStream output) {
            this.m_output = output;
        }

        @Override
        public URI getUri() {
            URI uri = null;
            try {
                new URI("memfs:///");
            } catch (URISyntaxException ex) {
                throw new RuntimeException("Failed to construct SingleOutputFileSystem URI", ex);
            }
            return uri;
        }

        @Override
        public FSDataInputStream open(Path path, int bufferSize) throws IOException {
            throw new RuntimeException("Can't read from SingleOutputFileSystem");
        }

        @Override
        public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
            return new FSDataOutputStream(this.m_output, null);
        }

        @Override
        public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
            return new FSDataOutputStream(this.m_output, null);
        }

        @Override
        public boolean rename(Path path, Path path1) throws IOException {
            throw new RuntimeException("Can't rename files in SingleOutputFileSystem");
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            throw new RuntimeException("Can't delete files in SingleOutputFileSystem");
        }

        @Override
        public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
            throw new RuntimeException("Can't get status of files in SingleOutputFileSystem");
        }

        @Override
        public void setWorkingDirectory(Path path) {
            throw new RuntimeException("Cannot change directory in SingleOutputFileSystem");
        }

        @Override
        public Path getWorkingDirectory() {
            return new Path(getUri());
        }

        @Override
        public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
            return true;
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            throw new RuntimeException("Can't get status of file in SingleOutputFileSystem");
        }
    }
}
