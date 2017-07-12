package io.opentraffic.datastore.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import io.opentraffic.datastore.BucketSize;
import io.opentraffic.datastore.Measurement;
import io.opentraffic.datastore.TimeBucket;
import io.opentraffic.datastore.VehicleType;

/**
 * Round-trip ORC files to check that the serialisation and deserialisation is
 * working correctly.
 */
public class ORCSinkTest {
  @Test
  public void testORCSink() {
    TimeBucket timeBucket = new TimeBucket(BucketSize.HOURLY, 415787);
    ArrayList<Measurement> measurements = new ArrayList<>();

    measurements.add(new Measurement(VehicleType.AUTO, 3L << 25L, 1L, 255, 50, 60, 2, null, 0, 1));

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      ORCSink.write(measurements, output, timeBucket);
    } catch (IOException ex) {
      fail("Failed to write to sink: " + ex.getLocalizedMessage());
    }

    // check that an ORC file was written, and has the ORC header.
    assertTrue(output.size() >= 3);
    final byte[] bytes = output.toByteArray();
    assertEquals('O', bytes[0]);
    assertEquals('R', bytes[1]);
    assertEquals('C', bytes[2]);

    Path path = new Path("foo");
    Configuration conf = new Configuration();
    FileSystem fs = new InMemReadFileSystem(conf, bytes);

    TypeDescription schema = TypeDescription.fromString(ORCSink.SCHEMA_DEF);

    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf).filesystem(fs);

    Reader reader = null;
    try {
      reader = OrcFile.createReader(path, options);

      assertEquals(schema, reader.getSchema());
      VectorizedRowBatch batch = schema.createRowBatch();
      RecordReader rows = reader.rows();

      assertTrue(rows.nextBatch(batch));
      assertEquals(1, batch.size);

      LongColumnVector vType = (LongColumnVector) batch.cols[0];      
      LongColumnVector segmentId = (LongColumnVector) batch.cols[1];
      LongColumnVector epochHour = (LongColumnVector) batch.cols[2];
      LongColumnVector nextSegmentId = (LongColumnVector) batch.cols[3];
      LongColumnVector duration = (LongColumnVector) batch.cols[4];
      LongColumnVector queue = (LongColumnVector) batch.cols[5];
      LongColumnVector count = (LongColumnVector) batch.cols[6];

      assertEquals(VehicleType.AUTO, VehicleType.values()[(int) vType.vector[0]]);
      assertEquals(3L, segmentId.vector[0]);
      assertEquals(415787L, epochHour.vector[0]);
      assertEquals(1L, nextSegmentId.vector[0]);
      assertEquals(60, duration.vector[0]);
      assertEquals(2, count.vector[0]);

    } catch (IOException e) {
      fail(e.getLocalizedMessage());
    }
  }

  private static class InMemReadFileSystem extends FileSystem {

    private final byte[] m_data;

    public InMemReadFileSystem(Configuration conf, byte[] data) {
      setConf(conf);
      this.m_data = data;
    }

    @Override
    public URI getUri() {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
      SeekableByteArrayInputStream sbais = new SeekableByteArrayInputStream(this.m_data);
      return new FSDataInputStream(sbais);
    }

    private static class SeekableByteArrayInputStream extends ByteArrayInputStream
        implements Seekable, PositionedReadable {

      public SeekableByteArrayInputStream(byte[] data) {
        super(data);
      }

      @Override
      public void seek(long pos) throws IOException {
        reset();
        skip(pos);
      }

      @Override
      public long getPos() throws IOException {
        return pos;
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
      }

      @Override
      public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        final long remaining = buf.length - position;
        assert remaining < Integer.MAX_VALUE;
        final int readable = Integer.min((int) remaining, Integer.min(length, buffer.length - offset));
        for (int i = 0; i < readable; i++) {
          buffer[offset + i] = buf[(int) position + i];
        }
        return readable;
      }

      @Override
      public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        // because we know read() always reads fully
        read(position, buffer, offset, length);
      }

      @Override
      public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position, buffer, 0, buffer.length);
      }
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
        Progressable progressable) throws IOException {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public void setWorkingDirectory(Path path) {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public Path getWorkingDirectory() {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
      throw new RuntimeException("Unimplemented");
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {

      long length = this.m_data.length;
      boolean is_dir = false;
      int block_replication = 1;
      long blocksize = 4096;
      long modification_time = 0;

      FileStatus status = new FileStatus(length, is_dir, block_replication, blocksize, modification_time, path);
      return status;
    }
  }
}
