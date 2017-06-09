package io.opentraffic.datastore.source;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * File source which downloads files from S3.
 *
 * Input file names for this should be in the form s3://bucket/key
 */
public class S3FileSource implements FileSource {

    public static class S3Location {
        public final String bucket;
        public final String key;

        public S3Location(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }
    }
    private final List<S3Location> m_locations;
    private final AmazonS3 m_s3;

    public static S3Location parse(String file) {
        try {
            URI uri = new URI(file);
            if (!uri.getScheme().equals("s3")) {
                throw new RuntimeException("S3 file path must start with s3://");
            }
            return new S3Location(uri.getAuthority(), uri.getPath());

        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to parse s3:// URI from \"" + file + "\"", e);
        }
    }

    public S3FileSource(List<String> files, AmazonS3 s3) {
        this.m_locations = new ArrayList<>();
        for (String file : files) {
            this.m_locations.add(parse(file));
        }
        this.m_s3 = s3;
    }

    @Override
    public Iterator<InputStream> iterator() {
        return new S3FileIterator(this.m_locations.iterator());
    }

    private class S3FileIterator implements Iterator<InputStream> {
        private final Iterator<S3Location> m_wrapped;

        public S3FileIterator(Iterator<S3Location> wrapped) {
            this.m_wrapped = wrapped;
        }

        @Override
        public boolean hasNext() {
            return this.m_wrapped.hasNext();
        }

        @Override
        public InputStream next() {
            S3Location location = this.m_wrapped.next();
            S3Object obj = m_s3.getObject(location.bucket, location.key);
            return obj.getObjectContent();
        }
    }
}
