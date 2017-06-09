package io.opentraffic.datastore.source;

import io.opentraffic.datastore.source.FileSource;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * A FileSource over local files.
 */
public class LocalFileSource implements FileSource {

    private final List<String> m_files;

    public LocalFileSource(List<String> files) {
        this.m_files = files;
    }

    @Override
    public Iterator<InputStream> iterator() {
        return new LocalFileSourceIterator(m_files.iterator());
    }

    private static class LocalFileSourceIterator implements Iterator<InputStream> {
        private final Iterator<String> m_iterator;

        public LocalFileSourceIterator(Iterator<String> iterator) {
            this.m_iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return this.m_iterator.hasNext();
        }

        @Override
        public InputStream next() {
            String s = this.m_iterator.next();
            if (s != null) {
                try {
                    FileInputStream fis = new FileInputStream(s);
                    return fis;
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException("Unable to open file \"" + s + "\"", ex);
                }
            }
            return null;
        }
    }
}
