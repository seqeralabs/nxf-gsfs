package nextflow.file.gs

import java.nio.file.DirectoryStream
import java.nio.file.DirectoryStream.Filter
import java.nio.file.Path

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Bucket
import groovy.transform.CompileStatic
/**
 * Implements a directory stream iterator
 *
 * @see java.nio.file.DirectoryStream
 * @see GsFileSystem#newDirectoryStream(nextflow.file.gs.GsPath, java.nio.file.DirectoryStream.Filter)
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
abstract class GsPathIterator<T> implements Iterator<Path> {

    private GsFileSystem fs

    private Iterator<T> itr

    private Filter<? super Path> filter

    private GsPath next


    GsPathIterator(GsFileSystem fs, Iterator<T> itr, Filter<? super Path> filter) {
        this.fs = fs
        this.itr = itr
        this.filter = filter
        advance()
    }

    abstract GsPath createPath(GsFileSystem fs, T item)

    private void advance() {

        GsPath result = null
        while(itr.hasNext() && result == null) {
            def item = itr.next()
            def path = createPath(fs, item)
            if( filter ) {
                result = filter.accept(path) ? path : null
            }
            else {
                result = path
            }
        }

        next = result
    }

    @Override
    boolean hasNext() {
        return next != null
    }

    @Override
    Path next() {
        def result = next
        if( result == null )
            throw new NoSuchElementException()
        advance()
        return result
    }

    @Override
    void remove() {
        throw new UnsupportedOperationException()
    }


    static class GsDirectoryIterator extends GsPathIterator<Blob> {

        GsDirectoryIterator(GsFileSystem fs, Iterator<Blob> itr, Filter<? super Path> filter) {
            super(fs, itr, filter)
        }

        @Override
        GsPath createPath(GsFileSystem fs, Blob item) {
            return new GsPath(fs, item)
        }
    }

    static class GsBucketIterator extends GsPathIterator<Bucket> {

        GsBucketIterator(GsFileSystem fs, Iterator<Bucket> itr, Filter<? super Path> filter) {
            super(fs, itr, filter)
        }

        @Override
        GsPath createPath(GsFileSystem fs, Bucket item) {
            return fs.getPath("/${item.name}").setAttributes( new GsBucketAttributes(item) )
        }
    }


    static DirectoryStream<Path> dirs(GsFileSystem fs, Iterator<Blob> itr, Filter<? super Path> filter ) {

        return new DirectoryStream<Path>() {

            @Override
            Iterator<Path> iterator() {
                return new GsDirectoryIterator(fs, itr, filter)
            }

            @Override void close() throws IOException { }
        }
    }

    static DirectoryStream<Path> buckets(GsFileSystem fs, Iterator<Bucket> buckets, Filter<? super Path> filter ) {

        return new DirectoryStream<Path>() {

            @Override
            Iterator<Path> iterator() {
                return new GsBucketIterator(fs, buckets, filter)
            }

            @Override void close() throws IOException { }
        }
    }
}
