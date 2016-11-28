package nextflow.file.gs

import java.nio.file.DirectoryStream.Filter
import java.nio.file.Path

import com.google.cloud.storage.Blob
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
class GsDirectoryIterator implements Iterator<Path> {

    private GsFileSystem fs

    private Iterator<Blob> itr

    private Filter<? super Path> filter

    private GsPath next


    GsDirectoryIterator(GsFileSystem fs, Iterator<Blob> itr, Filter<? super Path> filter) {
        this.fs = fs
        this.itr = itr
        this.filter = filter
        advance()
    }


    private void advance() {

        GsPath result = null
        while(itr.hasNext() && result == null) {
            def blob = itr.next()
            def path = new GsPath(fs, blob)
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
}
