/*
 * Copyright (c) 2018, Seqera Labs.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * This Source Code Form is "Incompatible With Secondary Licenses", as
 * defined by the Mozilla Public License, v. 2.0.
 *
 */

package nextflow.file.gs

import java.nio.file.DirectoryStream
import java.nio.file.DirectoryStream.Filter
import java.nio.file.Path

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Bucket
import groovy.transform.CompileStatic
/**
 * Implements a directory stream iterator for the Google Cloud storage file system provider
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

    private GsPath origin

    GsPathIterator(GsPath origin, Iterator<T> itr, Filter<? super Path> filter) {
        this.origin = origin
        this.fs = origin.fileSystem
        this.itr = itr
        this.filter = filter
        advance()
    }

    abstract GsPath createPath(GsFileSystem fs, T item)

    private void advance() {

        GsPath result = null
        while( result == null && itr.hasNext() ) {
            def item = itr.next()
            def path = createPath(fs, item)
            if( path == origin )    // make sure to  skip the origin path
                result = null
            else if( filter )
                result = filter.accept(path) ? path : null
            else
                result = path
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

    /**
     * Implements a path iterator for blob object i.e. files and path in a
     * Google cloud storage file system
     */
    static class ForBlobs extends GsPathIterator<Blob> {

        ForBlobs(GsPath path, Iterator<Blob> itr, Filter<? super Path> filter) {
            super(path, itr, filter)
        }

        @Override
        GsPath createPath(GsFileSystem fs, Blob item) {
            return new GsPath(fs, item)
        }
    }

    /**
     * Implements an iterator for buckets in a Google Cloud storage file system
     */
    static class ForBuckets extends GsPathIterator<Bucket> {

        ForBuckets(GsPath path, Iterator<Bucket> itr, Filter<? super Path> filter) {
            super(path, itr, filter)
        }

        @Override
        GsPath createPath(GsFileSystem fs, Bucket item) {
            return fs.getPath("/${item.name}").setAttributes( new GsBucketAttributes(item) )
        }
    }

}
