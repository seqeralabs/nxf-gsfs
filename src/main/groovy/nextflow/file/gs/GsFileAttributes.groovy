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

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.util.concurrent.TimeUnit

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Bucket
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
/**
 * Models file attributes for Google Cloud Storage object
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
@EqualsAndHashCode(includeFields = true)
@ToString(includeFields = true, includeNames = true)
class GsFileAttributes implements BasicFileAttributes {

    private FileTime updateTime

    private FileTime creationTime

    private boolean directory

    private long size

    private String objectId

    static GsFileAttributes root() {
        new GsFileAttributes(size: 0, objectId: '/', directory: true)
    }

    GsFileAttributes() {}

    GsFileAttributes(Blob blob){
        objectId = "/${blob.getBucket()}/${blob.getName()}".toString()
        creationTime = time(blob.getCreateTime())
        updateTime = time(blob.getUpdateTime())
        directory = blob.getName().endsWith('/')
        size = blob.getSize()
    }

    protected GsFileAttributes(Bucket bucket) {
        objectId = "/$bucket.name".toString()
        creationTime = time(bucket.getCreateTime())
        directory = true
    }

    static protected FileTime time(Long millis) {
        millis ? FileTime.from(millis, TimeUnit.MILLISECONDS) : null
    }


    @Override
    FileTime lastModifiedTime() {
        updateTime
    }

    @Override
    FileTime lastAccessTime() {
        return null
    }

    @Override
    FileTime creationTime() {
        creationTime
    }

    @Override
    boolean isRegularFile() {
        return !directory
    }

    @Override
    boolean isDirectory() {
        return directory
    }

    @Override
    boolean isSymbolicLink() {
        return false
    }

    @Override
    boolean isOther() {
        return false
    }

    @Override
    long size() {
        return size
    }

    @Override
    Object fileKey() {
        return objectId
    }

    @Override
    boolean equals( Object obj ) {
        if( this.class != obj?.class ) return false
        def other = (GsFileAttributes)obj
        if( creationTime() != other.creationTime() ) return false
        if( lastModifiedTime() != other.lastModifiedTime() ) return false
        if( isRegularFile() != other.isRegularFile() ) return false
        if( size() != other.size() ) return false
        return true
    }

    @Override
    int hashCode() {
        Objects.hash( creationTime(), lastModifiedTime(), isRegularFile(), size() )
    }

}
