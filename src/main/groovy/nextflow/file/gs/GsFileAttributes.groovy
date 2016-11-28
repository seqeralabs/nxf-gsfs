package nextflow.file.gs
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.util.concurrent.TimeUnit

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BucketInfo
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

/**
 * Models file attributes for Google Cloud Storage
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
@EqualsAndHashCode(includeFields = true)
class GsFileAttributes implements BasicFileAttributes {

    private FileTime updateTime

    private FileTime creationTime

    private boolean isDir

    private long size

    private objectId

    GsFileAttributes( Blob blob ) {
        objectId = blob.getBlobId()
        // creation
        creationTime = time(blob.getCreateTime())
        // update
        updateTime = time(blob.getUpdateTime())
        // is dir
        isDir = blob.getName().endsWith('/')
        // size
        size = blob.getSize()
    }

    static private FileTime time(Long millis) {
        millis ? FileTime.from(millis, TimeUnit.MILLISECONDS) : null
    }

    GsFileAttributes( BucketInfo info ) {
        objectId = info.getGeneratedId()
        creationTime = time(info.getCreateTime())
        updateTime = null
        isDir = true
        size = 0
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
        return !isDir
    }

    @Override
    boolean isDirectory() {
        return isDir
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
