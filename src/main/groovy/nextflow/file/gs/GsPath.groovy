package nextflow.file.gs

import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.PackageScope

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@EqualsAndHashCode(includes = 'fs,bucket,parts', includeFields = true)
@CompileStatic
class GsPath implements Path {

    private GsFileSystem fs

    private GsFileAttributes attributes

    private String bucketName

    private List<String> parts

    @PackageScope
    boolean isDirectory

    GsPath( GsFileSystem fs, String bucket, String objectName=null ) {
        assert fs, "GsPath file system cannot be null"
        this.fs = fs
        this.bucketName = bucket
        this.isDirectory = objectName==null || objectName.endsWith("/")

        if( bucket ) {
            if( bucket.contains('/') ) throw new IllegalArgumentException("Not a valid bucket name: `$bucket`")
        }
        else {
            if( !objectName ) throw new IllegalArgumentException("Path name cannot be empty")
        }

        this.parts = objectName ? objectName.tokenize('/') : new ArrayList<String>()
    }

    GsPath( GsFileSystem fs, Blob blob ) {
        this(fs, blob.bucket(), blob.name())
        this.attributes = new GsFileAttributes(blob)
    }

    @Override
    GsFileSystem getFileSystem() {
        return fs
    }

    @Override
    boolean isAbsolute() {
        return bucketName != null
    }

    @Override
    Path getRoot() {
        return bucketName ? new GsPath(fs, bucketName) : null
    }

    @Override
    Path getFileName() {
        parts ? new GsPath(fs, null, parts[-1]) : new GsPath(fs,null,bucketName)
    }

    @Override
    Path getParent() {
        if( !objectName )
            return null
        if( parts.size()==1 )
            return bucketName ? new GsPath(fs, bucketName) : null
        else
            return new GsPath(fs, bucketName, parts[0..-2].join('/') + '/')
    }

    @Override
    int getNameCount() {
        parts ? parts.size()+1 : 1
    }

    @Override
    Path getName(int index) {
        if( index==0 )
            return new GsPath(fs,bucketName)

        def len = parts.size()
        if( index<len )
            return new GsPath(fs,null, parts.get(index))
        else
            throw new IllegalArgumentException("Not a valid path name index: $index -- path: ${toString()}" )
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        return null
    }

    @Override
    boolean startsWith(Path other) {
        return false
    }

    @Override
    boolean startsWith(String other) {
        return false
    }

    @Override
    boolean endsWith(Path other) {
        return false
    }

    @Override
    boolean endsWith(String other) {
        return false
    }

    @Override
    Path normalize() {
        return null
    }

    @Override
    GsPath resolve(Path other) {
        if(this.class != other.class)
            throw new IllegalArgumentException("File system path types do not match")
        if(other.isAbsolute())
            return (GsPath)other
        String otherName = ((GsPath)other).objectName
        return otherName ? this.resolve(otherName) : this
    }

    @Override
    GsPath resolve(String other) {
        if( other.startsWith('/') )
            return new GsPath(fs, bucketName, other.substring(1))

        if( !objectName )
            return new GsPath(fs, bucketName, other)

        return new GsPath(fs, bucketName, "$objectName/$other")
    }

    @Override
    Path resolveSibling(Path other) {
        return null
    }

    @Override
    Path resolveSibling(String other) {
        return null
    }

    @Override
    Path relativize(Path other) {
        return null
    }

    @Override
    String toString() {
        if( bucketName && parts )
            return "/$bucketName/${parts.join('/')}"

        if( bucketName )
            return "/$bucketName"

        if( parts )
            return parts.join('/')

        throw new IllegalStateException("Not a valid Google Storage path -- Both bucket and name attribute are missing")
    }

    @Override
    URI toUri() {
        return new URI(toUriString())
    }

    @Override
    Path toAbsolutePath() {
        if(isAbsolute()) return this
        throw new UnsupportedOperationException()
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        return toAbsolutePath()
    }

    @Override
    File toFile() {
        throw new UnsupportedOperationException()
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException()
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        throw new UnsupportedOperationException()
    }

    @Override
    Iterator<Path> iterator() {
        List<Path> paths = [ getRoot() ]
        if( objectName ) {
            paths.addAll( objectName.tokenize('/').collect { new GsPath(fs,null,it) } )
        }

        paths.iterator()
    }

    @Override
    int compareTo(Path other) {
        return this.toString() <=> other.toString()
    }

    String getBucketName() {
        bucketName
    }

    boolean isBucket() {
        bucketName && !objectName
    }

    String getObjectName() {
        if(!parts) return null
        final result = parts.join('/')
        isDirectory ? result+'/' : result
    }

    BlobId getBlobId() {
        BlobId.of(bucketName,getObjectName())
    }

    String toUriString() {
        def name = getObjectName()
        if( name && bucketName ) {
            return "${GsFileSystemProvider.SCHEME}://$bucketName/$name"
        }
        else if( bucketName ) {
            return "${GsFileSystemProvider.SCHEME}://$bucketName"
        }
        else if( name ) {
            return "${GsFileSystemProvider.SCHEME}:$name"
        }
        throw new IllegalStateException("Not a valid Google Storage path -- Both bucket and name attribute are missing")
    }

    GsFileAttributes attributesCache() {
        def result = attributes
        attributes = null
        return result
    }


}
