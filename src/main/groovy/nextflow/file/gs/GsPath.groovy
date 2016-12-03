package nextflow.file.gs

import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Bucket
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.PackageScope
/**
 * Model a Google Storage path
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@EqualsAndHashCode(includes = 'fs,path,directory', includeFields = true)
@CompileStatic
class GsPath implements Path {

    private GsFileSystem fs

    private Path path

    private GsFileAttributes attributes

    @PackageScope
    boolean directory

    @PackageScope
    GsPath() {}

    @PackageScope
    GsPath( GsFileSystem fs, String path ) {
        this(fs, Paths.get(path), path.endsWith("/") || path=="/$fs.bucket".toString())
    }

    @PackageScope
    GsPath(GsFileSystem fs, Blob blob ) {
        this(fs, "/${blob.bucket}/${blob.name}")
        this.attributes = new GsFileAttributes(blob)
    }

    @PackageScope
    GsPath( GsFileSystem fs, Path path, boolean directory ) {
        // make sure that the path bucket match the file system bucket
        if( path.isAbsolute() && path.nameCount>0 ) {
            def bkt = path.getName(0).toString()
            if( bkt != fs.bucket )
                throw new IllegalArgumentException("Path bucket `$bkt` does not match file system bucket: `${fs.bucket}`")
        }

        this.fs = fs
        this.path = path
        this.directory = directory
    }

    @PackageScope
    GsPath(GsFileSystem fs, Bucket bucket ) {
        this(fs, "/${bucket.name}/")
        this.attributes = new GsBucketAttributes(bucket)
    }

    @PackageScope
    GsPath setAttributes(GsFileAttributes attrs ) {
        this.attributes = attrs
        return this
    }

    @Override
    GsFileSystem getFileSystem() {
        return fs
    }

    @Override
    boolean isAbsolute() {
        path.isAbsolute()
    }

    @Override
    Path getRoot() {
        path.isAbsolute() ? new GsPath(fs, "/${path.getName(0)}/") : null
    }

    @Override
    Path getFileName() {
        final name = path.getFileName()
        name ? new GsPath(fs, name, directory) : null
    }

    @Override
    Path getParent() {
        if( path.isAbsolute() && path.nameCount>1 ) {
            new GsPath(fs, path.parent, true)
        }
        else {
            null
        }
    }

    @Override
    int getNameCount() {
        path.getNameCount()
    }

    @Override
    Path getName(int index) {
        final dir = index < path.getNameCount()-1
        new GsPath(fs, path.getName(index), dir)
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        final dir = endIndex < path.getNameCount()-1
        new GsPath(fs, path.subpath(beginIndex,endIndex), dir)
    }

    @Override
    boolean startsWith(Path other) {
        path.startsWith(other.toString())
    }

    @Override
    boolean startsWith(String other) {
        path.startsWith(other)
    }

    @Override
    boolean endsWith(Path other) {
        path.endsWith(other.toString())
    }

    @Override
    boolean endsWith(String other) {
        path.endsWith(other)
    }

    @Override
    Path normalize() {
        new GsPath(fs, path.normalize(), directory)
    }

    @Override
    GsPath resolve(Path other) {
        if( other.class != GsPath )
            throw new ProviderMismatchException()

        final that = (GsPath)other
        if( other.isAbsolute() )
            return that

        def newPath = path.resolve(that.path)
        new GsPath(fs, newPath, false)
    }

    @Override
    GsPath resolve(String other) {
        if( other.startsWith('/') )
            return (GsPath)fs.provider().getPath(new URI("$GsFileSystemProvider.SCHEME:/$other"))

        def dir = other.endsWith('/')
        def newPath = path.resolve(other)
        new GsPath(fs, newPath, dir)
    }

    @Override
    Path resolveSibling(Path other) {
        if( other.class != GsPath )
            throw new ProviderMismatchException()

        final that = (GsPath)other
        def newPath = path.resolveSibling(that.path)
        if( newPath.isAbsolute() )
            fs.getPath(newPath.toString())
        else
            new GsPath(fs, newPath, false)
    }

    @Override
    Path resolveSibling(String other) {
        def newPath = path.resolveSibling(other)
        if( newPath.isAbsolute() )
            fs.getPath(newPath.toString())
        else
            new GsPath(fs, newPath, false)
    }

    @Override
    Path relativize(Path other) {
        if( other.class != GsPath )
            throw new ProviderMismatchException()

        def newPath = path.relativize( ((GsPath)other).path )
        new GsPath(fs,newPath,false)
    }

    @Override
    String toString() {
        path.toString()
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
        final count = path.nameCount
        List<Path> paths = new ArrayList<>()
        for( int i=0; i<count; i++ ) {
            def dir = i<count-1
            paths.add(i, new GsPath(fs, path.getName(i), dir))
        }
        paths.iterator()
    }

    @Override
    int compareTo(Path other) {
        return this.toString() <=> other.toString()
    }

    String getBucketName() {
        if( path.isAbsolute() ) {
            path.nameCount==0 ? '/' : path.getName(0)
        }
        else
            return null
    }

    boolean isBucket() {
        path.isAbsolute() && path.nameCount<2
    }

    String getObjectName() {
        if( !path.isAbsolute() )
            return path.toString()

        if( path.nameCount>1 )
            return path.subpath(1, path.nameCount).toString()

        return null
    }

    BlobId getBlobId() {
        path.isAbsolute() && path.nameCount>1 ? BlobId.of(bucketName,getObjectName()) : null
    }

    String toUriString() {

        if( path.isAbsolute() ) {
            return "${GsFileSystemProvider.SCHEME}:/${path.toString()}"
        }
        else {
            return "${GsFileSystemProvider.SCHEME}:${path.toString()}"
        }
    }

    GsFileAttributes attributesCache() {
        def result = attributes
        attributes = null
        return result
    }

//    static GsPath get(String str)  {
//        if( str == null )
//            return null
//
//        def uri = new URI(null,null,str,null,null)
//
//        if( uri.scheme && GsFileSystemProvider.SCHEME != uri.scheme.toLowerCase())
//            throw new ProviderMismatchException()
//
//        uri.authority ? (GsPath)Paths.get(uri) : new GsPath(null, str)
//    }

}
