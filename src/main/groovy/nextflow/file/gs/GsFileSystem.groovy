package nextflow.file.gs

import static com.google.cloud.storage.Storage.BlobListOption.prefix

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.DirectoryStream
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService

import com.google.cloud.ReadChannel
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Bucket
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageBatch
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.Storage.BlobListOption
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
/**
 * JSR-203 file system implementation for Google Cloud Storage
 *
 * See
 *  http://googlecloudplatform.github.io/google-cloud-java/
 *  https://github.com/GoogleCloudPlatform/google-cloud-java#google-cloud-storage
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class GsFileSystem extends FileSystem {

    private static String ROOT = '/'

    private GsFileSystemProvider provider

    private Storage storage

    private String bucket

    /**
     * The storage class type. Either {@code multi_regional}, {@code regional}, {@code nearline} or {@code coldline}
     *
     * See https://cloud.google.com/storage/docs/storage-classes
     */
    String storageClass

    /**
     * The storage location e.g. {@code eu} or {@code us}
     *
     * See https://cloud.google.com/storage/docs/bucket-locations
     */
    String location

    @PackageScope GsFileSystem() {}

    @PackageScope
    GsFileSystem( GsFileSystemProvider provider, Storage storage, String bucket ) {
        this.provider = provider
        this.bucket = bucket
        this.storage = storage
    }

    String getBucket() { bucket }

    Storage getStorage() { storage }

    @Override
    GsFileSystemProvider provider() {
        return provider
    }

    @Override
    void close() throws IOException {
        // nothing to do
    }

    @Override
    boolean isOpen() {
        return true
    }

    @Override
    boolean isReadOnly() {
        return bucket == ROOT
    }

    @Override
    String getSeparator() {
        return '/'
    }

    Iterable<? extends Path> getRootDirectories() {
        return bucket == ROOT ? listBuckets() : [ new GsPath(this, "/$bucket/") ]
    }

    private Iterable<? extends Path> listBuckets() {
        storage
                .list()
                .iterateAll()
                .collect { Bucket b -> provider.getPath(b.getName()) }
    }

    @Override
    Iterable<FileStore> getFileStores() {
        throw new UnsupportedOperationException()
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        return Collections.unmodifiableSet( ['basic'] as Set )
    }

    /**
     * Get a {@link GsPath} given a string path.
     *
     */
    @Override
    GsPath getPath(String path, String... more) {
        assert path

        if( more ) {
            path = concat(path,more)
        }

        if( path.startsWith('/') ) {
            return provider().getPath(path.substring(1))
        }
        else {
            return new GsPath(this, path)
        }
    }

    private String concat(String path, String... more) {
        def concat = []
        while( path.length()>1 && path.endsWith('/') )
            path = path.substring(0,path.length()-1)
        concat << path
        concat.addAll( more.collect {String it -> trimSlash(it)} )
        return concat.join('/')
    }

    private String trimSlash(String str) {
        while( str.startsWith('/') )
            str = str.substring(1)
        while( str.endsWith('/') )
            str = str.substring(0,str.length()-1)
        return str
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException()
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException()
    }

    @Override
    WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException()
    }

    @PackageScope
    SeekableByteChannel newReadableByteChannel(GsPath path) {
        final Blob blob = storage.get(path.blobId)
        if( !blob ) throw new NoSuchFileException("File does not exist: ${path.toUriString()}")

        final size = blob.getSize()
        final ReadChannel reader = storage.reader(path.blobId)

        return new SeekableByteChannel() {

            long _position

            @Override
            int read(ByteBuffer dst) throws IOException {
                final len = reader.read(dst)
                _position += len
                return len
            }

            @Override
            int write(ByteBuffer src) throws IOException {
                throw new UnsupportedOperationException()
            }

            @Override
            long position() throws IOException {
                return _position
            }

            @Override
            SeekableByteChannel position(long newPosition) throws IOException {
                reader.seek(newPosition)
                return this
            }

            @Override
            long size() throws IOException {
                return size
            }

            @Override
            SeekableByteChannel truncate(long dummy) throws IOException {
                throw new UnsupportedOperationException()
            }

            @Override
            boolean isOpen() {
                return true
            }

            @Override
            void close() throws IOException {
                reader.close()
            }
        }
    }

    @PackageScope
    SeekableByteChannel newWritableByteChannel(GsPath path) {
        final blobInfo = BlobInfo.newBuilder(path.blobId).build()
        final writer = storage.writer(blobInfo)

        return new SeekableByteChannel()  {

            long _pos

            @Override
            int read(ByteBuffer dst) throws IOException {
                throw new UnsupportedOperationException()
            }

            @Override
            int write(ByteBuffer src) throws IOException {
                def len = writer.write(src)
                _pos += len
                return len
            }

            @Override
            long position() throws IOException {
                return _pos
            }

            @Override
            SeekableByteChannel position(long newPosition) throws IOException {
                throw new UnsupportedOperationException()
            }

            @Override
            long size() throws IOException {
                return _pos
            }

            @Override
            SeekableByteChannel truncate(long size) throws IOException {
                throw new UnsupportedOperationException()
            }

            @Override
            boolean isOpen() {
                return true
            }

            @Override
            void close() throws IOException {
                writer.close()
            }
        }
    }

    @PackageScope
    DirectoryStream<Path> newDirectoryStream(GsPath dir, DirectoryStream.Filter<? super Path> filter) {
        if( !dir.bucketName )
            throw new NoSuchFileException("Missing Google storage bucket name: ${dir.toUriString()}")

        final fs = dir.getFileSystem()
        if( dir.bucketName == ROOT ) {
            Iterator<Bucket> buckets = storage.list().iterateAll()
            return GsPathIterator.buckets(fs, buckets, filter)
        }

        else {
            def opts = [BlobListOption.currentDirectory()]
            def prefix = dir.objectName
            if( prefix && !prefix.endsWith('/') ) {
                prefix += '/'
                opts << BlobListOption.prefix(prefix)
            }
            Iterator<Blob> blobs = storage.list(dir.bucketName, opts as BlobListOption[]).iterateAll()
            GsPathIterator.dirs(fs, blobs, filter)
        }

    }

    @PackageScope
    def createDirectory(GsPath path) {
        if( isReadOnly() )
            throw new UnsupportedOperationException('Operation not support in root path')

        if( !path.bucketName )
            throw new IllegalArgumentException("Missing Google Storage bucket name")

        if( path.isBucket() ) {
            def builder = BucketInfo.newBuilder(path.bucketName)
            if( location )
                builder.setLocation(location)
            if( storageClass )
                builder.setStorageClass(storageClass)
            storage.create(builder.build())
        }
        else {
            path.directory = true
            final blobId = BlobId.of(path.bucketName, path.objectName + '/')
            final info = BlobInfo.newBuilder(blobId).build()
            storage.create(info)
        }
    }

    @PackageScope
    void delete(GsPath path)  {
        if( !path.bucketName )
            throw new IllegalArgumentException("Missing Google Storage bucket name")

        if( path.isBucket() ) {
            deleteBucket(path)
        }
        else {
            deleteFile(path)
        }
    }

    private void checkExistOrEmpty(GsPath path) {
        boolean exists = false
        boolean dirEmpty = true

        try {
            def values = storage
                    .list(bucket, prefix(path.objectName))
                    .getValues()

            final char SLASH = '/'
            final String name = path.objectName

            for( Blob blob : values ) {
                if( blob.name == name )
                    exists = true
                else if( blob.name.startsWith(name) && blob.name.charAt(name.length())==SLASH ) {
                    exists = true
                    dirEmpty = false
                }
            }

        }
        catch( StorageException e )  {
            if(e.reason=='notFound')
                exists = false
            else
                throw e
        }

        if( !exists )
            throw new NoSuchFileException(path.toUriString())

        if( !dirEmpty )
            throw new DirectoryNotEmptyException(path.toUriString())
    }

    private void deleteFile(GsPath path) {
        boolean result
        try {
            checkExistOrEmpty(path)
            result = storage.delete(path.blobId)
        }
        catch( StorageException e ) {
            throw new IOException("Error deleting file: ${path.toUriString()}", e)
        }

        if( !result ) {
            throw new IOException("Failed to delete file: ${path.toUriString()}")
        }
    }

    private void deleteBucket(GsPath path) {
        boolean result
        try {
            result = storage.delete(path.bucketName)
        }
        catch( StorageException e ) {
            if( e.message == 'The bucket you tried to delete was not empty.' )
                throw new DirectoryNotEmptyException(path.toUriString())
            else
                throw new IOException("Error deleting bucket: ${path.toUriString()}", e)
        }

        if( !result ) {
            if(exists(path))
                throw new IOException("Error deleting bucket: ${path.toUriString()}")
            else
                throw new NoSuchFileException(path.toUriString())
        }
    }

    @PackageScope
    void copy(GsPath source, GsPath target) {

        def request = Storage.CopyRequest
                .newBuilder()
                .setSource(source.blobId)
                .setTarget(target.blobId)
                .build()

        def copyWriter = storage.copy(request)
        while (!copyWriter.isDone()) {
            copyWriter.copyChunk()
        }
    }

    @PackageScope
    GsFileAttributes readAttributes(GsPath path)  {
        def cache = path.attributesCache()
        if( cache )
            return cache

        if( path.toString() == ROOT ) {
            return GsFileAttributes.root()
        }

        if( path.bucketName && !path.objectName ) {
            def bucket = storage.get(path.bucketName)
            return bucket ? new GsBucketAttributes(bucket) : null
        }

        if( path.directory ) {
            return readDirectoryAttrs(storage,path)
        }

        def blob = storage.get(path.blobId)
        GsFileAttributes result = blob ? new GsFileAttributes(blob) : null
        return result ?: readDirectoryAttrs(storage,path)
    }

    @PackageScope
    GsFileAttributes readDirectoryAttrs(Storage storage, GsPath path) {
        final opts = []
        opts << Storage.BlobListOption.prefix(path.objectName)
        opts << Storage.BlobListOption.currentDirectory()
        final itr = storage.list(path.bucketName, opts as Storage.BlobListOption[]).iterateAll()
        while( itr.hasNext() ) {
            def blob = itr.next()
            if( blob.name == path.objectName + '/')
                return new GsFileAttributes(blob)
        }
        return null
    }

    @PackageScope
    GsFileAttributesView getFileAttributeView(GsPath path) {
        def blob = storage.get(path.blobId)
        if( blob ) {
            return new GsFileAttributesView(blob)
        }

        StorageBatch batch = storage.batch();
        batch.get(path.blobId).notify()
        batch.submit()

        throw new NoSuchFileException("File does not exist: ${path.toUriString()}")
    }

    @PackageScope
    boolean exists( GsPath path ) {
        try {
            return readAttributes(path) != null
        }
        catch( IOException e ){
            return false
        }
    }
}
