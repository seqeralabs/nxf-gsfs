package nextflow.file.gs

import static com.google.cloud.storage.Storage.CopyRequest
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING
import static java.nio.file.StandardOpenOption.APPEND
import static java.nio.file.StandardOpenOption.CREATE
import static java.nio.file.StandardOpenOption.CREATE_NEW
import static java.nio.file.StandardOpenOption.DSYNC
import static java.nio.file.StandardOpenOption.READ
import static java.nio.file.StandardOpenOption.SYNC
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import static java.nio.file.StandardOpenOption.WRITE

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.DirectoryStream
import java.nio.file.DirectoryStream.Filter
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

import com.google.cloud.ReadChannel
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageBatch
import com.google.cloud.storage.StorageException
import groovy.transform.CompileStatic
import groovy.transform.PackageScope

/**
 * JSR-203 file system provider implementation for Google Cloud Storage
 *
 * See
 *  http://googlecloudplatform.github.io/google-cloud-java/0.3.0/index.html
 *  https://github.com/GoogleCloudPlatform/google-cloud-java#google-cloud-storage
 *
 * Examples
 *  https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-examples/src/main/java/com/google/cloud/examples/storage/snippets
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class GsFileSystemProvider extends FileSystemProvider {

    public static String SCHEME = 'gs'

    private GsFileSystem fs

    @PackageScope
    static GsFileSystemProvider create(File credentials, String projectId) {
        def result = new GsFileSystemProvider()
        result.newFileSystem(new URI('gs:///'), [credentials: credentials, projectId: projectId])
        return result
    }

    /**
     * @inheritDoc
     */
    @Override
    String getScheme() {
        return SCHEME
    }

    static private GsPath gpath( Path path ) {
        if( path instanceof GsPath )
            return (GsPath)path
        throw new IllegalArgumentException("Not a valid Google Storage path object: `$path` [${path?.class?.name?:'-'}]" )
    }


    /**
     * @inheritDoc
     */
    @Override
    synchronized FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        if( fs )
            throw new FileSystemAlreadyExistsException()

        def credentials = (File)env.get('credentials')
        def projectId = (String)env.get('projectId')
        if( credentials && projectId )
            return fs = new GsFileSystem(this, credentials, projectId)

        // -- look-up config settings in the environment variables
        credentials = (String)env.get('GOOGLE_APPLICATION_CREDENTIALS')
        projectId = (String)env.get('GOOGLE_PROJECT_ID')

        if( credentials && projectId )
            return fs = new GsFileSystem(this, new File(credentials), projectId)

        if( !credentials ) throw new IllegalStateException("Missing Google Cloud credentials file")
        throw new IllegalStateException("Missing Google Cloud project ID")
    }


    @Override
    FileSystem getFileSystem(URI uri) {
        return fs
    }

    @Override
    Path getPath(URI uri) {
        if( uri.scheme != SCHEME )
            throw new IllegalArgumentException("Not a valid Google Storage URI scheme: ${uri.scheme}")

        if( fs == null )
            throw new IllegalStateException("Google Storage file system was not created")

        def bucket = uri.authority
        def object = uri.path
        while( object.startsWith('/') )
            object = object.substring(1)

        return fs.getPath(bucket, object)
    }


    GsPath getPath( String path ) {
        assert path.startsWith('/')
        def p = path.substring(1).indexOf('/')
        if( p == -1 ) {
            return new GsPath(fs, path.substring(1))
        }
        else {
            return new GsPath(fs, path.substring(1,p+1), path.substring(p+2))
        }
    }

    static private FileSystemProvider provider( Path path ) {
        path.fileSystem.provider()
    }

    static private Storage storage( Path path ) {
        ((GsPath)path).getFileSystem().getStorage()
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        final modeRead = options.contains(READ)
        final modeWrite = options.contains(WRITE) || options.contains(APPEND)

        if( modeRead && modeWrite ) {
            throw new IllegalArgumentException("Google Storage file cannot be opened in R/W mode at the same time")
        }
        if( options.contains(APPEND) ) {
            throw new IllegalArgumentException("Google Storage file system does not support `APPEND` mode")
        }
        if( options.contains(SYNC) ) {
            throw new IllegalArgumentException("Google Storage file system does not support `SYNC` mode")
        }
        if( options.contains(DSYNC) ) {
            throw new IllegalArgumentException("Google Storage file system does not support `DSYNC` mode")
        }

        final gpath = gpath(path)
        if( modeWrite ) {
            if( options.contains(CREATE_NEW) && exists(gpath)) {
                throw new FileAlreadyExistsException(gpath.toUriString())
            }
            if( !options.contains(CREATE) && !exists(gpath) ) {
                throw new NoSuchFileException(gpath.toUriString())
            }
            if( !options.contains(TRUNCATE_EXISTING) ) {
                throw new IllegalArgumentException("Google Storage file can only written using TRUNCATE mode")
            }
            return newWritableByteChannel(gpath)
        }
        else {
            return newReadableByteChannel(gpath)
        }
    }

    protected SeekableByteChannel newReadableByteChannel(GsPath path) {
        final storage = storage(path)
        final Blob blob = storage.get(path.blobId)
        if( !blob ) throw new NoSuchFileException("File does not exist: ${path.toUriString()}")

        final size = blob.size()
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

    protected SeekableByteChannel newWritableByteChannel(GsPath path) {
        final storage = storage(path)
        final blobInfo = BlobInfo.builder(path.blobId).build()
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


    @Override
    DirectoryStream<Path> newDirectoryStream(Path obj, Filter<? super Path> filter) throws IOException {
        final dir = gpath(obj)
        final storage = storage(dir)
        final bucket = storage.get(dir.bucketName)
        if( !bucket )
            throw new NoSuchFileException("Unknown Google Storage bucket: ${dir.bucketName}")

        def prefix = dir.objectName
        if( prefix && !prefix.endsWith('/') ) prefix += '/'
        Iterator<Blob> blobs = (prefix
                ? bucket.list( Storage.BlobListOption.prefix(prefix) ).iterateAll()
                : bucket.list().iterateAll() )

        return new DirectoryStream<Path>() {
            @Override
            Iterator<Path> iterator() {
                return new GsDirectoryIterator(dir.fileSystem, blobs, filter)
            }

            @Override void close() throws IOException { }
        }
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        final path = gpath(dir)
        final storage = storage(dir)
        assert path.bucketName, "Missing Google Storage bucket name"

        if( path.isBucket() ) {
            final info = BucketInfo.of(path.bucketName)
            storage.create(info)
        }
        else {
            path.isDirectory = true
            final blobId = BlobId.of(path.bucketName, path.objectName)
            final info = BlobInfo.builder(blobId).build()
            storage.create(info)
        }
    }

    void createFile(GsPath path, String content) {
        createFile(path, content.getBytes())
    }

    void createFile(GsPath path, byte[] content) {
        final storage = storage(path)
        final blobId = BlobId.of(path.bucketName, path.objectName)
        final info = BlobInfo.builder(blobId).build()
        storage.create(info, content)
    }

    @Override
    void delete(Path obj) throws IOException {
        final path = gpath(obj)
        assert path.bucketName, "Missing Google Storage bucket name"

        if( path.isBucket() ) {
            deleteBucket(path)
        }
        else {
            deleteFile(path)
        }
    }

    private void deleteFile(GsPath path) {
        final storage = storage(path)
        boolean result
        try {
            result = storage.delete(path.blobId)
        }
        catch( StorageException e ) {
            throw new IOException("Error deleting file: ${path.toUriString()}", e)
        }

        if( !result ) {
            throw new IOException("Error deleting file: ${path.toUriString()}")
        }
    }

    private void deleteBucket(GsPath path) {
        final storage = storage(path)
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

    @Override
    void copy(Path from, Path to, CopyOption... options) throws IOException {
        assert provider(from) == provider(to)
        if( from == to )
            return // nothing to do -- just return

        final source = gpath(from)
        final target = gpath(to)
        if( options.contains(REPLACE_EXISTING) && exists(target) ) {
            delete(target)
        }

        def request = CopyRequest
                        .builder()
                        .source(source.blobId)
                        .target(target.blobId)
                        .build();
        def copyWriter = storage(source).copy(request);
        while (!copyWriter.isDone()) {
            copyWriter.copyChunk();
        }

    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        copy(source,target,options)
        delete(source)
    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        return path == path2
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        return path.getFileName()?.toString()?.startsWith('.')
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException()
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        if( modes.size() )
            throw new UnsupportedOperationException()

        readAttributes(gpath(path), GsFileAttributes.class)
    }

    protected boolean exists( GsPath path ) {
        try {
            return readAttributes(path, GsFileAttributes) != null
        }
        catch( IOException e ){
            return false
        }
    }

    protected GsFileAttributes readAttributes0(GsPath path)  {
        final storage = storage(path)
        def cache = path.attributesCache()
        if( cache )
            return cache

        if( path.bucketName && !path.objectName ) {
            def bucket = storage.get(path.bucketName)
            return bucket ? new GsFileAttributes(bucket) : null
        }

        def blob = storage.get(path.blobId)
        return blob ? new GsFileAttributes(blob) : null
    }

    protected GsFileAttributesView getFileAttributeView0(GsPath path) {
        final storage = storage(path)
        def blob = storage.get(path.blobId)
        if( blob ) {
            return new GsFileAttributesView(blob)
        }

        StorageBatch batch = storage.batch();
        batch.get(path.blobId).notify()
        batch.submit()

        throw new NoSuchFileException("File does not exist: ${path.toUriString()}")
    }

    @Override
    def <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        if( type == BasicFileAttributeView || type == GsFileAttributesView ) {
            return (V)getFileAttributeView0(gpath(path))
        }
        throw new UnsupportedOperationException("Not a valid Google Storage file attribute view: $type")
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path obj, Class<A> type, LinkOption... options) throws IOException {
        if( type == BasicFileAttributes || type == GsFileAttributes ) {
            def path = gpath(obj)
            def result = (A)readAttributes0(path)
            if( result ) return result
            throw new NoSuchFileException(path.toUriString())
        }
        throw new UnsupportedOperationException("Not a valid Google Storage file attribute type: $type")
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException()
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException()
    }
}
