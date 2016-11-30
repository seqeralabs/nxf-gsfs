package nextflow.file.gs

import static com.google.cloud.storage.Storage.CopyRequest
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING
import static java.nio.file.StandardOpenOption.APPEND
import static java.nio.file.StandardOpenOption.CREATE
import static java.nio.file.StandardOpenOption.CREATE_NEW
import static java.nio.file.StandardOpenOption.DSYNC
import static java.nio.file.StandardOpenOption.READ
import static java.nio.file.StandardOpenOption.SYNC
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
import java.nio.file.FileSystemNotFoundException
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.ReadChannel
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageBatch
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions
import groovy.transform.CompileStatic
/**
 * JSR-203 file system provider implementation for Google Cloud Storage
 *
 * See
 *  http://googlecloudplatform.github.io/google-cloud-java
 *  https://github.com/GoogleCloudPlatform/google-cloud-java#google-cloud-storage
 *
 * Examples
 *  https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-examples/src/main/java/com/google/cloud/examples/storage/snippets
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class GsFileSystemProvider extends FileSystemProvider {

    public final static String SCHEME = 'gs'

    private Map<String,GsFileSystem> fileSystems = [:]

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

    protected String getBucket(URI uri) {
        assert uri
        if( !uri.scheme )
            throw new IllegalArgumentException("Missing URI scheme")

        if( !uri.authority )
            throw new IllegalArgumentException("Missing bucket name")

        if( uri.scheme.toLowerCase() != SCHEME )
            throw new IllegalArgumentException("Mismatch provider URI scheme: `$scheme`")

        return uri.authority.toLowerCase()
    }

    protected Storage createStorage(File credentials, String projectId) {
        StorageOptions
                .newBuilder()
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credentials)))
                .setProjectId(projectId)
                .build()
                .getService()
    }

    protected Storage createDefaultStorage() {
        StorageOptions.getDefaultInstance().getService()
    }

    /**
     * Constructs a new {@code FileSystem} object identified by a URI. This
     * method is invoked by the {@link java.nio.file.FileSystems#newFileSystem(URI,Map)}
     * method to open a new file system identified by a URI.
     *
     * <p> The {@code uri} parameter is an absolute, hierarchical URI, with a
     * scheme equal (without regard to case) to the scheme supported by this
     * provider. The exact form of the URI is highly provider dependent. The
     * {@code env} parameter is a map of provider specific properties to configure
     * the file system.
     *
     * <p> This method throws {@link FileSystemAlreadyExistsException} if the
     * file system already exists because it was previously created by an
     * invocation of this method. Once a file system is {@link
     * java.nio.file.FileSystem#close closed} it is provider-dependent if the
     * provider allows a new file system to be created with the same URI as a
     * file system it previously created.
     *
     * @param   uri
     *          URI reference
     * @param   env
     *          A map of provider specific properties to configure the file system;
     *          may be empty
     *
     * @return  A new file system
     *
     * @throws  IllegalArgumentException
     *          If the pre-conditions for the {@code uri} parameter aren't met,
     *          or the {@code env} parameter does not contain properties required
     *          by the provider, or a property value is invalid
     * @throws  IOException
     *          An I/O error occurs creating the file system
     * @throws  SecurityException
     *          If a security manager is installed and it denies an unspecified
     *          permission required by the file system provider implementation
     * @throws  FileSystemAlreadyExistsException
     *          If the file system has already been created
     */
    @Override
    synchronized GsFileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        final bucket = getBucket(uri)

        if( fileSystems.containsKey(bucket) )
            throw new FileSystemAlreadyExistsException("File system already exists for Google Storage bucket: `$bucket`")

        def credentials = (File)env.get('credentials')
        def projectId = (String)env.get('projectId')
        if( credentials && projectId ) {
            def storage = createStorage(credentials, projectId)
            def result = new GsFileSystem(this, storage, bucket)
            fileSystems[bucket] = result
            return result
        }

        // -- look-up config settings in the environment variables
        credentials = System.getProperty('GOOGLE_APPLICATION_CREDENTIALS')
        projectId = System.getProperty('GOOGLE_PROJECT_ID')
        if( credentials && projectId ) {
            def storage = createStorage(new File(credentials), projectId)
            def result = new GsFileSystem(this, storage, bucket)
            fileSystems[bucket] = result
            return result
        }

        // -- fallback on default configuration
        def storage = createDefaultStorage()
        def result = new GsFileSystem(this, storage, bucket)
        fileSystems[bucket] = result
        return result
    }

    /**
     * Returns an existing {@code FileSystem} created by this provider.
     *
     * <p> This method returns a reference to a {@code FileSystem} that was
     * created by invoking the {@link #newFileSystem(URI,Map) newFileSystem(URI,Map)}
     * method. File systems created the {@link #newFileSystem(Path,Map)
     * newFileSystem(Path,Map)} method are not returned by this method.
     * The file system is identified by its {@code URI}. Its exact form
     * is highly provider dependent. In the case of the default provider the URI's
     * path component is {@code "/"} and the authority, query and fragment components
     * are undefined (Undefined components are represented by {@code null}).
     *
     * <p> Once a file system created by this provider is {@link
     * java.nio.file.FileSystem#close closed} it is provider-dependent if this
     * method returns a reference to the closed file system or throws {@link
     * java.nio.file.FileSystemNotFoundException}. If the provider allows a new file system to
     * be created with the same URI as a file system it previously created then
     * this method throws the exception if invoked after the file system is
     * closed (and before a new instance is created by the {@link #newFileSystem
     * newFileSystem} method).
     *
     * @param   uri
     *          URI reference
     *
     * @return  The file system
     *
     * @throws  IllegalArgumentException
     *          If the pre-conditions for the {@code uri} parameter aren't met
     * @throws  java.nio.file.FileSystemNotFoundException
     *          If the file system does not exist
     * @throws  SecurityException
     *          If a security manager is installed and it denies an unspecified
     *          permission.
     */
    @Override
    FileSystem getFileSystem(URI uri) {
        getFileSystem0(uri,false)
    }

    private GsFileSystem getFileSystem0(URI uri, boolean canCreate) {
        final bucket = getBucket(uri)

        def fs = fileSystems.get(bucket)
        if( !fs ) {
            if( canCreate )
                fs = newFileSystem(uri, System.getenv())
            else
                throw new FileSystemNotFoundException("Missing Google Storage file system for bucket: `$bucket`")
        }

        return fs
    }

    /**
     * Return a {@code Path} object by converting the given {@link URI}. The
     * resulting {@code Path} is associated with a {@link FileSystem} that
     * already exists or is constructed automatically.
     *
     * <p> The exact form of the URI is file system provider dependent. In the
     * case of the default provider, the URI scheme is {@code "file"} and the
     * given URI has a non-empty path component, and undefined query, and
     * fragment components. The resulting {@code Path} is associated with the
     * default {@link java.nio.file.FileSystems#getDefault default} {@code FileSystem}.
     *
     * @param   uri
     *          The URI to convert
     *
     * @return  The resulting {@code Path}
     *
     * @throws  IllegalArgumentException
     *          If the URI scheme does not identify this provider or other
     *          preconditions on the uri parameter do not hold
     * @throws  java.nio.file.FileSystemNotFoundException
     *          The file system, identified by the URI, does not exist and
     *          cannot be created automatically
     * @throws  SecurityException
     *          If a security manager is installed and it denies an unspecified
     *          permission.
     */
    @Override
    Path getPath(URI uri) {
        final fs = getFileSystem0(uri,true)

        def object = uri.path
        while( object.startsWith('/') )
            object = object.substring(1)

        return fs.getPath(object)
    }


    static private FileSystemProvider provider( Path path ) {
        path.getFileSystem().provider()
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
            if( options.contains(CREATE_NEW) ) {
                if(exists(gpath)) throw new FileAlreadyExistsException(gpath.toUriString())
            }
            else if( !options.contains(CREATE)  ) {
                if(!exists(gpath)) throw new NoSuchFileException(gpath.toUriString())
            }
            if( options.contains(APPEND) ) {
                throw new IllegalArgumentException("File can only written using APPEND mode is not supported by Google Storage")
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

    protected SeekableByteChannel newWritableByteChannel(GsPath path) {
        final storage = storage(path)
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
            path.directory = true
            final blobId = BlobId.of(path.bucketName, path.objectName)
            final info = BlobInfo.newBuilder(blobId).build()
            storage.create(info)
        }
    }

    void createFile(GsPath path, String content) {
        createFile(path, content.getBytes())
    }

    void createFile(GsPath path, byte[] content) {
        final storage = storage(path)
        final blobId = BlobId.of(path.bucketName, path.objectName)
        final info = BlobInfo.newBuilder(blobId).build()
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
                        .newBuilder()
                        .setSource(source.blobId)
                        .setTarget(target.blobId)
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
