package nextflow.file.gs

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING
import static java.nio.file.StandardOpenOption.APPEND
import static java.nio.file.StandardOpenOption.CREATE
import static java.nio.file.StandardOpenOption.CREATE_NEW
import static java.nio.file.StandardOpenOption.DSYNC
import static java.nio.file.StandardOpenOption.READ
import static java.nio.file.StandardOpenOption.SYNC
import static java.nio.file.StandardOpenOption.WRITE

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.CopyOption
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
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import groovy.transform.CompileStatic
/**
 * JSR-203 file system provider implementation for Google Cloud Storage
 *
 * See
 *  http://googlecloudplatform.github.io/google-cloud-java
 *  https://github.com/GoogleCloudPlatform/google-cloud-java#google-cloud-storage
 *
 *  Relevant links
 *    https://cloud.google.com/storage/docs/consistency
 *    https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork
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

    static private GsPath asGsPath(Path path ) {
        if( path instanceof GsPath )
            return (GsPath)path
        throw new IllegalArgumentException("Not a valid Google storage path object: `$path` [${path?.class?.name?:'-'}]" )
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
     * @param   config
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
    synchronized GsFileSystem newFileSystem(URI uri, Map<String, ?> config) throws IOException {
        final bucket = getBucket(uri)
        newFileSystem0(bucket, config)
    }

    synchronized GsFileSystem newFileSystem0(String bucket, Map<String, ?> config) throws IOException {

        if( fileSystems.containsKey(bucket) )
            throw new FileSystemAlreadyExistsException("File system already exists for Google Storage bucket: `$bucket`")

        def credentials = (File)config.get('credentials')
        def projectId = (String)config.get('projectId')
        if( credentials && projectId ) {
            def storage = createStorage(credentials, projectId)
            def result = createFileSystem(storage, bucket, config)
            fileSystems[bucket] = result
            return result
        }

        // -- look-up config settings in the environment variables
        credentials = System.getProperty('GOOGLE_APPLICATION_CREDENTIALS')
        projectId = System.getProperty('GOOGLE_PROJECT_ID')
        if( credentials && projectId ) {
            def storage = createStorage(new File(credentials), projectId)
            def result = createFileSystem(storage, bucket, config)
            fileSystems[bucket] = result
            return result
        }

        // -- fallback on default configuration
        def storage = createDefaultStorage()
        def result = createFileSystem(storage, bucket, config)
        fileSystems[bucket] = result
        return result
    }

    private GsFileSystem createFileSystem(Storage storage, String bucket, Map<String,?> config) {

        def result = new GsFileSystem(this, storage, bucket)

        // -- location
        if( config.location )
            result.location = config.location

        // -- storage class
        if( config.storageClass )
            result.storageClass = config.storageClass

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
        final bucket = getBucket(uri)
        getFileSystem0(bucket,false)
    }

    protected GsFileSystem getFileSystem0(String bucket, boolean canCreate) {

        def fs = fileSystems.get(bucket)
        if( !fs ) {
            if( canCreate )
                fs = newFileSystem0(bucket, System.getenv())
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
    GsPath getPath(URI uri) {
        final bucket = getBucket(uri)
        getPath("$bucket/${uri.path}")
    }

    /**
     * Get a {@link GsPath} from an object path
     *
     * See https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork
     *
     * @param path A path in the form {@code bucket/objectName}
     * @return A {@link GsPath} object
     */
    GsPath getPath(String path) {
        assert path
        assert !path.startsWith('/')

        int p = path.indexOf('/')
        final bucket = p==-1 ? path : path.substring(0,p)
        final fs = getFileSystem0(bucket,true)

        new GsPath(fs, "/$path")
    }

    static private FileSystemProvider provider( Path path ) {
        path.getFileSystem().provider()
    }

    @Deprecated
    static private Storage storage( Path path ) {
        ((GsPath)path).getFileSystem().getStorage()
    }

    @Override
    SeekableByteChannel newByteChannel(Path obj, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        final modeWrite = options.contains(WRITE) || options.contains(APPEND)
        final modeRead = options.contains(READ) || !modeWrite

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

        final path = asGsPath(obj)
        final fs = path.getFileSystem()
        if( modeRead ) {
            return fs.newReadableByteChannel(path)
        }

        // -- mode write
        if( options.contains(CREATE_NEW) ) {
            if(fs.exists(path)) throw new FileAlreadyExistsException(path.toUriString())
        }
        else if( !options.contains(CREATE)  ) {
            if(!fs.exists(path)) throw new NoSuchFileException(path.toUriString())
        }
        if( options.contains(APPEND) ) {
            throw new IllegalArgumentException("File can only written using APPEND mode is not supported by Google Storage")
        }
        return fs.newWritableByteChannel(path)
    }


    @Override
    DirectoryStream<Path> newDirectoryStream(Path obj, Filter<? super Path> filter) throws IOException {
        final path = asGsPath(obj)
        path.fileSystem.newDirectoryStream(path, filter)
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        final path = asGsPath(dir)
        path.fileSystem.createDirectory(path)
    }

    @Override
    void delete(Path obj) throws IOException {
        final path = asGsPath(obj)
        path.fileSystem.delete(path)
    }


    @Override
    void copy(Path from, Path to, CopyOption... options) throws IOException {
        assert provider(from) == provider(to)
        if( from == to )
            return // nothing to do -- just return

        final source = asGsPath(from)
        final target = asGsPath(to)
        final fs = source.getFileSystem()

        if( options.contains(REPLACE_EXISTING) && fs.exists(target) ) {
            delete(target)
        }

        fs.copy(source, target)
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
        final gs = asGsPath(path)
        readAttributes(gs, GsFileAttributes.class)
        if( AccessMode.EXECUTE in modes)
            throw new AccessDeniedException(gs.toUriString(), null, 'Execute permission not allowed')
    }


    @Override
    def <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        if( type == BasicFileAttributeView || type == GsFileAttributesView ) {
            def gsPath = asGsPath(path)
            return (V)gsPath.fileSystem.getFileAttributeView(gsPath)
        }
        throw new UnsupportedOperationException("Not a valid Google Storage file attribute view: $type")
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        if( type == BasicFileAttributes || type == GsFileAttributes ) {
            def gsPath = asGsPath(path)
            def result = (A)gsPath.fileSystem.readAttributes(gsPath)
            if( result )
                return result
            throw new NoSuchFileException(gsPath.toUriString())
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
