package nextflow.file.gs

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService

import com.google.cloud.AuthCredentials
import com.google.cloud.storage.Bucket
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
/**
 * JSR-203 file system implementation for Google Cloud Storage
 *
 *
 * See
 *  http://googlecloudplatform.github.io/google-cloud-java/0.3.0/index.html
 *  https://github.com/GoogleCloudPlatform/google-cloud-java#google-cloud-storage
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class GsFileSystem extends FileSystem {

    private GsFileSystemProvider provider

    private Storage storage

    /**
     * Create a new Google storage file system
     *
     * @param provider The {@link GsFileSystemProvider} instance
     * @param credentials The JSON file holding the Google cloud credentials
     * @param projectId The Google cloud project id
     */
    @PackageScope
    GsFileSystem( GsFileSystemProvider provider, File credentials, String projectId ) {
        this.provider = provider
        this.storage = StorageOptions
                .builder()
                .authCredentials(AuthCredentials.createForJson(new FileInputStream(credentials)))
                .projectId(projectId)
                .build()
                .service()
    }

    @PackageScope
    GsFileSystem( GsFileSystemProvider provider, Storage storage ) {
        this.provider = provider
        this.storage = storage
    }

    @PackageScope
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
        return false
    }

    @Override
    String getSeparator() {
        return '/'
    }

    @Override
    @CompileDynamic
    Iterable<Path> getRootDirectories() {
        storage
                .list()
                .iterateAll()
                .collect { Bucket b -> new GsPath(this, b.name()) }
    }

    @Override
    Iterable<FileStore> getFileStores() {
        throw new UnsupportedOperationException()
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        return Collections.unmodifiableSet( ['basic'] as Set )
    }

    @Override
    GsPath getPath(String bucket, String... more) {
        if( !bucket ) throw new IllegalArgumentException("Missing bucket name")
        return more ? new GsPath(this, bucket, more.join('/')) : new GsPath(this,bucket)
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        //TODO
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

//    @PackageScope
//    InputStream newInputStream(GsPath path) {
//        ReadChannel reader = storage.reader(path.blobId)
//        Channels.newInputStream(reader)
//    }
//
//    OutputStream newOutputStream(GsPath path) {
//        final blobInfo = BlobInfo.builder(path.blobId).build()
//        final writer = storage.writer(blobInfo)
//        Channels.newOutputStream(writer)
//    }


}
