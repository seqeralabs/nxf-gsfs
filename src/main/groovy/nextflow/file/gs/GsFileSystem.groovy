package nextflow.file.gs

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService

import com.google.cloud.storage.Bucket
import com.google.cloud.storage.Storage
import groovy.transform.CompileDynamic
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

    private GsFileSystemProvider provider

    private Storage storage

    private String bucket

    @PackageScope
    GsFileSystem( GsFileSystemProvider provider, Storage storage, String bucket ) {
        this.provider = provider
        this.bucket = bucket
        this.storage = storage
    }

    String getBucket() {
        bucket
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
                .collect { Bucket b -> provider.getPath(new URI("$GsFileSystemProvider.SCHEME://${b.getName()}")) }
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
    GsPath getPath(String first, String... more) {
        if( more ) {
            def path = [first]; path.addAll(more)
            new GsPath(this, path.collect {trimSlash(it)}.join('/'))
        }
        else {
            return new GsPath(this, first)
        }
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

}
