package nextflow.file.gs

import java.nio.file.FileSystemAlreadyExistsException

import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import spock.lang.Specification
import spock.lang.Unroll

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class GsFileSystemProviderTest extends Specification {

    def 'should return google storage scheme'() {
        given:
        def provider = new GsFileSystemProvider()
        expect:
        provider.getScheme() == 'gs'
    }

    @Unroll
    def 'should return a google storage path' () {
        given:
        def fs = Mock(GsFileSystem)
        fs.getBucket() >> 'bucket'
        def provider = Spy(GsFileSystemProvider)

        when:
        def path = provider.getPath(new URI(uri))
        then:
        1 * provider.getFileSystem0(_, true) >> fs
        path == new GsPath(fs, expected)

        where:
        uri                             | expected
        'gs:///'                        | '/'
        'gs://bucket'                   | '/bucket/'
        'gs://bucket/'                  | '/bucket/'
        'gs://bucket/this/and/that'     | '/bucket/this/and/that'
        'gs://bucket/this/and/that/'    | '/bucket/this/and/that/'

    }

    @Unroll
    def 'should get a google storage path' () {
        given:
        def fs = Mock(GsFileSystem)
        fs.getBucket() >> bucket
        def provider = Spy(GsFileSystemProvider)

        when:
        def path = provider.getPath(objectName)
        then:
        1 * provider.getFileSystem0(bucket, true) >> fs
        path == new GsPath(fs, expected)

        where:
        bucket              | objectName            | expected
        'bucket'            | 'bucket'              | '/bucket/'
        'bucket'            | 'bucket/'             | '/bucket/'
        'bucket'            | 'bucket/a/b'          | '/bucket/a/b'
        'bucket'            | 'bucket/a/b/'         | '/bucket/a/b/'
    }

    def 'should return the bucket given a URI'() {
        given:
        def provider = new GsFileSystemProvider()

        expect:
        provider.getBucket(new URI('gs://bucket/alpha/bravo')) == 'bucket'
        provider.getBucket(new URI('gs://BUCKET/alpha/bravo')) == 'bucket'

        when:
        provider.getBucket(new URI('s3://xxx'))
        then:
        thrown(IllegalArgumentException)

        when:
        provider.getBucket(new URI('gs:/alpha/bravo'))
        then:
        thrown(IllegalArgumentException)

        when:
        provider.getBucket(new URI('/alpha/bravo'))
        then:
        thrown(IllegalArgumentException)
    }

    def 'should create a new file system'() {

        given:
        def storage = Stub(Storage)
        def provider = Spy(GsFileSystemProvider)

        and:
        def uri = new URI('gs://bucket-example/alpha/bravo')
        def credentials = new File('xxx')
        def env = [credentials: credentials, projectId: 'project-xyz']

        when:
        def fs = provider.newFileSystem(uri, env)
        then:
        1 * provider.createStorage(credentials, 'project-xyz') >> storage
        fs.bucket == 'bucket-example'
        fs.provider() == provider
        provider.getFileSystem(uri) == fs

        when:
        provider.newFileSystem(uri, env)
        then:
        thrown(FileSystemAlreadyExistsException)
    }

    def 'should create paths and file systems'() {

        given:
        def storage = Stub(Storage)
        def provider = Spy(GsFileSystemProvider)

        when:
        def path1 = provider.getPath(new URI('gs://bucket-example/alpha/bravo'))
        then:
        1 * provider.createDefaultStorage() >> storage
        path1.getFileSystem().provider() == provider
        path1.toString() == '/bucket-example/alpha/bravo'

        when:
        def path2 = provider.getPath(new URI('gs://bucket-example/alpha/charlie'))
        0 * provider.createDefaultStorage() >> storage
        then:
        path2.getFileSystem().provider() == provider
        path2.toString() == '/bucket-example/alpha/charlie'

        when:
        def path3 = provider.getPath(new URI('gs://another-bucket/x/y'))
        then:
        1 * provider.createDefaultStorage() >> storage
        path3.getFileSystem().provider() == provider
        path3.toString() == '/another-bucket/x/y'
    }

    def 'should create directory' () {

        given:
        def storage = Mock(Storage)
        and:
        def credentials = new File('my-credentials.keys')
        def config = [location: location, storageClass: storageClass, credentials: credentials, projectId: projectId]
        def provider = Spy(GsFileSystemProvider)

        when:
        def fs= provider.newFileSystem0(bucket, config)
        then:
        1 * provider.createStorage(credentials, projectId) >> storage
        1 * provider.createFileSystem(_, bucket, config)
        fs.location == location
        fs.storageClass == storageClass

        when:
        fs.createDirectory(new GsPath(fs, "/$bucket"))
        then:
        1 * storage.create({ BucketInfo it ->
            it.location == location
            it.storageClass == storageClass }, _)

        where:
        bucket      | location  | storageClass  | projectId
        'foo'       | 'eu'      | 'nearline'    | 'x-1'
        'bar'       | 'us'      | 'coldline'    | 'y-2'
    }
}