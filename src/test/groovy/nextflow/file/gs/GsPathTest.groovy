package nextflow.file.gs

import java.nio.file.Paths

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import spock.lang.Specification
import spock.lang.Unroll
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class GsPathTest extends Specification {

    private Map<String,GsFileSystem> fileSystems

    def setup() {
        fileSystems = [:]
    }

    private fsmock(String bucket) {
        def fs = fileSystems.get(bucket)
        if( !fs ) {
            fs = Mock(GsFileSystem)
            fs.getBucket() >> "/$bucket".toString()

            def provider = Mock(GsFileSystemProvider)
            provider.getPath(_) >> { URI uri->
                def b=uri.authority;
                new GsPath(fsmock(b), Paths.get("/$b/${uri.path}"), false)
            }
            fs.provider() >> provider

            fileSystems[bucket] = fs
        }
        return fs
    }

    private GsPath getPath(String str) {
        def path = Paths.get(str)
        def bucket = path.getName(0).toString()

        if( path.isAbsolute() ) {
            return new GsPath(fsmock(bucket), path, false)
        }
        else {
            return new GsPath(fsmock(""), path, false)
        }

    }

    @Unroll
    def 'should create a path: #objectName'() {

        given:
        def fs = Mock(GsFileSystem)
        fs.getBucket() >> 'bucket'

        when:
        def path = new GsPath(fs, objectName)
        then:
        path.toString() == expected
        path.directory == dir

        where:
        objectName              | expected              | dir
        'file.txt'              | '/bucket/file.txt'    | false
        '/file.txt'             | '/bucket/file.txt'    | false
        '/file.txt'             | '/bucket/file.txt'    | false
        '/a/b/c'                | '/bucket/a/b/c'       | false
        '/a/b/c/'               | '/bucket/a/b/c'       | true
        null                    | '/bucket'             | true
        ''                      | '/bucket'             | true
        '/'                     | '/bucket'             | true

    }

    def 'should validate blob constructor and cached attributes'() {
        given:
        def fs = Mock(GsFileSystem)
        fs.getBucket() >> 'alpha'
        def blob = Mock(Blob)
        blob.getBucket() >> 'alpha'
        blob.getName() >> 'beta/delta'
        blob.getSize() >> 100

        when:
        def path = new GsPath(fs, blob)
        then:
        path.bucketName == 'alpha'
        path.objectName == 'beta/delta'

        when:
        def attrs = path.attributesCache()
        then:
        attrs == new GsFileAttributes(blob)

        expect:
        path.attributesCache() == null
    }

    def 'should validate equals and hashCode'() {

        when:
        def path1 = getPath('/bucket/some/file-name.txt')
        def path2 = getPath('/bucket/some/file-name.txt')
        def path3 = getPath('/bucket/other/file-name.txt')
        def path4 = getPath('/bucket2/some/file-name.txt')

        then:
        path1 == path2
        path1 != path3
        path1 != path4

        path1.hashCode() == path2.hashCode()
        path1.hashCode() != path3.hashCode()

        when:
        def rel1 = getPath('file.txt')
        def rel2 = getPath('file.txt')
        then:
        rel1 == rel2
        rel1.hashCode() == rel2.hashCode()
    }

    def 'should validate isAbsolute'() {

        when:
        def path1 = getPath('/some/file-name.txt')
        def path2 = getPath('file-name.txt')

        then:
        path1.isAbsolute()
        !path2.isAbsolute()

    }

    def 'should validate getRoot'() {
        when:
        def path1 = getPath('/bucket/some/file-name.txt')
        def path2 = getPath('file-name.txt')

        then:
        path1.root == getPath('/bucket')
        path1.root.toString() == '/bucket'
        path2.root == null

    }

    @Unroll
    def 'should return bucket name and id' () {

        when:
        def p = getPath(path)
        then:
        p.isBucket() == expected
        p.getBucketName() == bucketName
        p.getBlobId() == blobId

        where:
        path                                    | expected  |  bucketName       | blobId
        '/nxf-bucket/file-name.txt'             | false     | 'nxf-bucket'      | BlobId.of('nxf-bucket', 'file-name.txt')
        '/nxf-bucket/some/data/file-name.txt'   | false     | 'nxf-bucket'      | BlobId.of('nxf-bucket', 'some/data/file-name.txt')
        'file-name.txt'                         | false     | null              | null
        '/nxf-bucket'                           | true      | 'nxf-bucket'      | null
    }

    @Unroll
    def 'should validate getFileName'() {

        expect:
        getPath(path).getFileName() == getPath(fileName)

        where:
        path                                    | fileName
        '/nxf-bucket/file-name.txt'             | 'file-name.txt'
        '/nxf-bucket/some/data/file-name.txt'   | 'file-name.txt'
        'file-name.txt'                         | 'file-name.txt'
        '/nxf-bucket'                           | 'nxf-bucket'

    }


    @Unroll
    def 'should validate getParent: #path'() {

        expect:
        getPath(path).getParent() == (parent ? getPath(parent) : null)

        where:
        path                                    | parent
        '/nxf-bucket/some/data/file-name.txt'   | '/nxf-bucket/some/data'
        '/nxf-bucket/file-name.txt'             | '/nxf-bucket'
        'file-name.txt'                         | null
        '/nxf-bucket'                           | null
    }

    @Unroll
    def 'should validate toUri'() {

        expect:
        getPath(path).toUri() == new URI(uri)

        where:
        path                            | uri
        '/alpha/some/file.txt'          | 'gs://alpha/some/file.txt'
        '/alpha/'                       | 'gs://alpha'
        '/alpha'                        | 'gs://alpha'
        'some-file.txt'                 | 'gs:some-file.txt'
    }


    @Unroll
    def 'should validate toString: #path'() {

        expect:
        getPath(path).toString() == str

        where:
        path                    | str
        '/alpha/some/file.txt'  | '/alpha/some/file.txt'
        '/alpha'                | '/alpha'
        '/alpha/'               | '/alpha'
        'some-file.txt'         | 'some-file.txt'
    }

    @Unroll
    def 'should validate resolve: base:=#base; path=#path'() {

        expect:
        getPath(base).resolve(path) == getPath(expected)
        getPath(base).resolve(getPath(path)) == getPath(expected)

        where:
        base                        | path                          | expected
        '/nxf-bucket/some/path'     | 'file-name.txt'               | '/nxf-bucket/some/path/file-name.txt'
        '/nxf-bucket/data'          | 'path/file-name.txt'          | '/nxf-bucket/data/path/file-name.txt'
        '/nxf-bucket/data'          | '/other/file-name.txt'        | '/other/file-name.txt'
        '/nxf-bucket'               | 'some/file-name.txt'          | '/nxf-bucket/some/file-name.txt'
    }

    @Unroll
    def 'should validate subpath: #expected'() {
        expect:
        getPath(path).subpath(from, to) == getPath(expected)
        where:
        path                                | from  | to    | expected
        '/bucket/some/big/data/file.txt'    | 0     | 2     | 'bucket/some'
        '/bucket/some/big/data/file.txt'    | 1     | 2     | 'some'
        '/bucket/some/big/data/file.txt'    | 4     | 5     | 'file.txt'
    }

    @Unroll
    def 'should validate startsWith'() {
        expect:
        getPath(path).startsWith(prefix) == expected
        getPath(path).startsWith(getPath(prefix)) == expected
        where:
        path                            | prefix            | expected
        '/bucket/some/data/file.txt'    | '/bucket/some'    | true
        '/bucket/some/data/file.txt'    | '/bucket/'        | true
        '/bucket/some/data/file.txt'    | '/bucket'         | true
        '/bucket/some/data/file.txt'    | 'file.txt'        | false
        'data/file.txt'                 | 'data'            | true
        'data/file.txt'                 | 'file.txt'        | false

    }

    def 'should validate endsWith'() {
        expect:
        getPath(path).endsWith(suffix) == expected
        getPath(path).endsWith(getPath(suffix)) == expected
        where:
        path                            | suffix            | expected
        '/bucket/some/data/file.txt'    | 'file.txt'        | true
        '/bucket/some/data/file.txt'    | 'data/file.txt'   | true
        '/bucket/some/data/file.txt'    | '/data/file.txt'  | false
        '/bucket/some/data/file.txt'    | '/bucket'         | false
        'data/file.txt'                 | 'data'            | false
        'data/file.txt'                 | 'file.txt'        | true

    }


    def 'should validate normalise'() {
        expect:
        getPath(path).normalize() == getPath(expected)
        where:
        path                            | expected
        '/bucket/some/data/file.txt'    | '/bucket/some/data/file.txt'
        '/bucket/some/../file.txt'      | '/bucket/file.txt'
        'bucket/some/../file.txt'       | 'bucket/file.txt'
        'file.txt'                       | 'file.txt'

    }

    @Unroll
    def 'should validate resolveSibling' () {
        expect:
        getPath(base).resolveSibling(path) == getPath(expected)
        getPath(base).resolveSibling(getPath(path)) == getPath(expected)

        where:
        base                        | path                          | expected
        '/nxf-bucket/some/path'     | 'file-name.txt'               | '/nxf-bucket/some/file-name.txt'
        '/nxf-bucket/data'          | 'path/file-name.txt'          | '/nxf-bucket/path/file-name.txt'
        '/nxf-bucket/data'          | '/other/file-name.txt'        | '/other/file-name.txt'
        '/nxf-bucket'               | 'some/file-name.txt'          | '/some/file-name.txt'
    }

    @Unroll
    def 'should validate relativize' () {
        expect:
        getPath(path).relativize(getPath(other)) == getPath(expected)
        where:
        path                    | other                                 | expected
        '/nxf-bucket/some/path' | '/nxf-bucket/some/path/data/file.txt' | 'data/file.txt'
    }

    def 'should validate toAbsolutePath' () {
        expect:
        getPath('/bucket/data/file.txt').toAbsolutePath() == getPath('/bucket/data/file.txt')

        when:
        getPath('file.txt').toAbsolutePath()
        then:
        thrown(UnsupportedOperationException)
    }

    def 'should validate toRealPath' () {
        expect:
        getPath('/bucket/data/file.txt').toRealPath() == getPath('/bucket/data/file.txt')

        when:
        getPath('file.txt').toRealPath()
        then:
        thrown(UnsupportedOperationException)
    }

    def 'should validate iterator' () {
        given:
        def itr = getPath('/nxf-bucket/some/file-name.txt').iterator()
        expect:
        itr.hasNext()
        itr.next() == getPath('nxf-bucket')
        itr.hasNext()
        itr.next() == getPath('some')
        itr.hasNext()
        itr.next() == getPath('file-name.txt')
        !itr.hasNext()

    }
}
