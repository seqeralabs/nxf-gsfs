package nextflow.file.gs

import com.google.cloud.storage.Blob
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class GsPathTest extends Specification {

    def 'should create a path' () {

        given:
        def fs = Mock(GsFileSystem)
        def path

        when:
        path = new GsPath(fs,'nxf-bucket')
        then:
        path.bucketName == 'nxf-bucket'
        path.objectName == null
        path.fileName == new GsPath(fs,null,'nxf-bucket')
        path.parent == null

        when:
        path = new GsPath(fs,'nxf-bucket', 'some/path/name.txt')
        then:
        path.bucketName == 'nxf-bucket'
        path.objectName == 'some/path/name.txt'
        path.fileName == new GsPath(fs, null, 'name.txt')
        path.parent == new GsPath(fs,'nxf-bucket','some/path')

        when:
        path = new GsPath(fs,'nxf-bucket', '/some/path/name.txt')
        then:
        path.bucketName == 'nxf-bucket'
        path.objectName == 'some/path/name.txt'
        path.fileName == new GsPath(fs, null, 'name.txt')
        path.parent == new GsPath(fs,'nxf-bucket','some/path/')

        when:
        path = new GsPath(fs, null, 'file-name.txt')
        then:
        path.bucketName == null
        path.objectName == 'file-name.txt'
        path.fileName == new GsPath(fs, null, 'file-name.txt')
        path.parent == null

        when:
        new GsPath(fs,'/nxf-bucket')
        then:
        thrown(IllegalArgumentException)

        when:
        new GsPath(fs, null, null)
        then:
        thrown(IllegalArgumentException)

    }

    def 'should validate blob constructor and cached attributes' () {
        given:
        def fs = Mock(GsFileSystem)
        def blob = Mock(Blob)
        blob.bucket() >> 'alpha'
        blob.name() >> 'beta/delta'
        blob.size() >> 100

        when:
        def path = new GsPath(fs,blob)
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

    def 'should validate equals and hashCode' () {

        given:
        def fs = Mock(GsFileSystem)

        when:
        def path1 = new GsPath(fs,'nxf-bucket', 'some/file-name.txt')
        def path2 = new GsPath(fs,'nxf-bucket', 'some/file-name.txt')
        def path3 = new GsPath(fs,'nxf-bucket', 'other/file-name.txt')

        then:
        path1 == path2
        path1 != path3

        path1.hashCode() == path2.hashCode()
        path1.hashCode() != path3.hashCode()
    }

    def 'should validate isAbsolute' () {
        given:
        def fs = Mock(GsFileSystem)

        when:
        def path1 = new GsPath(fs,'nxf-bucket', 'some/file-name.txt')
        def path2 = new GsPath(fs, null, 'file-name.txt')

        then:
        path1.isAbsolute()
        !path2.isAbsolute()

    }

    def 'should validate getRoot' () {
        given:
        def fs = Mock(GsFileSystem)

        when:
        def path1 = new GsPath(fs,'nxf-bucket', 'some/file-name.txt')
        def path2 = new GsPath(fs, null, 'file-name.txt')

        then:
        path1.root == new GsPath(fs,'nxf-bucket')
        path2.root == null

    }

    def 'should validate getFileName' () {

        given:
        def fs = Mock(GsFileSystem)

        when:
        def path1 = new GsPath(fs,'nxf-bucket', 'some/file-name.txt')
        then:
        path1.getFileName() == new GsPath(fs,null,'file-name.txt')

        when:
        def path2 = new GsPath(fs, null, 'file-name.txt')
        then:
        path2.getFileName() == new GsPath(fs,null,'file-name.txt')

        when:
        def path3 = new GsPath(fs, 'nxf-bucket', 'file-name.txt')
        then:
        path3.getFileName() == new GsPath(fs,null,'file-name.txt')

        when:
        def path4 = new GsPath(fs, 'nxf-bucket')
        then:
        path4.getFileName() == null

    }


    def 'should validate getParent' () {

        given:
        def fs = Mock(GsFileSystem)

        when:
        def path1 = new GsPath(fs,'nxf-bucket', 'some/data/file-name.txt')
        then:
        path1.getParent() == new GsPath(fs,'nxf-bucket','some/data')

        when:
        def path2 = new GsPath(fs, 'nxf-bucket', 'file-name.txt')
        then:
        path2.getParent() == new GsPath(fs,'nxf-bucket')

        when:
        def path3 = new GsPath(fs, null, 'file-name.txt')
        then:
        path3.getParent() == null

        when:
        def path4 = new GsPath(fs, 'nxf-bucket')
        then:
        path4.getParent() == null

    }


    def 'should validate toUru' () {

        given:
        def fs = Mock(GsFileSystem)

        expect:
        new GsPath(fs, 'alpha', 'some/file.txt').toUri() == new URI('gs://alpha/some/file.txt')
        new GsPath(fs, 'alpha' ).toUri() == new URI('gs://alpha')
        new GsPath(fs, null, 'some-file.txt' ).toUri() == new URI('gs:some-file.txt')
    }

    def testURI() {

        expect:
        new URI('gs:some-file.txt').toString() == 'gs:some-file.txt'

    }


    def 'should validate toString' () {

        given:
        def fs = Mock(GsFileSystem)

        expect:
        new GsPath(fs, 'alpha', 'some/file.txt').toString() == '/alpha/some/file.txt'
        new GsPath(fs, 'alpha' ).toString() == '/alpha'
        new GsPath(fs, null, 'some-file.txt' ).toString() == 'some-file.txt'
    }

    def 'should validate resolve' () {

        given:
        def fs = Mock(GsFileSystem)

        expect:
        new GsPath(fs,'bucket','some/path').resolve('file-name.txt').toString() == '/bucket/some/path/file-name.txt'
        new GsPath(fs,'bucket','data').resolve('path/file-name.txt').toString() == '/bucket/data/path/file-name.txt'
        new GsPath(fs,'bucket','data').resolve('/path/file-name.txt').toString() == '/bucket/path/file-name.txt'
        new GsPath(fs,'bucket').resolve('some/file-name.txt').toString() == '/bucket/some/file-name.txt'
        new GsPath(fs,'bucket').resolve('/some/file-name.txt').toString() == '/bucket/some/file-name.txt'
    }

}
