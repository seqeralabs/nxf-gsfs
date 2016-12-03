package nextflow.file.gs

import java.nio.file.Paths

import com.google.cloud.Page
import com.google.cloud.storage.Bucket
import com.google.cloud.storage.Storage
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class GsFileSystemTest extends Specification  {


    def 'should get a path' () {
        given:
        final storage = Mock(Storage)
        final provider = Spy(GsFileSystemProvider)
        final foo_fs = new GsFileSystem(provider, storage, 'foo')
        final bar_fs = new GsFileSystem(provider, storage, 'bar')

        when:
        def result = foo_fs.getPath(path, more as String[])
        then:
        call * provider.getFileSystem0(_,true) >> { it, create -> it=='foo' ? foo_fs : bar_fs }
        result.toUriString()

        where:
        call| path                  | more          | expected
        1   | '/foo'                | null          | 'gs://foo/'
        1   | '/foo/'               | null          | 'gs://foo/'
        1   | '/foo/alpha/bravo'    | null          | 'gs://foo/alpha/bravo'
        1   | '/foo/alpha/bravo/'   | null          | 'gs://foo/alpha/bravo/'
        1   | '/bar'                | null          | 'gs://bar/'
        1   | '/bar'                | ['a','b']     | 'gs://bar/a/b'
        1   | '/bar/'               | ['a/','b/']   | 'gs://bar/a/b'
        1   | '/bar/'               | ['/a','/b']   | 'gs://bar/a/b'
        0   | 'this/and/that'       | null          | 'gs:/this/and/that'
        0   | 'this/and/that'       | 'x/y'         | 'gs:/this/and/that/x/y'

    }

    def 'should return root path' () {

        given:
        def provider = Mock(GsFileSystemProvider)
        def storage = Mock(Storage)
        def fs = new GsFileSystem(provider, storage, 'bucket-example')

        expect:
        fs.getRootDirectories() == [ new GsPath(fs, '/bucket-example/') ]
    }

    def 'should return root paths' () {

        given:
        def page = Mock(Page)
        def storage = Mock(Storage)
        and:
        def bucket1 = Mock(Bucket); bucket1.getName() >> 'alpha'
        def bucket2 = Mock(Bucket); bucket2.getName() >> 'beta'
        def bucket3 = Mock(Bucket); bucket3.getName() >> 'delta'
        and:
        def provider = Mock(GsFileSystemProvider)
        and:
        def fs = new GsFileSystem(provider, storage, '/')

        when:
        def roots = fs.getRootDirectories()
        then:
        1 * storage.list() >> page
        1 * page.iterateAll() >> { [bucket1, bucket2, bucket3].iterator() }
        3 * provider.getPath(_ as String) >>  { String it -> new GsPath(fs:Mock(GsFileSystem), path:Paths.get("/$it")) }
        roots.size() == 3
        roots[0].toString() == '/alpha'
        roots[1].toString() == '/beta'
        roots[2].toString() == '/delta'
    }

    def 'should test basic properties' () {

        given:
        def BUCKET_NAME = 'bucket'
        def provider = Stub(GsFileSystemProvider)
        def storage = Stub(Storage)

        when:
        def fs = new GsFileSystem(provider, storage, BUCKET_NAME)
        then:
        fs.getSeparator() == '/'
        fs.isOpen()
        fs.provider() == provider
        fs.bucket == BUCKET_NAME
        !fs.isReadOnly()
        fs.supportedFileAttributeViews() == ['basic'] as Set

        when:
        fs = new GsFileSystem(provider, storage, '/')
        then:
        fs.bucket == '/'
        fs.isReadOnly()
        fs.isOpen()
    }

    def 'should test getPath' () {
        given:
        def BUCKET_NAME = 'bucket'
        def provider = Stub(GsFileSystemProvider)
        def storage = Stub(Storage)
        and:
        def fs = new GsFileSystem(provider, storage, BUCKET_NAME)

        expect:
        fs.getPath('file-name.txt') == new GsPath(fs, Paths.get('file-name.txt'), false)
        fs.getPath('alpha/bravo') == new GsPath(fs, Paths.get('alpha/bravo'), false)
        fs.getPath('/alpha/bravo') == new GsPath(fs, Paths.get('/bucket/alpha/bravo'), false)
        fs.getPath('/alpha','/gamma','/delta') == new GsPath(fs, Paths.get('/bucket/alpha/gamma/delta'), false)
        fs.getPath('/alpha','gamma//','delta//') == new GsPath(fs, Paths.get('/bucket/alpha/gamma/delta'), false)
    }

//
//
//    private String read(SeekableByteChannel sbc) {
//
//        // We open the file in order to read it ()
//        def result = new StringBuilder()
//        try  {
//
//            ByteBuffer buff = ByteBuffer.allocate(1024);
//            // Position is set to 0
//            buff.clear();
//
//            // We use the current encoding to read
//            String encoding = 'UTF-8'
//
//            // While the number of bytes from the channel are > 0
//            while(sbc.read(buff)>0) {
//
//                // Prepare the data to be written
//                buff.flip();
//
//                // Usins the current enconding we decode the bytes read
//                result.append(Charset.forName(encoding).decode(buff))
//
//                // Prepare the buffer for a new read
//                buff.clear();
//            }
//
//        } catch (IOException ioe) {
//            ioe.printStackTrace();
//        }
//
//        return result.toString()
//    }
}
