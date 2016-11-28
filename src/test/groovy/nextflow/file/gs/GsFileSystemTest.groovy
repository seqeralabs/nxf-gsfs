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
        def fs = new GsFileSystem(provider, storage, 'bucket')

        when:
        def roots = fs.getRootDirectories()
        then:
        1 * storage.list() >> page
        1 * page.iterateAll() >> { [bucket1, bucket2, bucket3].iterator() }
        3 * provider.getPath(_) >>  { URI uri -> new GsPath(fs, Paths.get("/${uri.authority}")) }
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
        and:
        def fs = new GsFileSystem(provider, storage, BUCKET_NAME)

        expect:
        fs.getSeparator() == '/'
        fs.isOpen()
        fs.provider() == provider
        fs.bucket == BUCKET_NAME
        !fs.isReadOnly()
        fs.supportedFileAttributeViews() == ['basic'] as Set
    }

    def 'should test getPath' () {
        given:
        def BUCKET_NAME = 'bucket'
        def provider = Stub(GsFileSystemProvider)
        def storage = Stub(Storage)
        and:
        def fs = new GsFileSystem(provider, storage, BUCKET_NAME)

        expect:
        fs.getPath('file-name.txt') == new GsPath(fs, Paths.get('/bucket/file-name.txt'))
        fs.getPath('alpha/bravo') == new GsPath(fs, Paths.get('/bucket/alpha/bravo'))
        fs.getPath('/alpha/bravo') == new GsPath(fs, Paths.get('/bucket/alpha/bravo'))
        fs.getPath('/alpha','/gamma','/delta') == new GsPath(fs, Paths.get('/bucket/alpha/gamma/delta'))
        fs.getPath('/alpha','gamma//','delta//') == new GsPath(fs, Paths.get('/bucket/alpha/gamma/delta'))
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
