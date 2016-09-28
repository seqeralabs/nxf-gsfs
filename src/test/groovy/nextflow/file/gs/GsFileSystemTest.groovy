package nextflow.file.gs

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class GsFileSystemTest extends Specification  {

    def 'should return root paths' () {

        given:
        def provider = Mock(GsFileSystemProvider)

        when:
        def roots = new GsFileSystem(provider, CREDENTIALS, PROJECT).getRootDirectories()
        roots.each { println it }
        then:
        roots.size()>0
    }


    def 'should iterate the bucket' () {

        given:
        def provider = Mock(GsFileSystemProvider)
        def fs = new GsFileSystem(provider, CREDENTIALS, PROJECT)

        when:
        def stream = fs.newDirectoryStream(new GsPath(fs, 'cloudflow'), null)
        stream.iterator().each { println it }
        then:
        true
    }

    def 'should create a new byte steam' () {

        given:
        def provider = Mock(GsFileSystemProvider)
        def fs = new GsFileSystem(provider, CREDENTIALS, PROJECT)

        when:
        def channel = fs.newReadableByteChannel(fs.getPath('cloudflow','work/1a/e3302bb84ed79cf7f3ede7e10cff68/.command.out'))
        def str = read(channel)
        then:
        str == 'Bonjour world!\n'
    }


    private String read(SeekableByteChannel sbc) {

        // We open the file in order to read it ()
        def result = new StringBuilder()
        try  {

            ByteBuffer buff = ByteBuffer.allocate(1024);
            // Position is set to 0
            buff.clear();

            // We use the current encoding to read
            String encoding = 'UTF-8'

            // While the number of bytes from the channel are > 0
            while(sbc.read(buff)>0) {

                // Prepare the data to be written
                buff.flip();

                // Usins the current enconding we decode the bytes read
                result.append(Charset.forName(encoding).decode(buff))

                // Prepare the buffer for a new read
                buff.clear();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return result.toString()
    }
}
