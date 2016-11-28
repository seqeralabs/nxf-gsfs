package nextflow.file.gs
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.Files
import java.nio.file.NoSuchFileException

import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Requires( { env['GCLOUD_SERVICE_KEY'] && env['GOOGLE_PROJECT_ID'] } )
class GsFilesTest extends Specification  {

    static Random RND = new Random()

    @Shared
    GsFileSystemProvider provider

    @Shared
    GsFileSystem fs

    @Shared
    GsPath bucket

    @Shared
    Storage storage

    def setupSpec() {
        def credentials = System.getenv('GCLOUD_SERVICE_KEY').decodeBase64()
        def projectId = System.getenv('GOOGLE_PROJECT_ID')
        def file = Files.createTempFile('gcloud-keys',null).toFile()
        file.deleteOnExit()
        file.text = new String(credentials)
        provider = GsFileSystemProvider.create(file, projectId)
        fs = provider.fs
        storage = fs.storage

        // -- create a bucket
        bucket = newBucketName()
        storage.create( BucketInfo.of(bucket.bucketName) )

    }

    private GsPath newBucketName() {
        def name
        while( true ) {
            name = "nxf-${rnd()}"
            if( storage.get(name) == null ) break
        }
        return new GsPath(fs,name)
    }

    private GsPath newFileName(GsPath dir, String prefix=null, String suffix='txt') {
        def name
        GsPath result
        while( true ) {
            name = "${prefix ?: 'nxf'}-${rnd()}.$suffix"
            result = dir.resolve(name)
            def info = result.getBlobId()
            if( storage.get(info) == null ) break
        }
        return result
    }




    static String rnd() {
        Integer.toHexString(RND.nextInt(Integer.MAX_VALUE))
    }



    def 'should create and delete directories' () {

        given:
        def dir = newFileName(bucket, 'dir')

        when:
        provider.createDirectory(dir)
        then:
        provider.exists(dir)

        when:
        provider.delete(dir)
        then:
        !provider.exists(dir)

    }

    def 'should create and delete a top level dir ie. a bucket' () {

        given:
        def bucket = newBucketName()

        when:
        provider.createDirectory(bucket)
        then:
        provider.exists(bucket)

        when:
        provider.delete(bucket)
        then:
        !provider.exists(bucket)

    }

    def 'should throw a NoSuchFileException when deleting a bucket not existing' () {

        given:
        def bucket = newBucketName()

        when:
        provider.delete(bucket)
        then:
        thrown(NoSuchFileException)

    }

    def 'should throw a NoSuchFileException when deleting a file not existing' () {
        given:
        def path = newFileName(bucket, 'file')

        when:
        provider.delete(path)
        then:
        thrown(NoSuchFileException)

    }

    def 'should throw a DirectoryNotEmptyException' () {

        given:
        def dir = newFileName(bucket, 'dir')
        def file = newFileName(dir, 'file')

        when:
        provider.createDirectory(dir)
        provider.createFile(file, 'Hello world')
        then:
        provider.exists(dir)
        provider.exists(file)
        Files.isDirectory(dir)

        when:
        provider.delete(dir)
        then:
        thrown(DirectoryNotEmptyException)

        when:
        provider.delete(bucket)
        then:
        thrown(DirectoryNotEmptyException)
    }

    def 'should read & write a stream' () {

        given:
        def EXPECTED = 'Hello world!'
        def file = newFileName(bucket, "file")

        def SAMPLE = Files.createTempFile('test',null)
        SAMPLE.text = EXPECTED

        when:
        // create the file using a `newOutputStream`
        def _out = provider.newOutputStream(file)
        Files.copy(SAMPLE, _out)
        _out.close()

        // read it back
        def _in = provider.newInputStream(file)
        def result = new BufferedReader(new InputStreamReader(_in)).text
        _in.close()

        then:
        result == EXPECTED

        cleanup:
        Files.delete(SAMPLE)
    }

    def 'should copy a file'  ()  {

        given:
        def EXPECTED = 'Hello world!'
        // create the source
        def source = newFileName(bucket, 'file')
        source.text = EXPECTED
        def target = newFileName(bucket, "copy")

        when:
        provider.copy(source, target)
        then:
        target.text == EXPECTED
        provider.exists(target)
        provider.exists(source)

    }

    def 'should move a file'  ()  {

        given:
        def EXPECTED = 'Hello world!'
        // create the source
        def source = newFileName(bucket, 'file')
        source.text = EXPECTED
        def target = newFileName(bucket, "copy")

        when:
        provider.move(source, target)
        then:
        target.text == EXPECTED
        provider.exists(target)
        !provider.exists(source)

    }
}
