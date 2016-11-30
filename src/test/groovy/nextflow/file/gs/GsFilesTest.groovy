package nextflow.file.gs

import java.nio.charset.Charset
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import spock.lang.Ignore
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Requires( { env['GCLOUD_SERVICE_KEY'] && env['GOOGLE_PROJECT_ID'] } )
class GsFilesTest extends Specification implements StorageHelper {


    @Shared
    private Storage _storage

    Storage getStorage() { _storage }

    def setupSpec() {
        def credentials = System.getenv('GCLOUD_SERVICE_KEY').decodeBase64()
        String projectId = System.getenv('GOOGLE_PROJECT_ID')
        def file = Files.createTempFile('gcloud-keys',null).toFile()
        file.deleteOnExit()
        file.text = new String(credentials)
        this._storage = createStorage(file, projectId)
        //
        System.setProperty('GOOGLE_APPLICATION_CREDENTIALS', file.toString())
        System.setProperty('GOOGLE_PROJECT_ID', projectId)
    }


    def 'should write a file' () {
        given:
        def TEXT = "Hello world!"
        def bucket = createBucket()
        def path = Paths.get(new URI("gs://$bucket/file-name.txt"))

        when:
        //TODO check write options
        Files.write(path, TEXT.bytes)
        then:
        existsPath("$bucket/file-name.txt")
        readObject(path) == TEXT

        cleanup:
        if( bucket ) deleteBucket(bucket)
    }

    def 'should read a file' () {
        given:
        def TEXT = "Hello world!"
        def bucket = createBucket()
        def path = Paths.get(new URI("gs://$bucket/file-name.txt"))

        when:
        createObject("$bucket/file-name.txt", TEXT)
        then:
        new String(Files.readAllBytes(path)) == TEXT
        Files.readAllLines(path).get(0) == TEXT

        cleanup:
        if( bucket ) deleteBucket(bucket)
    }

    def 'should read file attributes' () {
        given:
        final start = System.currentTimeMillis()
        final TEXT = "Hello world!"
        final bucket = createBucket()
        final keyName = "$bucket/data/alpha.txt"
        createObject(keyName, TEXT)

        when:
        def path = Paths.get(new URI("gs://$keyName"))
        def attrs = Files.readAttributes(path, BasicFileAttributes)
        then:
        attrs.isRegularFile()
        !attrs.isDirectory()
        attrs.size() == 12
        !attrs.isSymbolicLink()
        !attrs.isOther()
        attrs.fileKey() == keyName
        attrs.lastAccessTime() == null
        attrs.lastModifiedTime().toMillis()-start < 5_000
        attrs.creationTime().toMillis()-start < 5_000

        when:
        def time = Files.getLastModifiedTime(path)
        then:
        time == attrs.lastModifiedTime()

        when:
        def view = Files.getFileAttributeView(path, BasicFileAttributeView)
        then:
        view.readAttributes() == attrs

        when:
        attrs = Files.readAttributes(path.getParent(), BasicFileAttributes)
        then:
        !attrs.isRegularFile()
        attrs.isDirectory()
        attrs.size() == 0
        !attrs.isSymbolicLink()
        !attrs.isOther()
        attrs.fileKey() == "$bucket/data/"
        attrs.lastAccessTime() == null
        attrs.lastModifiedTime() == null
        attrs.creationTime() == null

        cleanup:
        if( bucket ) deleteBucket(bucket)
    }

    def 'should copy a stream to bucket' () {
        given:
        final TEXT = "Hello world!"
        final bucketName = createBucket()
        final target = Paths.get(new URI("gs://$bucketName/data/file.txt"))

        when:
        def stream = new ByteArrayInputStream(new String(TEXT).bytes)
        Files.copy(stream, target)
        then:
        existsPath(target)
        readObject(target) == TEXT

        when:
        stream = new ByteArrayInputStream(new String(TEXT).bytes)
        Files.copy(stream, target, StandardCopyOption.REPLACE_EXISTING)
        then:
        existsPath(target)
        readObject(target) == TEXT

        when:
        stream = new ByteArrayInputStream(new String(TEXT).bytes)
        Files.copy(stream, target)
        then:
        thrown(FileAlreadyExistsException)

        cleanup:
        if( bucketName ) deleteBucket(bucketName)
    }

    def 'copy local file to a bucket' () {
        given:
        final TEXT = "Hello world!"
        final bucketName = createBucket()
        final target = Paths.get(new URI("gs://$bucketName/data/file.txt"))
        final source = Files.createTempFile('test','nf')
        source.text = TEXT

        when:
        Files.copy(source, target)
        then:
        readObject(target) == TEXT

        cleanup:
        if( source ) Files.delete(source)
        if( bucketName ) deleteBucket(bucketName)
    }

    def 'copy a remote file to a bucket' () {
        given:
        final TEXT = "Hello world!"
        final bucketName = createBucket()
        final target = Paths.get(new URI("gs://$bucketName/target/file.txt"))

        and:
        final objectName = "$bucketName/source/file.txt"
        final source = Paths.get(new URI("gs://$objectName"))
        createObject(objectName, TEXT)

        when:
        Files.copy(source, target)
        then:
        existsPath(source)
        existsPath(target)
        readObject(target) == TEXT

        cleanup:
        if( source ) Files.delete(source)
        if( bucketName ) deleteBucket(bucketName)
    }

    def 'move a remote file to a bucket' () {
        given:
        final TEXT = "Hello world!"
        final bucketName = createBucket()
        final target = Paths.get(new URI("gs://$bucketName/target/file.txt"))

        and:
        final objectName = "$bucketName/source/file.txt"
        final source = Paths.get(new URI("gs://$objectName"))
        createObject(objectName, TEXT)

        when:
        Files.move(source, target)
        then:
        !existsPath(source)
        existsPath(target)
        readObject(target) == TEXT

        cleanup:
        if( bucketName ) deleteBucket(bucketName)
    }

    def 'should create a directory' () {

        given:
        def bucketName = getRndBucketName()
        def dir = Paths.get(new URI("gs://$bucketName"))

        when:
        Files.createDirectory(dir)
        then:
        existsPath(dir)

        cleanup:
        if(bucketName) {
            deleteBucket(bucketName)
        }
    }

    def 'should create a directory tree' () {
        given:
        def bucketName = getRndBucketName()
        def dir = Paths.get(new URI("gs://$bucketName/alpha/bravo"))

        when:
        Files.createDirectories(dir)
        then:
        existsPath(dir)

        cleanup:
        deleteBucket(bucketName)
    }


    def 'should create a file' () {
        given:
        final bucketName = createBucket()

        when:
        def path = Paths.get(new URI("gs://$bucketName/data/file.txt"))
        Files.createFile(path)
        then:
        existsPath(path)

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should create temp file and directory' () {
        given:
        final bucketName = createBucket()
        final base = Paths.get(new URI("gs://$bucketName"))

        when:
        def t1 = Files.createTempDirectory(base, 'test')
        then:
        existsPath(t1)

        when:
        def t2 = Files.createTempFile(base, 'prefix', 'suffix')
        then:
        existsPath(t2)

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should delete a file' () {
        given:
        final bucketName = createBucket()
        final target = Paths.get(new URI("gs://$bucketName/data/file.txt"))
        and:
        createObject(target.toString(), 'HELLO WORLD')

        when:
        Files.delete(target)
        then:
        !existsPath(target)

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should delete a bucket' () {
        given:
        final bucketName = createBucket()

        when:
        Files.delete(Paths.get(new URI("gs://$bucketName")))
        then:
        !existsPath(bucketName)

    }

    def 'should throw when deleting a not empty bucket' () {
        given:
        final bucketName = createBucket()
        and:
        createObject("$bucketName/this/that", 'HELLO')

        when:
        def path1 = new URI("gs://$bucketName")
        Files.delete(Paths.get(path1))
        then:
        thrown(DirectoryNotEmptyException)

        when:
        def path2 = new URI("gs://$bucketName/this")
        Files.delete(Paths.get(path2))
        then:
        thrown(DirectoryNotEmptyException)

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should throw a NoSuchFileException when deleting a bucket not existing' () {

        given:
        def bucketName = getRndBucketName()
        def path = Paths.get(new URI("gs://$bucketName/alpha/bravo"))

        when:
        Files.delete(path)
        then:
        thrown(NoSuchFileException)

    }

    def 'should validate exists method' () {
        given:
        final bucketName = createBucket()
        and:
        final missingBucket = getRndBucketName()
        and:
        createObject("$bucketName/file.txt", 'HELLO')

        expect:
        Files.exists(Paths.get(new URI("gs://$bucketName")))
        Files.exists(Paths.get(new URI("gs://$bucketName/file.txt")))
        !Files.exists(Paths.get(new URI("gs://$bucketName/fooooo.txt")))
        !Files.exists(Paths.get(new URI("gs://$missingBucket")))

        cleanup:
        deleteBucket(bucketName)

    }

    def 'should check is it is a directory' () {
        given:
        final bucketName = createBucket()

        when:
        def path = Paths.get(new URI("gs://$bucketName"))
        then:
        Files.isDirectory(path)
        !Files.isRegularFile(path)

        when:
        def file = path.resolve('this/and/that')
        createObject(file, 'Hello world')
        then:
        !Files.isDirectory(file)
        Files.isRegularFile(file)
        Files.isReadable(file)
        Files.isWritable(file)
        !Files.isExecutable(file)
        !Files.isSymbolicLink(file)

        expect:
        Files.isDirectory(file.parent)
        !Files.isRegularFile(file.parent)

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should check that is the same file' () {

        given:
        def file1 = Paths.get(new URI("gs://some/data/file.txt"))
        def file2 = Paths.get(new URI("gs://some/data/file.txt"))
        def file3 = Paths.get(new URI("gs://some/data/fooo.txt"))

        expect:
        Files.isSameFile(file1, file2)
        !Files.isSameFile(file1, file3)

    }

    def 'should create a newBufferedReader' () {
        given:
        final bucketName = createBucket()
        and:
        final TEXT = randomText(50 * 1024)
        final path = Paths.get(new URI("gs://$bucketName/file.txt"))
        createObject(path, TEXT)

        when:
        def reader = Files.newBufferedReader(path, Charset.forName('UTF-8'))
        then:
        reader.text == TEXT

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should create a newBufferedWriter' () {
        given:
        final bucketName = createBucket()
        and:
        final TEXT = randomText(50 * 1024)
        final path = Paths.get(new URI("gs://$bucketName/file.txt"))

        when:
        def writer = Files.newBufferedWriter(path, Charset.forName('UTF-8'))
        TEXT.readLines().each { it -> writer.println(it) }
        writer.close()
        then:
        readObject(path) == TEXT

        cleanup:
        deleteBucket(bucketName)
    }


    def 'should create a newInputStream' () {
        given:
        final bucketName = createBucket()
        and:
        final TEXT = randomText(50 * 1024)
        final path = Paths.get(new URI("gs://$bucketName/file.txt"))
        createObject(path, TEXT)

        when:
        def reader = Files.newInputStream(path)
        then:
        reader.text == TEXT

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should create a newOutputStream' () {
        given:
        final bucketName = createBucket()
        and:
        final TEXT = randomText(50 * 1024)
        final path = Paths.get(new URI("gs://$bucketName/file.txt"))

        when:
        def writer = Files.newOutputStream(path)
        TEXT.readLines().each { it -> writer.write(it.bytes); writer.write((int)('\n' as char)) }
        writer.close()
        then:
        readObject(path) == TEXT

        cleanup:
        deleteBucket(bucketName)
    }


    def 'should read a newByteChannel' () {
        given:
        final bucketName = createBucket()
        and:
        final TEXT = randomText(1024)
        final path = Paths.get(new URI("gs://$bucketName/file.txt"))
        createObject(path, TEXT)

        when:
        def channel = Files.newByteChannel(path)
        then:
        readChannel(channel, 100) == TEXT

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should write a byte channel' () {
        given:
        final bucketName = createBucket()
        and:
        final TEXT = randomText(1024)
        final path = Paths.get(new URI("gs://$bucketName/file.txt"))

        when:
        def channel = Files.newByteChannel(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
        writeChannel(channel, TEXT, 200)
        channel.close()
        then:
        readObject(path) == TEXT

        cleanup:
        deleteBucket(bucketName)
    }

    def 'should check file size' () {
        given:
        final bucketName = createBucket()
        and:
        final TEXT = randomText(50 * 1024)
        final path = Paths.get(new URI("gs://$bucketName/file.txt"))

        when:
        createObject(path, TEXT)
        then:
        Files.size(path) == TEXT.size()

        when:
        Files.size(path.resolve('xxx'))
        then:
        thrown(NoSuchFileException)

        cleanup:
        deleteBucket(bucketName)
    }


    def 'should check walkTree' () {

        given:
        final bucketName = createBucket()
        createObject("$bucketName/foo/file1.txt",'A')
        createObject("$bucketName/foo/file2.txt",'BB')
        createObject("$bucketName/foo/bar/file3.txt",'CCC')
        createObject("$bucketName/foo/bar/baz/file4.txt",'DDDD')
        createObject("$bucketName/foo/bar/file5.txt",'EEEEE')
        createObject("$bucketName/foo/file6.txt",'FFFFFF')

        when:
        List<String> dirs = []
        Map<String,BasicFileAttributes> files = [:]
        def base = Paths.get(new URI("gs://$bucketName"))
        Files.walkFileTree(base, new SimpleFileVisitor<Path>() {

            @Override
            FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
            {
                dirs << base.relativize(dir).toString()
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                files[file.getFileName().toString()] = attrs
                return FileVisitResult.CONTINUE;
            }
        })

        then:
        files.size() == 6
        files ['file1.txt'].size() == 1
        files ['file2.txt'].size() == 2
        files ['file3.txt'].size() == 3
        files ['file4.txt'].size() == 4
        files ['file5.txt'].size() == 5
        files ['file6.txt'].size() == 6
        dirs.size() == 4
        dirs.contains("")
        dirs.contains('foo')
        dirs.contains('foo/bar')
        dirs.contains('foo/bar/baz')


        when:
        dirs = []
        files = [:]
        base = Paths.get(new URI("gs://$bucketName/foo/bar/"))
        Files.walkFileTree(base, new SimpleFileVisitor<Path>() {

            @Override
            FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
            {
                dirs << base.relativize(dir).toString()
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                files[file.getFileName().toString()] = attrs
                return FileVisitResult.CONTINUE;
            }
        })

        then:
        println files
        files.size()==3
        files.containsKey('file3.txt')
        files.containsKey('file4.txt')
        files.containsKey('file5.txt')
        dirs.size() == 2
        dirs.contains("")
        dirs.contains('baz')

        cleanup:
        deleteBucket(bucketName)
    }

    @Ignore
    def 'test list dirs' () {
        given:
        final bucketName = createBucket()
        createObject("$bucketName/foo/file1.txt",'A')
        createObject("$bucketName/foo/file2.txt",'BB')
        createObject("$bucketName/foo/bar/file3.txt",'CCC')
        createObject("$bucketName/foo/bar/baz/file4.txt",'DDDD')
        createObject("$bucketName/foo/bar/file5.txt",'EEEEE')
        createObject("$bucketName/foo/file6.txt",'FFFFFF')

        when:
        def opts = [Storage.BlobListOption.currentDirectory()]
        opts << Storage.BlobListOption.prefix('foo/bar')
        def values = storage.list(bucketName, opts as Storage.BlobListOption[]).getValues()
        values.each { Blob it -> println "${it.name}; dir=${it.directory}" }
        then:
        true

        cleanup:
        deleteBucket(bucketName)
    }

}
