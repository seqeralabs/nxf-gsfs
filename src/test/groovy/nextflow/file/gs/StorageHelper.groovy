package nextflow.file.gs

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.Path
import java.nio.file.Paths

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
trait StorageHelper {

    static private Random RND = new Random()


    static private String rnd() {
        Integer.toHexString(RND.nextInt(Integer.MAX_VALUE))
    }

    abstract Storage getStorage()

    Storage createStorage(File credentials, String projectId) {
        StorageOptions
                .newBuilder()
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credentials)))
                .setProjectId(projectId)
                .build()
                .getService()
    }

    String createBucket(String bucketName) {
        final info = BucketInfo.of(bucketName)
        storage.create(info)
        return bucketName
    }

    String createBucket() {
        createBucket(getRndBucketName())
    }

    String getRndBucketName() {
        def bucketName = null
        while( !bucketName || existsPath(bucketName) ) {
            bucketName = "nxf-gcloud-${rnd()}"
        }

        return bucketName
    }

    BlobId getBlobId(String path) {
        getBlobId(Paths.get(path))
    }

    BlobId getBlobId(Path path) {
        if( path.nameCount<2 )
            throw new IllegalArgumentException("Not a valid GS path: $path")

        def bucket = path.getName(0).toString()
        def object = path.subpath(1,path.nameCount).toString()
        BlobId.of(bucket, object)
    }

    def createObject(String path, String content) {
        createObject(Paths.get(path), content)
    }

    def createObject(Path path, String content) {
        final blobId = getBlobId(path)
        final info = BlobInfo.newBuilder(blobId).build()
        storage.create(info, content.bytes)
    }

    def deleteObject(String path) {
        storage.delete(getBlobId(path))
    }

    def deleteBucket(Path path) {
        assert path.nameCount == 1
        deleteBucket(path.getName(0).toString())
    }

    def deleteBucket(String bucketName) {
        if( !bucketName )
            return

        final bucket = storage.get(bucketName)
        if( !bucket )
            return

        bucket.list().iterateAll().each { Blob blob ->  storage.delete(blob.getBlobId()) }
        storage.delete(bucketName)
    }

    boolean existsPath(String path) {
        existsPath(Paths.get(path))
    }

    boolean existsPath(Path path) {
        if( path.nameCount == 1 ) {
            def bucket = path.getName(0).toString()
            return storage.get(bucket) != null
        }
        else {
            return storage.get(getBlobId(path)) != null
        }
    }

    String readObject(String path) {
        readObject(Paths.get(path))
    }

    String readObject(Path path) {
        def blobId = getBlobId(path)
        def blob = storage.get(blobId)
        return new String(blob.getContent())
    }


    String randomText(int size) {
        def result = new StringBuilder()
        while( result.size() < size ) {
            result << UUID.randomUUID().toString() << '\n'
        }
        return result.toString()
    }

    String readChannel( SeekableByteChannel sbc, int buffLen )  {
        def buffer = new ByteArrayOutputStream()
        ByteBuffer bf = ByteBuffer.allocate(buffLen)
        while((sbc.read(bf))>0) {
            bf.flip();
            buffer.write(bf.array(), 0, bf.limit())
            bf.clear();
        }

        buffer.toString()
    }

    void writeChannel( SeekableByteChannel channel, String content, int buffLen ) {

        def bytes = content.getBytes()
        ByteBuffer buf = ByteBuffer.allocate(buffLen);
        int i=0
        while( i < bytes.size()) {

            def len = Math.min(buffLen, bytes.size()-i);
            buf.clear();
            buf.put(bytes, i, len);
            buf.flip();
            channel.write(buf);

            i += len
        }

    }
}