package nextflow.file.gs
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

import com.google.cloud.storage.Blob
import groovy.transform.CompileStatic

/**
 * * Models file attributes view for a Google Cloud Storage
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class GsFileAttributesView implements BasicFileAttributeView {

    private Blob blob

    GsFileAttributesView( Blob blob )  {
        this.blob = blob
    }

    @Override
    String name() {
        return 'basic'
    }

    @Override
    BasicFileAttributes readAttributes() throws IOException {
        return new GsFileAttributes(blob)
    }

    /**
     * This API is implemented is not supported but instead of throwing an exception just do nothing
     * to not break the method {@link java.nio.file.CopyMoveHelper#copyToForeignTarget(java.nio.file.Path, java.nio.file.Path, java.nio.file.CopyOption...)}
     *
     * @param lastModifiedTime
     * @param lastAccessTime
     * @param createTime
     * @throws IOException
     */
    @Override
    void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        // ignore
    }
}
