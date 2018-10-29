package nextflow.file.gs

import com.google.cloud.storage.Bucket
import groovy.transform.CompileStatic

/**
 * Models attributes of a Google storage bucket
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class GsBucketAttributes extends GsFileAttributes {

    private String location

    private String storageClass

    GsBucketAttributes(Bucket bucket) {
        super (bucket)
        this.location = bucket.location
        this.storageClass = bucket.storageClass
    }

}