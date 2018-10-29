/*
 * Copyright (c) 2018, Seqera Labs.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * This Source Code Form is "Incompatible With Secondary Licenses", as
 * defined by the Mozilla Public License, v. 2.0.
 *
 */

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