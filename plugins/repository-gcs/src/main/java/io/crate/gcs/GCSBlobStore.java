package io.crate.gcs;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import com.google.cloud.storage.*;

/**
 * "An interface for storing blobs."
 */
public class GCSBlobStore implements BlobStore {

    private final Bucket bucket;

    GCSBlobStore(Bucket bucket) {
        this.bucket = bucket;
    }

    // From interface BlobStore.
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new GCSBlobContainer(bucket, path);
    }

    // From interface Closeable.
    @Override
    public void close() { }
}
