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

    GCSBlobStore(String bucket) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        this.bucket = storage.get(bucket);
    }

    // From interface BlobStore.
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new GCSBlobContainer(path, bucket);
    }

    // From interface Closeable.
    @Override
    public void close() { }
}
