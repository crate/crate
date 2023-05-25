package io.crate.gcs;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

/**
 * "An interface for storing blobs."
 */
public class GCSBlobStore implements BlobStore {

    GCSBlobStore(String bucket) {
    }

    // From interface BlobStore
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new GCSBlobContainer(path);
    }

    // From interface Closeable
    @Override
    public void close() { }
}
