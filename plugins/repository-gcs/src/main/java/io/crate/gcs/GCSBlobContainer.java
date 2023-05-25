package io.crate.gcs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * AbstractBlobContainer inherits from BlobContainer which is: "An interface for
 * managing a repository of blob entries, where each blob entry is just a named
 * group of bytes."
 */
public class GCSBlobContainer extends AbstractBlobContainer {

    private final Logger logger = LogManager.getLogger(GCSBlobContainer.class);

    public GCSBlobContainer(BlobPath path) {
        super(path);
    }

    @Override
    public boolean blobExists(String blobName) throws IOException {
        throw new IOException("Unimplemented");
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        throw new IOException("Unimplemented");
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream,
                          long blobSize, boolean failIfAlreadyExists) throws IOException {
        throw new IOException("Unimplemented");
    }

    @Override
    public void delete() throws IOException {
        throw new IOException("Unimplemented");
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        throw new IOException("Unimplemented");
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        throw new IOException("Unimplemented");
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        throw new IOException("Unimplemented");
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        throw new IOException("Unimplemented");
    }
}
