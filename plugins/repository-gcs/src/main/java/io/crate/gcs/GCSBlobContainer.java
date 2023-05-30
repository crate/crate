package io.crate.gcs;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.cloud.storage.*;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.api.gax.paging.Page;


/**
 * AbstractBlobContainer inherits from BlobContainer which is: "An interface for
 * managing a repository of blob entries, where each blob entry is just a named
 * group of bytes."
 */
public class GCSBlobContainer extends AbstractBlobContainer {

    private final Bucket bucket;

    public GCSBlobContainer(BlobPath path, Bucket bucket) {
        super(path);
        this.bucket = bucket;
    }

    @Override
    public boolean blobExists(String blobName) throws StorageException {
        return bucket.get(blobName) != null;
    }

    @Override
    public InputStream readBlob(String blobName) throws StorageException, IOException {
        final Blob blob = bucket.get(blobName);
        if (blob != null) {
            final ReadChannel channel = blob.reader();
            return Channels.newInputStream(channel);
        } else {
            throw new IOException("blob `" + blobName + "` does not exist");
        }
    }

    @Override
    public InputStream readBlob(final String blobName, final long position, final long length) throws UnsupportedOperationException {
        // This method seems redundant; why not directly use InputStream.skip?
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream,
                          long blobSize, boolean failIfAlreadyExists) throws IOException {
        // As an additional integrity check we could read the whole input
        // stream here and compute an CRC32C, which we can add to the blob info.
        // However, this might be inefficient with very large files. The
        // transferTo method does _not_ buffer the whole input internally, and
        // therefore should provide streamed writing.
        final Blob blob = createBlob(blobName, failIfAlreadyExists);
        final WriteChannel channel = blob.writer();
        final OutputStream outputStream = Channels.newOutputStream(channel);
        inputStream.transferTo(outputStream);
        channel.close();
    }

    @Override
    public void delete() {
        // Delete this container and all its contents from the bucket.
        // This means we delete all blobs with the base path as prefix.
        final String basePath = path().buildAsString();
        final List<Blob> blobs = _listBlobsByPrefix(basePath);
        for (final Blob blob : blobs) {
            blob.delete();
        }
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
        for (final String blobName : blobNames) {
            final Blob blob = bucket.get(blobName);
            if (blob != null) {
                blob.delete();
            }
        }
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        // https://community.crate.io/t/question-about-blobcontainer-children/1499
        throw new IOException("Unimplemented");
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() {
        return listBlobsByPrefix("");
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
        // If the path is not empty, buildAsString adds a trailing separator.
        final String basePath = path().buildAsString() + blobNamePrefix;
        final List<Blob> blobs = _listBlobsByPrefix(basePath);
        return convertBlobsToMap(normalizeBlobs(basePath, blobs));
    }

    /**
     * Create a new empty blob with the specified name.
     */
    private Blob createBlob(String blobName, boolean failIfAlreadyExists) throws IOException {
        final Blob blob = bucket.get(blobName);
        if (blob != null) {
            if (failIfAlreadyExists) {
                throw new IOException("blob `" + blobName + "` already exists");
            } else {
                return blob;
            }
        }
        return bucket.create(blobName, new byte[] {});
    }

    /**
     * List all blobs starting with a given prefix.
     */
    private List<Blob> _listBlobsByPrefix(String blobNamePrefix) {
        final Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(blobNamePrefix));
        return StreamSupport.stream(blobs.iterateAll().spliterator(), false)
            .collect(Collectors.toList());
    }

    /**
     * Remove a prefix from a string.
     */
    private static String removePrefix(String prefix, String s) {
        assert s.startsWith(prefix);
        return s.substring(prefix.length());
    }

    /**
     * Convert a list of Blob instances to BlobMetadata instances.
     */
    private static List<BlobMetadata> normalizeBlobs(String prefix, List<Blob> blobs) {
        // Isn't the Collectors interface absolutely hideous?
        return blobs.stream()
            .map(blob -> new PlainBlobMetadata(
                removePrefix(prefix, blob.getName()), blob.getSize()))
            .collect(Collectors.toList());
    }

    /**
     * Convert a list of BlobMetadata objects to a Map.
     */
    private static Map<String, BlobMetadata> convertBlobsToMap(List<BlobMetadata> blobs) {
        /*
        I just want to note that this is a badly designed API: BlobMetadata
        contains a blob name and a blob size (in number of bytes), therefore
        returning a map with blob names as keys is redundant. Three reasons to
        design this api by returning a List<BlobMetadata> instead:
        1. The API is easier to implement.
        2. The caller doesn't always need a Map.
        3. The caller can pick the Map implementation.
         */
        return blobs.stream().collect(
            Collectors.toMap(BlobMetadata::name, m -> m));
    }
}
