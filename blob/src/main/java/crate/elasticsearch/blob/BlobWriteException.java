package crate.elasticsearch.blob;

import org.elasticsearch.ElasticSearchException;

public class BlobWriteException extends ElasticSearchException {

    public BlobWriteException(String digest, long size, Throwable cause) {
        super(String.format("digest: {} size:{}", digest, size), cause);
    }
}
