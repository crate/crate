package crate.elasticsearch.blob.v2;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexException;
import org.elasticsearch.rest.RestStatus;

public class BlobsDisabledException extends IndexException {

    public BlobsDisabledException(String index) {
        super(new Index(index), "blobs not enabled on this index");
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }
}
