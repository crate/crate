package org.cratedb.blob;

import org.elasticsearch.common.UUID;
import org.elasticsearch.common.bytes.BytesReference;

public interface IPutChunkRequest {

    public BytesReference content();
    public UUID transferId();
    public boolean isLast();
}
