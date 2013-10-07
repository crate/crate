package org.cratedb.blob;

import org.elasticsearch.common.bytes.BytesReference;

import java.util.UUID;

public interface IPutChunkRequest {

    public BytesReference content();
    public UUID transferId();
    public boolean isLast();
}
