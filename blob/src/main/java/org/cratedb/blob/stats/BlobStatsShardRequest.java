package org.cratedb.blob.stats;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;

public class BlobStatsShardRequest extends BroadcastShardOperationRequest {

    BlobStatsShardRequest() {
    }

    BlobStatsShardRequest(String index, int shardId, BlobStatsRequest request) {
        super(index, shardId, request);
    }
}
