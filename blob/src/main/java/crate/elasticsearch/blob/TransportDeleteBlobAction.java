package crate.elasticsearch.blob;

import crate.elasticsearch.blob.v2.BlobIndices;
import crate.elasticsearch.blob.v2.BlobShard;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteBlobAction extends TransportShardReplicationOperationAction<DeleteBlobRequest, DeleteBlobRequest,
        DeleteBlobResponse> {

    private final BlobIndices blobIndices;

    @Inject
    public TransportDeleteBlobAction(Settings settings,
            TransportService transportService,
            ClusterService clusterService,
            IndicesService indicesService,
            ThreadPool threadPool,
            ShardStateAction shardStateAction,
            BlobIndices blobIndices
                                    ) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.blobIndices = blobIndices;
        logger.info("Constructor");
    }

    @Override
    protected DeleteBlobRequest newRequestInstance() {
        return new DeleteBlobRequest();
    }

    @Override
    protected DeleteBlobRequest newReplicaRequestInstance() {
        return new DeleteBlobRequest();
    }

    @Override
    protected DeleteBlobResponse newResponseInstance() {
        return new DeleteBlobResponse();
    }

    @Override
    protected String transportAction() {
        return DeleteBlobAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected PrimaryResponse<DeleteBlobResponse, DeleteBlobRequest> shardOperationOnPrimary(ClusterState clusterState,
            PrimaryOperationRequest shardRequest) {
        logger.info("shardOperationOnPrimary {}", shardRequest);
        final DeleteBlobRequest request = shardRequest.request;
        BlobShard blobShard = blobIndices.blobShardSafe(shardRequest.request.index(), shardRequest.shardId);
        boolean deleted = blobShard.delete(request.id());
        final DeleteBlobResponse response = new DeleteBlobResponse(deleted);
        return new PrimaryResponse<DeleteBlobResponse, DeleteBlobRequest>(
                request, response, null);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        logger.warn("shardOperationOnReplica operating on replica but relocation is not implemented {}", shardRequest);
        BlobShard blobShard = blobIndices.blobShardSafe(shardRequest.request.index(), shardRequest.shardId);
        blobShard.delete(shardRequest.request.id());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, DeleteBlobRequest request) throws ElasticSearchException {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(),
                        request.index(),
                        null,
                        null,
                        request.id());
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DeleteBlobRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DeleteBlobRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }
}

