package crate.elasticsearch.blob;

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

import java.util.concurrent.ExecutionException;

public class TransportPutChunkAction extends TransportShardReplicationOperationAction<PutChunkRequest, PutChunkReplicaRequest, PutChunkResponse> {

    private final BlobTransferTarget transferTarget;

    @Inject
    public TransportPutChunkAction(Settings settings, TransportService transportService, ClusterService clusterService, IndicesService
            indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
            BlobTransferTarget transferTarget) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.transferTarget = transferTarget;
    }

    @Override
    protected PutChunkRequest newRequestInstance() {
        return new PutChunkRequest();
    }

    @Override
    protected PutChunkReplicaRequest newReplicaRequestInstance() {
        return new PutChunkReplicaRequest();
    }

    @Override
    protected PutChunkResponse newResponseInstance() {
        return new PutChunkResponse();
    }

    @Override
    protected String transportAction() {
        return PutChunkAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected PrimaryResponse<PutChunkResponse, PutChunkReplicaRequest> shardOperationOnPrimary(ClusterState clusterState,
            PrimaryOperationRequest shardRequest) {
        final PutChunkRequest request = shardRequest.request;
        PutChunkResponse response = newResponseInstance();
        transferTarget.continueTransfer(request, response);

        final PutChunkReplicaRequest replicaRequest = newReplicaRequestInstance();
        replicaRequest.transferId = request.transferId();
        replicaRequest.sourceNodeId = clusterState.getNodes().localNode().getId();
        replicaRequest.currentPos = request.currentPos();
        replicaRequest.content = request.content();
        replicaRequest.isLast = request.isLast();
        return new PrimaryResponse<PutChunkResponse, PutChunkReplicaRequest>(replicaRequest, response, null);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        final PutChunkReplicaRequest request = shardRequest.request;
        PutChunkResponse response = newResponseInstance();
        transferTarget.continueTransfer(request, response, shardRequest.shardId);
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, PutChunkRequest request) throws ElasticSearchException {
        return clusterService.operationRouting()
            .indexShards(clusterService.state(),
                request.index(),
                null,
                null,
                request.digest());
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PutChunkRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PutChunkRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }
}

