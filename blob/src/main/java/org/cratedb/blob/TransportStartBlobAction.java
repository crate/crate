package org.cratedb.blob;

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

public class TransportStartBlobAction extends TransportShardReplicationOperationAction<StartBlobRequest, StartBlobRequest,
        StartBlobResponse> {

    private final BlobTransferTarget transferTarget;

    @Inject
    public TransportStartBlobAction(Settings settings,
            TransportService transportService,
            ClusterService clusterService,
            IndicesService indicesService,
            ThreadPool threadPool,
            ShardStateAction shardStateAction,
            BlobTransferTarget transferTarget) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.transferTarget = transferTarget;
        logger.info("Constructor");
    }

    @Override
    protected StartBlobRequest newRequestInstance() {
        logger.info("newRequestInstance");
        return new StartBlobRequest();
    }

    @Override
    protected StartBlobRequest newReplicaRequestInstance() {
        logger.info("newReplicaRequestInstance");
        return new StartBlobRequest();
    }

    @Override
    protected StartBlobResponse newResponseInstance() {
        logger.info("newResponseInstance");
        return new StartBlobResponse();
    }

    @Override
    protected String transportAction() {
        return StartBlobAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected PrimaryResponse<StartBlobResponse, StartBlobRequest> shardOperationOnPrimary(ClusterState clusterState,
            PrimaryOperationRequest shardRequest) {
        logger.info("shardOperationOnPrimary {}", shardRequest);
        final StartBlobRequest request = shardRequest.request;
        final StartBlobResponse response = newResponseInstance();
        transferTarget.startTransfer(shardRequest.shardId, request, response);
        return new PrimaryResponse<StartBlobResponse, StartBlobRequest>(
                request, response, null);

    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        logger.trace("shardOperationOnReplica operating on replica {}", shardRequest);
        final StartBlobRequest request = shardRequest.request;
        final StartBlobResponse response = newResponseInstance();
        transferTarget.startTransfer(shardRequest.shardId, request, response);
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, StartBlobRequest request) throws ElasticSearchException {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(),
                        request.index(),
                        null,
                        null, request.id());
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, StartBlobRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, StartBlobRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }
}

