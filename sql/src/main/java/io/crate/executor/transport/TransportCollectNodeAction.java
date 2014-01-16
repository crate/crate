package io.crate.executor.transport;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportCollectNodeAction
        extends TransportNodesOperationAction<NodesCollectRequest, NodesCollectResponse, NodeCollectRequest, NodeCollectResponse> {

    @Inject
    public TransportCollectNodeAction(
            Settings settings,
            ClusterName clusterName,
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService)
    {
        super(settings, clusterName, threadPool, clusterService, transportService);
    }

    @Override
    protected String transportAction() {
        return "crate/sql/node/collect";
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    @Override
    protected NodesCollectRequest newRequest() {
        return new NodesCollectRequest();
    }

    @Override
    protected NodesCollectResponse newResponse(NodesCollectRequest request, AtomicReferenceArray nodesResponses) {
        NodeCollectResponse[] responses = new NodeCollectResponse[nodesResponses.length()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = (NodeCollectResponse)nodesResponses.get(i);
        }
        return new NodesCollectResponse(clusterName, responses);
    }

    @Override
    protected NodeCollectRequest newNodeRequest() {
        return new NodeCollectRequest();
    }

    @Override
    protected NodeCollectRequest newNodeRequest(String nodeId, NodesCollectRequest request) {
        return new NodeCollectRequest(request, nodeId);
    }

    @Override
    protected NodeCollectResponse newNodeResponse() {
        return new NodeCollectResponse(clusterService.localNode());
    }

    @Override
    protected NodeCollectResponse nodeOperation(NodeCollectRequest request) throws ElasticSearchException {
        // TODO: do stuff
        return newNodeResponse();
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }
}
