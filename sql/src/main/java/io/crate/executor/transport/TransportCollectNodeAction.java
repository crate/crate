package io.crate.executor.transport;

import com.google.common.base.Preconditions;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.List;

public class TransportCollectNodeAction {

    private final StreamerVisitor streamerVisitor;
    private final String transportAction = "crate/sql/node/collect";
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final String executor = ThreadPool.Names.SEARCH;

    @Inject
    public TransportCollectNodeAction(ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;

        streamerVisitor = new StreamerVisitor();
        transportService.registerHandler(transportAction, new TransportHandler());
    }

    public void execute(
            String targetNode,
            NodeCollectRequest request,
            ActionListener<NodeCollectResponse> listener) {
        new AsyncAction(targetNode, request, listener).start();
    }

    protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    private DataType.Streamer[] extractStreamers(List<Symbol> outputs) {
        DataType.Streamer[] streamers = new DataType.Streamer[outputs.size()];

        int i = 0;
        for (Symbol symbol : outputs) {
            streamers[i] = symbol.accept(streamerVisitor, null);
            i++;
        }

        return streamers;
    }

    private NodeCollectResponse nodeOperation(NodeCollectRequest request) throws CrateException {
        CollectNode node = request.collectNode();

        // TODO: generate real result
        Object[][] result = new Object[][] { new Object[] { 0.4 }};

        NodeCollectResponse response = new NodeCollectResponse(extractStreamers(node.outputs()));
        response.rows(result);
        return response;
    }

    private class AsyncAction {

        private final NodeCollectRequest request;
        private final ActionListener<NodeCollectResponse> listener;
        private final DataType.Streamer[] streamers;
        private final DiscoveryNode node;
        private final String nodeId;
        private final ClusterState clusterState;

        private AsyncAction(String nodeId, NodeCollectRequest request, ActionListener<NodeCollectResponse> listener) {
            Preconditions.checkNotNull(nodeId);
            clusterState = clusterService.state();
            node = clusterState.nodes().get(nodeId);
            Preconditions.checkNotNull(node);

            this.nodeId = nodeId;
            this.request = request;
            this.listener = listener;
            this.streamers = extractStreamers(request.collectNode().outputs());
        }

        private void start() {
            if (nodeId.equals("_local") || nodeId.equals(clusterState.nodes().localNodeId())) {
                threadPool.executor(executor).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            listener.onResponse(nodeOperation(request));
                        } catch (Throwable e) {
                            listener.onFailure(e);
                        }
                    }
                });
            } else {
                transportService.sendRequest(
                        node,
                        transportAction,
                        request,
                        new BaseTransportResponseHandler<NodeCollectResponse>() {

                            @Override
                            public NodeCollectResponse newInstance() {
                                return new NodeCollectResponse(streamers);
                            }

                            @Override
                            public void handleResponse(NodeCollectResponse response) {
                                listener.onResponse(response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                listener.onFailure(exp);
                            }

                            @Override
                            public String executor() {
                                return executor;
                            }
                        }
                );
            }
        }

    }

    private class TransportHandler extends BaseTransportRequestHandler<NodeCollectRequest> {

        @Override
        public NodeCollectRequest newInstance() {
            return new NodeCollectRequest();
        }

        @Override
        public void messageReceived(NodeCollectRequest request, TransportChannel channel) throws Exception {
            try {
                channel.sendResponse(nodeOperation(request));
            } catch (CrateException e) {
                channel.sendResponse(e);
            }
        }

        @Override
        public String executor() {
            return executor;
        }
    }
}
