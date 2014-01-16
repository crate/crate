package io.crate.executor.transport.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.executor.transport.NodesCollectRequest;
import io.crate.executor.transport.NodesCollectResponse;
import io.crate.executor.transport.TransportCollectNodeAction;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Routing;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodeOperationResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RemoteCollectTask implements Task<Object[][]> {

    private static Routing routing;
    private final static RoutingVisitor routingVisitor = new RoutingVisitor();

    private final CollectNode collectNode;
    private final List<ListenableFuture<Object[][]>> result;
    private final String[] nodeIds;
    private final TransportCollectNodeAction transportCollectNodeAction;

    public RemoteCollectTask(CollectNode collectNode, TransportCollectNodeAction transportCollectNodeAction) {
        this.collectNode = collectNode;
        this.transportCollectNodeAction = transportCollectNodeAction;

        routingVisitor.processSymbols(collectNode, null);
        Preconditions.checkArgument(
                routing.hasLocations(),
                "RemoteCollectTask currently only works for plans with locations"
        );

        int resultSize = routing.locations().size();
        nodeIds = routing.locations().keySet().toArray(new String[resultSize]);
        result = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            result.add(SettableFuture.<Object[][]>create());
        }
    }

    @Override
    public void start() {
        transportCollectNodeAction.execute(
                new NodesCollectRequest(collectNode, nodeIds),
                new ActionListener<NodesCollectResponse>() {
                    @Override
                    public void onResponse(NodesCollectResponse nodesCollectResponse) {
                        for (int i = 0; i < nodesCollectResponse.getNodes().length; i++) {
                            SettableFuture<Object[][]> settableFuture = (SettableFuture<Object[][]>) result.get(i);
                            settableFuture.set(nodesCollectResponse.getAt(i).value());
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        // TODO:
                    }
                }
        );
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return result;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException("nope");
    }

    static class RoutingVisitor extends SymbolVisitor<Void, Void> {

        @Override
        public Void visitRouting(Routing symbol, Void context) {
            Preconditions.checkArgument(routing == null, "Multiple routings are not supported");
            routing = symbol;
            for (Map.Entry<String, Map<String, Integer>> entry : routing.locations().entrySet()) {
                Preconditions.checkArgument(entry.getValue() == null, "Shards are not supported");
            }
            return null;
        }
    }
}
