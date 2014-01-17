package io.crate.executor.transport.task;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.executor.transport.NodesCollectRequest;
import io.crate.executor.transport.NodesCollectResponse;
import io.crate.executor.transport.TransportCollectNodeAction;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.plan.Routing;
import io.crate.planner.symbol.SymbolVisitor;
import org.elasticsearch.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RemoteCollectTask implements Task<Object[][]> {

    private final CollectNode collectNode;
    private final List<ListenableFuture<Object[][]>> result;
    private final String[] nodeIds;
    private final TransportCollectNodeAction transportCollectNodeAction;

    public RemoteCollectTask(CollectNode collectNode, TransportCollectNodeAction transportCollectNodeAction) {
        this.collectNode = collectNode;
        this.transportCollectNodeAction = transportCollectNodeAction;

        Preconditions.checkArgument(collectNode.isRouted(),
                "RemoteCollectTask currently only works for plans with routing"
        );

        for (Map.Entry<String, Map<String, Integer>> entry : collectNode.routing().locations().entrySet()) {
            Preconditions.checkArgument(entry.getValue() == null, "Shards are not supported");
        }

        int resultSize = collectNode.routing().nodes().size();
        nodeIds = collectNode.routing().nodes().toArray(new String[resultSize]);
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
}
