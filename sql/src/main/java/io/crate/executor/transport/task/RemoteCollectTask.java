package io.crate.executor.transport.task;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.executor.transport.NodeCollectRequest;
import io.crate.executor.transport.NodeCollectResponse;
import io.crate.executor.transport.TransportCollectNodeAction;
import io.crate.planner.plan.CollectNode;
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
        NodeCollectRequest request = new NodeCollectRequest(collectNode);
        for (int i = 0; i < nodeIds.length; i++) {
            final int resultIdx = i;

            transportCollectNodeAction.execute(
                    nodeIds[i],
                    request,
                    new ActionListener<NodeCollectResponse>() {
                        @Override
                        public void onResponse(NodeCollectResponse response) {
                            ((SettableFuture<Object[][]>)result.get(resultIdx)).set(response.rows());
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            ((SettableFuture<Object[][]>)result.get(resultIdx)).setException(e);
                        }
                    }
            );
        }
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
