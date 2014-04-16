package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.executor.transport.task.AbstractChainedTask;
import io.crate.planner.node.ddl.ESCreateIndexNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;

import java.util.List;

public class ESCreateIndexTask extends AbstractChainedTask<Object[][]> {

    private final TransportCreateIndexAction transport;
    private final CreateIndexRequest request;
    private final CreateIndexListener listener;

    public ESCreateIndexTask(ESCreateIndexNode node, TransportCreateIndexAction transportCreateIndexAction) {
        super();
        this.transport = transportCreateIndexAction;
        this.request = buildRequest(node);
        this.listener = new CreateIndexListener(this.result);
    }

    @Override
    protected void doStart(List<Object[][]> upstreamResults) {
        transport.execute(request, listener);
    }

    private static class CreateIndexListener implements ActionListener<CreateIndexResponse> {

        private final SettableFuture<Object[][]> result;

        private CreateIndexListener(SettableFuture<Object[][]> result) {
            this.result = result;
        }

        @Override
        public void onResponse(CreateIndexResponse createIndexResponse) {
            if (createIndexResponse.isAcknowledged()) {
                result.set(new Object[][] { new Object[] { 1L }});
            } else {
                result.set(new Object[][] { new Object[] { 0L }});
            }
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    private CreateIndexRequest buildRequest(ESCreateIndexNode node) {
        CreateIndexRequest request = new CreateIndexRequest(node.tableName(), node.indexSettings());
        request.mapping(Constants.DEFAULT_MAPPING_TYPE, node.indexMapping());
        return request;
    }
}
