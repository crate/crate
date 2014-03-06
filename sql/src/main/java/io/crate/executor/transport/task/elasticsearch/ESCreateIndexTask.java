package io.crate.executor.transport.task.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.planner.node.ddl.ESCreateIndexNode;
import io.crate.Constants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;

import java.util.List;

public class ESCreateIndexTask implements Task<Object[][]> {

    private final TransportCreateIndexAction transport;
    private final CreateIndexRequest request;
    private final List<ListenableFuture<Object[][]>> results;
    private final CreateIndexListener listener;

    public ESCreateIndexTask(ESCreateIndexNode node, TransportCreateIndexAction transportCreateIndexAction) {
        this.transport = transportCreateIndexAction;
        this.request = buildRequest(node);
        SettableFuture<Object[][]> result = SettableFuture.create();
        this.results = ImmutableList.<ListenableFuture<Object[][]>>of(result);
        this.listener = new CreateIndexListener(result);
    }

    @Override
    public void start() {
        transport.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException("CreateIndexTask doesn't support upstream results");
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
