package crate.elasticsearch.blob;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

public class PutChunkRequestBuilder extends ShardReplicationOperationRequestBuilder<PutChunkRequest, PutChunkResponse,
        PutChunkRequestBuilder> {

    protected PutChunkRequestBuilder(Client client) {
        super((InternalClient) client, new PutChunkRequest());
    }

    @Override
    protected void doExecute(ActionListener<PutChunkResponse> listener) {
        ((Client) client).execute(PutChunkAction.INSTANCE, request, listener);
    }
}
