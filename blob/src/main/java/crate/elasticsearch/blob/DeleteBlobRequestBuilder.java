package crate.elasticsearch.blob;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

public class DeleteBlobRequestBuilder extends ShardReplicationOperationRequestBuilder<DeleteBlobRequest, DeleteBlobResponse,
        DeleteBlobRequestBuilder> {

    protected DeleteBlobRequestBuilder(Client client) {
        super((InternalClient) client, new DeleteBlobRequest());
    }

    @Override
    protected void doExecute(ActionListener<DeleteBlobResponse> listener) {
        ((Client) client).execute(DeleteBlobAction.INSTANCE, request, listener);
    }
}
