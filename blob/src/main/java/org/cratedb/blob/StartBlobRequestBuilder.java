package org.cratedb.blob;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

public class StartBlobRequestBuilder extends ShardReplicationOperationRequestBuilder<StartBlobRequest, StartBlobResponse,
        StartBlobRequestBuilder> {

    protected StartBlobRequestBuilder(Client client) {
        super((InternalClient) client, new StartBlobRequest());
    }

    @Override
    protected void doExecute(ActionListener<StartBlobResponse> listener) {
        ((Client) client).execute(StartBlobAction.INSTANCE, request, listener);
    }
}
