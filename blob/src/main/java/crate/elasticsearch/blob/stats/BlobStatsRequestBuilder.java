package crate.elasticsearch.blob.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

public class BlobStatsRequestBuilder
    extends ActionRequestBuilder<BlobStatsRequest, BlobStatsResponse, BlobStatsRequestBuilder> {

    protected BlobStatsRequestBuilder(Client client, BlobStatsRequest request) {
        super((InternalClient)client, request);
    }

    @Override
    protected void doExecute(ActionListener<BlobStatsResponse> listener) {
        ((Client)client).execute(BlobStatsAction.INSTANCE, request, listener);
    }
}
