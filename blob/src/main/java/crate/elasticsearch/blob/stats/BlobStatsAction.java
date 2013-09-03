package crate.elasticsearch.blob.stats;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class BlobStatsAction
    extends Action<BlobStatsRequest, BlobStatsResponse, BlobStatsRequestBuilder> {

    public static final BlobStatsAction INSTANCE = new BlobStatsAction();
    public static final String NAME = "crate_blob_stats";

    public BlobStatsAction() {
        super(NAME);
    }

    @Override
    public BlobStatsRequestBuilder newRequestBuilder(Client client) {
        return new BlobStatsRequestBuilder(client, new BlobStatsRequest());
    }

    @Override
    public BlobStatsResponse newResponse() {
        return new BlobStatsResponse();
    }
}
