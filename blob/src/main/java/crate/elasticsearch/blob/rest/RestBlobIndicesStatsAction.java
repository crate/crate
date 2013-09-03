package crate.elasticsearch.blob.rest;

import crate.elasticsearch.blob.stats.BlobStatsAction;
import crate.elasticsearch.blob.stats.BlobStatsRequest;
import crate.elasticsearch.blob.stats.BlobStatsResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;
import static org.elasticsearch.rest.action.support.RestActions.splitIndices;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestBlobIndicesStatsAction extends BaseRestHandler {

    @Inject
    public RestBlobIndicesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_blobs/_status", this);
        controller.registerHandler(GET, "/{index}/_blobs/_status", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        BlobStatsRequest blobStatsRequest = new BlobStatsRequest();
        blobStatsRequest.indices(splitIndices(request.param("index")));

        client.execute(BlobStatsAction.INSTANCE, blobStatsRequest,
            new ActionListener<BlobStatsResponse>() {

            @Override
            public void onResponse(BlobStatsResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    buildBroadcastShardsHeader(builder, response);
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
