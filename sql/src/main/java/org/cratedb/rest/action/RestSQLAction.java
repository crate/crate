package org.cratedb.rest.action;

import org.cratedb.action.sql.SQLRequestBuilder;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

public class RestSQLAction extends BaseRestHandler {

    @Inject
    public RestSQLAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.POST, "/_sql", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {

        final SQLRequestBuilder requestBuilder = new SQLRequestBuilder(client);
        try {
            if (request.hasContent()) {
                requestBuilder.source(request.content());
            } else {
                throw new ElasticSearchException("missing request body");
            }
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("failed to parse sql request", e);
            }
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error",
                        e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        requestBuilder.execute(new ActionListener<SQLResponse>() {
            @Override
            public void onResponse(SQLResponse response) {

                try {
                    XContentBuilder builder = restContentBuilder(request);
                    response.toXContent(builder, request);
                    // TODO: implement shard counts on response to set status properly
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to execute search (building response)", e);
                    }
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new CrateThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}