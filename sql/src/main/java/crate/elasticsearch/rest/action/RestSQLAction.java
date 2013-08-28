package crate.elasticsearch.rest.action;

import com.akiban.sql.StandardException;
import crate.elasticsearch.action.SQLRequestBuilder;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import crate.elasticsearch.rest.action.SQLResponseBuilder;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;
import java.io.IOException;

public class RestSQLAction extends BaseRestHandler {

    @Inject
    public RestSQLAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.POST, "/_sql", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {

        SearchRequest searchRequest;
        final SQLRequestBuilder requestBuilder = new SQLRequestBuilder();
        try {

            if (request.hasContent()) {
                requestBuilder.source(request.content());
            } else {
                throw new ElasticSearchException("missing request body");
            }

            searchRequest = requestBuilder.buildSearchRequest();
            searchRequest.listenerThreaded(false);
            SearchOperationThreading operationThreading = SearchOperationThreading.fromString(request.param("operation_threading"), null);
            if (operationThreading != null) {
                if (operationThreading == SearchOperationThreading.NO_THREADS) {
                    // since we don't spawn, don't allow no_threads, but change it to a single thread
                    operationThreading = SearchOperationThreading.SINGLE_THREAD;
                }
                searchRequest.operationThreading(operationThreading);
            }
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("failed to parse search request parameters", e);
            }
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        client.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {

                SQLResponseBuilder responseBuilder = new SQLResponseBuilder();

                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject();
                    responseBuilder.build(response, builder, requestBuilder.getFieldNameMapping());
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, response.status(), builder));
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
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}