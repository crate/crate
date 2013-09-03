package crate.elasticsearch.action.sql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

public class TransportSQLAction extends TransportAction<SQLRequest, SQLResponse> {

    private final TransportSearchAction transportSearchAction;

    private final NodeExecutionContext executionContext;

    @Inject
    protected TransportSQLAction(Settings settings, ThreadPool threadPool,
            NodeExecutionContext executionContext,
            TransportService transportService, TransportSearchAction transportSearchAction) {
        super(settings, threadPool);
        this.executionContext = executionContext;
        transportService.registerHandler(SQLAction.NAME, new TransportHandler());
        this.transportSearchAction = transportSearchAction;
    }

    private class SearchResponseListener implements ActionListener<SearchResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public SearchResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            delegate = listener;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            delegate.onResponse(stmt.buildResponse(searchResponse));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(e);
        }
    }

    @Override
    protected void doExecute(SQLRequest request, ActionListener<SQLResponse> listener) {
        System.out.println("doExecute: " + request);
        ParsedStatement stmt;
        SearchRequest searchRequest;
        try {
            stmt = new ParsedStatement(request.stmt(), executionContext);
            searchRequest = stmt.buildSearchRequest();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        transportSearchAction.execute(searchRequest, new SearchResponseListener(stmt, listener));

    }

    private class TransportHandler extends BaseTransportRequestHandler<SQLRequest> {


        @Override
        public SQLRequest newInstance() {
            return new SQLRequest();
        }

        @Override
        public void messageReceived(SQLRequest request, final TransportChannel channel) throws Exception {
            // no need for a threaded listener
            request.listenerThreaded(false);
            execute(request, new ActionListener<SQLResponse>() {
                @Override
                public void onResponse(SQLResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for sql query", e1);
                    }
                }
            });
        }


        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}