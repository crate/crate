package org.cratedb.action.sql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
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
    private final TransportIndexAction transportIndexAction;
    private final TransportBulkAction transportBulkAction;

    private final NodeExecutionContext executionContext;

    @Inject
    protected TransportSQLAction(Settings settings, ThreadPool threadPool,
            NodeExecutionContext executionContext,
            TransportService transportService, TransportSearchAction transportSearchAction,
            TransportIndexAction transportIndexAction, TransportBulkAction transportBulkAction) {
        super(settings, threadPool);
        this.executionContext = executionContext;
        transportService.registerHandler(SQLAction.NAME, new TransportHandler());
        this.transportSearchAction = transportSearchAction;
        this.transportIndexAction = transportIndexAction;
        this.transportBulkAction = transportBulkAction;
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

    private class IndexResponseListener implements ActionListener<IndexResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public IndexResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            delegate = listener;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(IndexResponse indexResponse) {
            delegate.onResponse(stmt.buildResponse(indexResponse));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(e);
        }
    }

    private class BulkResponseListener implements ActionListener<BulkResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public BulkResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            delegate = listener;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(BulkResponse bulkResponse) {
            delegate.onResponse(stmt.buildResponse(bulkResponse));
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
        IndexRequest indexRequest;
        try {
            stmt = new ParsedStatement(request.stmt(), request.args(), executionContext);
            switch (stmt.type()) {
                case ParsedStatement.INSERT_ACTION:
                    indexRequest = stmt.buildIndexRequest();
                    transportIndexAction.execute(indexRequest, new IndexResponseListener(stmt, listener));
                    break;
                case ParsedStatement.BULK_ACTION:
                    BulkRequest bulkRequest = stmt.buildBulkRequest();
                    transportBulkAction.execute(bulkRequest, new BulkResponseListener(stmt, listener));
                    break;
                default:
                    searchRequest = stmt.buildSearchRequest();
                    transportSearchAction.execute(searchRequest, new SearchResponseListener(stmt, listener));
                    break;
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

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