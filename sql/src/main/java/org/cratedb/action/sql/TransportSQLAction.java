package org.cratedb.action.sql;

import org.cratedb.sql.DuplicateKeyException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.VersionConflictException;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

public class TransportSQLAction extends TransportAction<SQLRequest, SQLResponse> {

    private final TransportSearchAction transportSearchAction;
    private final TransportIndexAction transportIndexAction;
    private final TransportDeleteByQueryAction transportDeleteByQueryAction;
    private final TransportBulkAction transportBulkAction;
    private final TransportCountAction transportCountAction;
    private final TransportGetAction transportGetAction;
    private final TransportDeleteAction transportDeleteAction;
    private final TransportUpdateAction transportUpdateAction;

    private final NodeExecutionContext executionContext;

    @Inject
    protected TransportSQLAction(Settings settings, ThreadPool threadPool,
            NodeExecutionContext executionContext,
            TransportService transportService,
            TransportSearchAction transportSearchAction,
            TransportDeleteByQueryAction transportDeleteByQueryAction,
            TransportIndexAction transportIndexAction,
            TransportBulkAction transportBulkAction,
            TransportCountAction transportCountAction,
            TransportGetAction transportGetAction,
            TransportDeleteAction transportDeleteAction,
            TransportUpdateAction transportUpdateAction) {
        super(settings, threadPool);
        this.executionContext = executionContext;
        transportService.registerHandler(SQLAction.NAME, new TransportHandler());
        this.transportSearchAction = transportSearchAction;
        this.transportIndexAction = transportIndexAction;
        this.transportDeleteByQueryAction = transportDeleteByQueryAction;
        this.transportBulkAction = transportBulkAction;
        this.transportCountAction = transportCountAction;
        this.transportGetAction = transportGetAction;
        this.transportDeleteAction = transportDeleteAction;
        this.transportUpdateAction = transportUpdateAction;
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
            delegate.onFailure(reRaiseCrateException(e));
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
            delegate.onFailure(reRaiseCrateException(e));
        }
    }

    private class DeleteByQueryResponseListener implements ActionListener<DeleteByQueryResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public DeleteByQueryResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            this.delegate = listener;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(DeleteByQueryResponse deleteByQueryResponses) {
            delegate.onResponse(stmt.buildResponse(deleteByQueryResponses));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(reRaiseCrateException(e));
        }
    }

    private class DeleteResponseListener implements ActionListener<DeleteResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public DeleteResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            this.delegate = listener;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(DeleteResponse deleteResponse) {
            delegate.onResponse(stmt.buildResponse(deleteResponse));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(reRaiseCrateException(e));
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
            delegate.onFailure(reRaiseCrateException(e));
        }
    }

    @Override
    protected void doExecute(SQLRequest request, ActionListener<SQLResponse> listener) {
        logger.trace("doExecute: " + request);
        ParsedStatement stmt;
        SearchRequest searchRequest;
        IndexRequest indexRequest;
        DeleteByQueryRequest deleteByQueryRequest;
        try {
            stmt = new ParsedStatement(request.stmt(), request.args(), executionContext);
            switch (stmt.type()) {
                case ParsedStatement.INSERT_ACTION:
                    indexRequest = stmt.buildIndexRequest();
                    transportIndexAction.execute(indexRequest, new IndexResponseListener(stmt, listener));
                    break;
                case ParsedStatement.DELETE_BY_QUERY_ACTION:
                    deleteByQueryRequest = stmt.buildDeleteByQueryRequest();
                    transportDeleteByQueryAction.execute(deleteByQueryRequest, new DeleteByQueryResponseListener(stmt, listener));
                    break;
                case ParsedStatement.DELETE_ACTION:
                    DeleteRequest deleteRequest = stmt.buildDeleteRequest();
                    transportDeleteAction.execute(deleteRequest, new DeleteResponseListener(stmt, listener));
                    break;
                case ParsedStatement.BULK_ACTION:
                    BulkRequest bulkRequest = stmt.buildBulkRequest();
                    transportBulkAction.execute(bulkRequest, new BulkResponseListener(stmt, listener));
                    break;
                case ParsedStatement.GET_ACTION:
                    GetRequest getRequest = stmt.buildGetRequest();
                    transportGetAction.execute(getRequest, new GetResponseListener(stmt, listener));
                    break;
                case ParsedStatement.UPDATE_ACTION:
                    UpdateRequest updateRequest = stmt.buildUpdateRequest();
                    transportUpdateAction.execute(updateRequest, new UpdateResponseListener(stmt, listener));
                    break;
                default:
                    if (stmt.countRequest()) {
                        CountRequest countRequest = stmt.buildCountRequest();
                        transportCountAction.execute(countRequest, new CountResponseListener(stmt, listener));
                        break;
                    }
                    searchRequest = stmt.buildSearchRequest();
                    transportSearchAction.execute(searchRequest, new SearchResponseListener(stmt, listener));
                    break;
            }
        } catch (StandardException e) {
            listener.onFailure(standardExceptionToParseException(e));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Throwable reRaiseCrateException(Throwable e) {
        if (e instanceof DocumentAlreadyExistsException) {
            return new DuplicateKeyException(
                "A document with the same primary key exists already", e);
        } else if (e instanceof ReduceSearchPhaseException && e.getCause() instanceof VersionConflictException) {
            /**
             * For update or search requests we use upstream ES SearchRequests
             * These requests are executed using the transportSearchAction.
             *
             * The transportSearchAction (or the more specific QueryThenFetch/../ Action inside it
             * executes the TransportSQLAction.SearchResponseListener onResponse/onFailure
             * but adds its own error handling around it.
             * By doing so it wraps every exception raised inside our onResponse in its own ReduceSearchPhaseException
             * Here we unwrap it to get the original exception.
             */
            return e.getCause();
        }
        return e;
    }

    private SQLParseException standardExceptionToParseException(StandardException e) {
        return new SQLParseException(e.getMessage(), e);
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

    private class CountResponseListener implements ActionListener<CountResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;


        public CountResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            this.stmt = stmt;
            this.delegate = listener;
        }

        @Override
        public void onResponse(CountResponse countResponse) {
            delegate.onResponse(stmt.buildResponse(countResponse));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(reRaiseCrateException(e));
        }
    }

    private class GetResponseListener implements ActionListener<GetResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public GetResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            delegate = listener;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(GetResponse getResponse) {
            delegate.onResponse(stmt.buildResponse(getResponse));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(reRaiseCrateException(e));
        }
    }

    private class UpdateResponseListener implements ActionListener<UpdateResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public UpdateResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            delegate = listener;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(UpdateResponse updateResponse) {
            delegate.onResponse(stmt.buildResponse(updateResponse));
        }

        @Override
        public void onFailure(Throwable e) {
            if (e instanceof DocumentMissingException) {
                delegate.onResponse(stmt.buildMissingDocumentResponse());
            } else {
                delegate.onFailure(reRaiseCrateException(e));
            }
        }
    }

}
