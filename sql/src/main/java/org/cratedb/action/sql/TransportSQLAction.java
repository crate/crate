package org.cratedb.action.sql;

import org.cratedb.action.DistributedSQLRequest;
import org.cratedb.action.TransportDistributedSQLAction;
import org.cratedb.sql.ExceptionHelper;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
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
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
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
    private final TransportMultiGetAction transportMultiGetAction;
    private final TransportDeleteAction transportDeleteAction;
    private final TransportUpdateAction transportUpdateAction;
    private final TransportDistributedSQLAction transportDistributedSQLAction;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final NodeExecutionContext executionContext;

    @Inject
    protected TransportSQLAction(Settings settings, ThreadPool threadPool,
            NodeExecutionContext executionContext,
            TransportService transportService,
            TransportSearchAction transportSearchAction,
            TransportDeleteByQueryAction transportDeleteByQueryAction,
            TransportIndexAction transportIndexAction,
            TransportBulkAction transportBulkAction,
            TransportGetAction transportGetAction,
            TransportMultiGetAction transportMultiGetAction,
            TransportDeleteAction transportDeleteAction,
            TransportUpdateAction transportUpdateAction,
            TransportDistributedSQLAction transportDistributedSQLAction,
            TransportCountAction transportCountAction,
            TransportCreateIndexAction transportCreateIndexAction,
            TransportDeleteIndexAction transportDeleteIndexAction) {
        super(settings, threadPool);
        this.executionContext = executionContext;
        transportService.registerHandler(SQLAction.NAME, new TransportHandler());
        this.transportSearchAction = transportSearchAction;
        this.transportIndexAction = transportIndexAction;
        this.transportDeleteByQueryAction = transportDeleteByQueryAction;
        this.transportBulkAction = transportBulkAction;
        this.transportCountAction = transportCountAction;
        this.transportGetAction = transportGetAction;
        this.transportMultiGetAction = transportMultiGetAction;
        this.transportDeleteAction = transportDeleteAction;
        this.transportUpdateAction = transportUpdateAction;
        this.transportDistributedSQLAction = transportDistributedSQLAction;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportDeleteIndexAction = transportDeleteIndexAction;
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
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
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
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
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
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
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
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
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
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
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
                case INSERT_ACTION:
                    indexRequest = stmt.buildIndexRequest();
                    transportIndexAction.execute(indexRequest, new IndexResponseListener(stmt, listener));
                    break;
                case DELETE_BY_QUERY_ACTION:
                    deleteByQueryRequest = stmt.buildDeleteByQueryRequest();
                    transportDeleteByQueryAction.execute(deleteByQueryRequest, new DeleteByQueryResponseListener(stmt, listener));
                    break;
                case DELETE_ACTION:
                    DeleteRequest deleteRequest = stmt.buildDeleteRequest();
                    transportDeleteAction.execute(deleteRequest, new DeleteResponseListener(stmt, listener));
                    break;
                case BULK_ACTION:
                    BulkRequest bulkRequest = stmt.buildBulkRequest();
                    transportBulkAction.execute(bulkRequest, new BulkResponseListener(stmt, listener));
                    break;
                case GET_ACTION:
                    GetRequest getRequest = stmt.buildGetRequest();
                    transportGetAction.execute(getRequest, new GetResponseListener(stmt, listener));
                    break;
                case MULTI_GET_ACTION:
                    MultiGetRequest multiGetRequest = stmt.buildMultiGetRequest();
                    transportMultiGetAction.execute(multiGetRequest, new MultiGetResponseListener(stmt, listener));
                    break;
                case UPDATE_ACTION:
                    UpdateRequest updateRequest = stmt.buildUpdateRequest();
                    transportUpdateAction.execute(updateRequest, new UpdateResponseListener(stmt, listener));
                    break;
                case CREATE_INDEX_ACTION:
                    CreateIndexRequest createIndexRequest = stmt.buildCreateIndexRequest();
                    transportCreateIndexAction.execute(createIndexRequest, new CreateIndexResponseListener(stmt, listener));
                    break;
                case DELETE_INDEX_ACTION:
                    DeleteIndexRequest deleteIndexRequest = stmt.buildDeleteIndexRequest();
                    transportDeleteIndexAction.execute(deleteIndexRequest, new DeleteIndexResponseListener(stmt, listener));
                    break;
                default:
                    if (stmt.hasGroupBy()) {
                        transportDistributedSQLAction.execute(
                            new DistributedSQLRequest(request, stmt),
                            new DistributedSQLResponseListener(stmt, listener));
                        break;
                    }

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
            listener.onFailure(new SQLParseException(e.getMessage(), e));
        } catch (Exception e) {
            listener.onFailure(ExceptionHelper.transformToCrateException(e));
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
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
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
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private class MultiGetResponseListener implements ActionListener<MultiGetResponse> {
        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public MultiGetResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            this.delegate = listener;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(MultiGetResponse multiGetItemResponses) {
            delegate.onResponse(stmt.buildResponse(multiGetItemResponses));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private class DistributedSQLResponseListener implements ActionListener<SQLResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        public DistributedSQLResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
            this.stmt = stmt;
            this.delegate = listener;
        }

        @Override
        public void onResponse(SQLResponse sqlResponse) {
            delegate.onResponse(sqlResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
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
                delegate.onFailure(ExceptionHelper.transformToCrateException(e));
            }
        }
    }

    private class CreateIndexResponseListener implements ActionListener<CreateIndexResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        private CreateIndexResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> delegate) {
            this.delegate = delegate;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(CreateIndexResponse createIndexResponse) {
            delegate.onResponse(stmt.buildResponse(createIndexResponse));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private class DeleteIndexResponseListener implements ActionListener<DeleteIndexResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;

        private DeleteIndexResponseListener(ParsedStatement stmt, ActionListener<SQLResponse> delegate) {
            this.delegate = delegate;
            this.stmt = stmt;
        }

        @Override
        public void onResponse(DeleteIndexResponse deleteIndexResponse) {
            delegate.onResponse(stmt.buildResponse(deleteIndexResponse));
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

}
