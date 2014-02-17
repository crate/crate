/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.action.sql;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.executor.AffectedRowsResponseBuilder;
import io.crate.executor.Job;
import io.crate.executor.ResponseBuilder;
import io.crate.executor.RowsResponseBuilder;
import io.crate.executor.transport.TransportExecutor;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.cratedb.Constants;
import org.cratedb.action.DistributedSQLRequest;
import org.cratedb.action.TransportDistributedSQLAction;
import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.import_.ImportResponse;
import org.cratedb.action.import_.TransportImportAction;
import org.cratedb.action.parser.ESRequestBuilder;
import org.cratedb.action.parser.SQLResponseBuilder;
import org.cratedb.service.InformationSchemaService;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.ExceptionHelper;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
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

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


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
    private final InformationSchemaService informationSchemaService;
    private final SQLParseService sqlParseService;
    private final TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction;
    private final TransportImportAction transportImportAction;

    private final Analyzer analyzer;
    private final TransportExecutor transportExecutor;
    private final static Planner planner = new Planner();

    @Inject
    protected TransportSQLAction(Settings settings, ThreadPool threadPool,
            SQLParseService sqlParseService,
            Analyzer analyzer,
            TransportExecutor transportExecutor,
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
            TransportDeleteIndexAction transportDeleteIndexAction,
            TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction,
            TransportImportAction transportImportAction,
            InformationSchemaService informationSchemaService) {
        super(settings, threadPool);
        this.sqlParseService = sqlParseService;
        this.analyzer = analyzer;
        this.transportExecutor = transportExecutor;
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
        this.transportClusterUpdateSettingsAction = transportClusterUpdateSettingsAction;
        this.transportImportAction = transportImportAction;
        this.informationSchemaService = informationSchemaService;
    }

    private abstract class ESResponseToSQLResponseListener<T extends ActionResponse> implements ActionListener<T> {

        protected final ActionListener<SQLResponse> listener;
        protected final SQLResponseBuilder builder;
        protected final long requestStartedTime;

        public ESResponseToSQLResponseListener(ParsedStatement stmt,
                                               ActionListener<SQLResponse> listener,
                                               long requestStartedTime) {
            this.listener = listener;
            this.builder = new SQLResponseBuilder(sqlParseService.context, stmt);
            this.requestStartedTime = requestStartedTime;
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private class SearchResponseListener extends ESResponseToSQLResponseListener<SearchResponse> {

        public SearchResponseListener(ParsedStatement stmt,
                                      ActionListener<SQLResponse> listener,
                                      long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(SearchResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class IndexResponseListener extends ESResponseToSQLResponseListener<IndexResponse> {

        public IndexResponseListener(ParsedStatement stmt,
                                     ActionListener<SQLResponse> listener,
                                     long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(IndexResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class DeleteByQueryResponseListener extends ESResponseToSQLResponseListener<DeleteByQueryResponse> {

        public DeleteByQueryResponseListener(ParsedStatement stmt,
                                             ActionListener<SQLResponse> listener,
                                             long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(DeleteByQueryResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class DeleteResponseListener extends ESResponseToSQLResponseListener<DeleteResponse> {

        public DeleteResponseListener(ParsedStatement stmt,
                                      ActionListener<SQLResponse> listener,
                                      long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(DeleteResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }

        @Override
        public void onFailure(Throwable e) {
            DeleteResponse response = ExceptionHelper.deleteResponseFromVersionConflictException(e);
            if (response != null) {
                listener.onResponse(builder.buildResponse(response, requestStartedTime));
            } else {
                listener.onFailure(ExceptionHelper.transformToCrateException(e));
            }
        }
    }

    private class BulkResponseListener extends ESResponseToSQLResponseListener<BulkResponse> {

        public BulkResponseListener(ParsedStatement stmt,
                                    ActionListener<SQLResponse> listener,
                                    long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(BulkResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    @Override
    protected void doExecute(SQLRequest request, ActionListener<SQLResponse> listener) {
        logger.trace("doExecute: " + request);

        try {
            StatementNode akibanNode = getAkibanNode(request.stmt());
            ParsedStatement stmt;
            if (akibanNode != null) {
                stmt = sqlParseService.parse(request.stmt(), akibanNode, request.args());
            } else {
                usePresto(request, listener);
                return;
            }

            ESRequestBuilder builder = new ESRequestBuilder(stmt);
            switch (stmt.type()) {
                case INFORMATION_SCHEMA:
                    informationSchemaService.execute(stmt, listener, request.creationTime());
                    break;
                case INSERT_ACTION:
                    IndexRequest indexRequest = builder.buildIndexRequest();
                    transportIndexAction.execute(indexRequest, new IndexResponseListener(stmt,
                            listener, request.creationTime()));
                    break;
                case DELETE_BY_QUERY_ACTION:
                    DeleteByQueryRequest deleteByQueryRequest = builder.buildDeleteByQueryRequest();
                    transportDeleteByQueryAction.execute(deleteByQueryRequest,
                            new DeleteByQueryResponseListener(stmt, listener, request.creationTime()));
                    break;
                case DELETE_ACTION:
                    DeleteRequest deleteRequest = builder.buildDeleteRequest();
                    transportDeleteAction.execute(deleteRequest,
                            new DeleteResponseListener(stmt, listener, request.creationTime()));
                    break;
                case BULK_ACTION:
                    BulkRequest bulkRequest = builder.buildBulkRequest();
                    transportBulkAction.execute(bulkRequest,
                            new BulkResponseListener(stmt, listener, request.creationTime()));
                    break;
                case GET_ACTION:
                    GetRequest getRequest = builder.buildGetRequest();
                    transportGetAction.execute(getRequest,
                            new GetResponseListener(stmt, listener, request.creationTime()));
                    break;
                case MULTI_GET_ACTION:
                    MultiGetRequest multiGetRequest = builder.buildMultiGetRequest();
                    transportMultiGetAction.execute(multiGetRequest,
                            new MultiGetResponseListener(stmt, listener, request.creationTime()));
                    break;
                case UPDATE_ACTION:
                    UpdateRequest updateRequest = builder.buildUpdateRequest();
                    transportUpdateAction.execute(updateRequest,
                            new UpdateResponseListener(stmt, listener, request.creationTime()));
                    break;
                case CREATE_INDEX_ACTION:
                    CreateIndexRequest createIndexRequest = builder.buildCreateIndexRequest();
                    transportCreateIndexAction.execute(createIndexRequest,
                            new CreateIndexResponseListener(stmt, listener, request.creationTime()));
                    break;
                case DELETE_INDEX_ACTION:
                    DeleteIndexRequest deleteIndexRequest = builder.buildDeleteIndexRequest();
                    transportDeleteIndexAction.execute(deleteIndexRequest,
                            new DeleteIndexResponseListener(stmt, listener, request.creationTime()));
                    break;
                case CREATE_ANALYZER_ACTION:
                    ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = builder.buildClusterUpdateSettingsRequest();
                    transportClusterUpdateSettingsAction.execute(clusterUpdateSettingsRequest,
                            new ClusterUpdateSettingsResponseListener(stmt, listener, request.creationTime()));
                    break;
                case COPY_IMPORT_ACTION:
                    ImportRequest importRequest = builder.buildImportRequest();
                    transportImportAction.execute(importRequest,
                            new ImportResponseListener(stmt, listener, request.creationTime()));
                    break;
                case STATS:
                    transportDistributedSQLAction.execute(
                            new DistributedSQLRequest(request, stmt),
                            new DistributedSQLResponseListener(stmt, listener, request.creationTime()));
                    break;
                default:
                    // TODO: don't simply run globalAggregate Queries like Group By Queries
                    // Disable Reducers!!!
                    if (stmt.hasGroupBy() || stmt.isGlobalAggregate()) {
                        transportDistributedSQLAction.execute(
                            new DistributedSQLRequest(request, stmt),
                            new DistributedSQLResponseListener(stmt, listener, request.creationTime()));
                    } else if (stmt.countRequest()) {
                        CountRequest countRequest = builder.buildCountRequest();
                        transportCountAction.execute(countRequest,
                                new CountResponseListener(stmt, listener, request.creationTime()));
                    } else {
                        SearchRequest searchRequest = builder.buildSearchRequest();
                        transportSearchAction.execute(searchRequest,
                                new SearchResponseListener(stmt, listener, request.creationTime()));
                    }
                    break;
            }
        } catch (Exception e) {
            listener.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private void usePresto(final SQLRequest request, final ActionListener<SQLResponse> listener) {
        try {
            final Statement statement = SqlParser.createStatement(request.stmt());
            final Analysis analysis = analyzer.analyze(statement, request.args());
            final Plan plan = planner.plan(analysis);
            final Job job = transportExecutor.newJob(plan);
            final ListenableFuture<List<Object[][]>> resultFuture = Futures.allAsList(transportExecutor.execute(job));
            final ResponseBuilder responseBuilder = getResponseBuilder(plan);
            Futures.addCallback(resultFuture, new FutureCallback<List<Object[][]>>() {
                @Override
                public void onSuccess(@Nullable List<Object[][]> result) {
                    Object[][] rows;
                    if (result == null) {
                        rows = Constants.EMPTY_RESULT;
                    } else {
                        Preconditions.checkArgument(result.size() == 1);
                        rows = result.get(0);
                    }

                    SQLResponse response = responseBuilder.buildResponse(
                            analysis.outputNames().toArray(new String[analysis.outputNames().size()]),
                            rows,
                            request.creationTime());
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    listener.onFailure(ExceptionHelper.transformToCrateException(t));
                }
            });
        } catch (Exception e) {
            listener.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private ResponseBuilder getResponseBuilder(Plan plan) {
        if (plan.expectsAffectedRows()) {
            return new AffectedRowsResponseBuilder();
        } else {
            return new RowsResponseBuilder(true);
        }
    }

    /**
     * for the migration from akiban to the presto based sql-parser
     * if presto should be used it returns null, otherwise it returns the parsed StatementNode for akiban.
     *
     * @param stmt sql statement as string
     * @return null or the akiban StatementNode
     * @throws StandardException
     */
    private StatementNode getAkibanNode(String stmt) throws StandardException {

        SQLParser parser = new SQLParser();
        StatementNode node;
        try {
            node = parser.parseStatement(stmt);
        } catch (CrateException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLParseException(ex.getMessage(), ex);
        }
        final AtomicReference<Boolean> isPresto = new AtomicReference<>(false);

        // use presto for DeleteByQuery request (DISABLED)
        // TODO: enable if all needed system columns (_id + _version) and all needed operators (IN) are implemented
        /*
        if (node.getNodeType() == NodeType.DELETE_NODE) {
            DeleteNode deleteNode = (DeleteNode)node;
            if (((SelectNode)deleteNode.getResultSetNode()).getWhereClause() != null) {
                return null;
            }
        }
        */

        Visitor visitor = new Visitor() {
            @Override
            public Visitable visit(Visitable node) throws StandardException {
                if (((QueryTreeNode)node).getNodeType() == NodeType.FROM_BASE_TABLE) {
                    TableName tableName = ((FromBaseTable) node).getTableName();
                    if (tableName.getSchemaName() != null
                            && tableName.getSchemaName().equalsIgnoreCase("sys")) {

                        isPresto.set(true);
                        return null;
                    }
                }
                // use presto for inserts
                // TODO: keep disabled until type validation and stuff works correctly
                /*if (((QueryTreeNode) node).getNodeType() == NodeType.INSERT_NODE) {
                    isPresto.set(true);
                    return null;
                }*/
                return node;
            }

            @Override
            public boolean visitChildrenFirst(Visitable node) {
                return false;
            }

            @Override
            public boolean stopTraversal() {
                return isPresto.get();
            }

            @Override
            public boolean skipChildren(Visitable node) throws StandardException {
                return false;
            }
        };

        node.accept(visitor);

        if (isPresto.get()) {
            return null;
        }
        return node;
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

    private class CountResponseListener extends ESResponseToSQLResponseListener<CountResponse> {

        public CountResponseListener(ParsedStatement stmt,
                                     ActionListener<SQLResponse> listener,
                                     long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(CountResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }


    private class GetResponseListener extends ESResponseToSQLResponseListener<GetResponse> {

        public GetResponseListener(ParsedStatement stmt,
                                   ActionListener<SQLResponse> listener,
                                   long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(GetResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class MultiGetResponseListener extends ESResponseToSQLResponseListener<MultiGetResponse> {

        public MultiGetResponseListener(ParsedStatement stmt,
                                        ActionListener<SQLResponse> listener,
                                        long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(MultiGetResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class DistributedSQLResponseListener implements ActionListener<SQLResponse> {

        private final ActionListener<SQLResponse> delegate;
        private final ParsedStatement stmt;
        private final long requestStartedTime;

        public DistributedSQLResponseListener(ParsedStatement stmt,
                                              ActionListener<SQLResponse> listener,
                                              long requestStartedTime) {
            this.stmt = stmt;
            this.delegate = listener;
            this.requestStartedTime = requestStartedTime;
        }

        @Override
        public void onResponse(SQLResponse sqlResponse) {
            sqlResponse.requestStartedTime(requestStartedTime);
            delegate.onResponse(sqlResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            delegate.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private class UpdateResponseListener extends ESResponseToSQLResponseListener<UpdateResponse> {

        public UpdateResponseListener(ParsedStatement stmt,
                                      ActionListener<SQLResponse> listener,
                                      long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(UpdateResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }

        @Override
        public void onFailure(Throwable e) {
            if (e instanceof DocumentMissingException) {
                listener.onResponse(builder.buildMissingDocumentResponse(requestStartedTime));
            } else {
                listener.onFailure(ExceptionHelper.transformToCrateException(e));
            }
        }
    }

    private class CreateIndexResponseListener extends ESResponseToSQLResponseListener<CreateIndexResponse> {

        public CreateIndexResponseListener(ParsedStatement stmt,
                                           ActionListener<SQLResponse> listener,
                                           long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(CreateIndexResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class DeleteIndexResponseListener extends ESResponseToSQLResponseListener<DeleteIndexResponse> {

        public DeleteIndexResponseListener(ParsedStatement stmt,
                                           ActionListener<SQLResponse> listener,
                                           long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(DeleteIndexResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class ClusterUpdateSettingsResponseListener extends ESResponseToSQLResponseListener<ClusterUpdateSettingsResponse> {

        public ClusterUpdateSettingsResponseListener(ParsedStatement stmt,
                                                     ActionListener<SQLResponse> listener,
                                                     long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(ClusterUpdateSettingsResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class ImportResponseListener extends ESResponseToSQLResponseListener<ImportResponse> {

        public ImportResponseListener(ParsedStatement stmt,
                                      ActionListener<SQLResponse> listener,
                                      long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(ImportResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

}
