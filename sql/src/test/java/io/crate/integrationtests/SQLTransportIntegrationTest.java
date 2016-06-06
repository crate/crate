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

package io.crate.integrationtests;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.*;
import io.crate.action.sql.parser.SQLXContentSourceContext;
import io.crate.action.sql.parser.SQLXContentSourceParser;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportExecutor;
import io.crate.executor.transport.TransportShardAction;
import io.crate.executor.transport.TransportShardDeleteAction;
import io.crate.executor.transport.TransportShardUpsertAction;
import io.crate.executor.transport.kill.KillableCallable;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.plugin.CrateCorePlugin;
import io.crate.sql.parser.SqlParser;
import io.crate.test.GroovyTestSanitizer;
import io.crate.testing.SQLTransportExecutor;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public abstract class SQLTransportIntegrationTest extends ESIntegTestCase {

    static {
        GroovyTestSanitizer.isGroovySanitized();
    }

    protected final SQLTransportExecutor sqlExecutor;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CrateCorePlugin.class);
    }

    protected float responseDuration;
    protected SQLResponse response;

    public SQLTransportIntegrationTest() {
        this(new SQLTransportExecutor(
                new SQLTransportExecutor.ClientProvider() {
                    @Override
                    public Client client() {
                        return ESIntegTestCase.client();
                    }
                }
        ));
    }

    @Override
    protected double getPerTestTransportClientRatio() {
        return 0.0d;
    }

    public SQLTransportIntegrationTest(SQLTransportExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    @Override
    public Settings indexSettings() {
        // set number of replicas to 0 for getting a green cluster when using only one node
        return Settings.builder().put("number_of_replicas", 0).build();
    }

    @After
    public void assertNoJobExecutionContextAreLeftOpen() throws Exception {
        final Field activeContexts = JobContextService.class.getDeclaredField("activeContexts");
        final Field activeOperationsSb = TransportShardAction.class.getDeclaredField("activeOperations");

        activeContexts.setAccessible(true);
        activeOperationsSb.setAccessible(true);
        try {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    for (JobContextService jobContextService : internalCluster().getInstances(JobContextService.class)) {
                        try {
                            //noinspection unchecked
                            Map<UUID, JobExecutionContext> contexts = (Map<UUID, JobExecutionContext>) activeContexts.get(jobContextService);
                            assertThat(contexts.size(), is(0));
                        } catch (IllegalAccessException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                    for (TransportShardUpsertAction action : internalCluster().getInstances(TransportShardUpsertAction.class)) {
                        try {
                            Multimap<UUID, KillableCallable> operations = (Multimap<UUID, KillableCallable>) activeOperationsSb.get(action);
                            assertThat(operations.size(), is(0));
                        } catch (IllegalAccessException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                    for (TransportShardDeleteAction action : internalCluster().getInstances(TransportShardDeleteAction.class)) {
                        try {
                            Multimap<UUID, KillableCallable> operations = (Multimap<UUID, KillableCallable>) activeOperationsSb.get(action);
                            assertThat(operations.size(), is(0));
                        } catch (IllegalAccessException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                }
            }, 10L, TimeUnit.SECONDS);
        } catch (AssertionError e) {
            StringBuilder errorMessageBuilder = new StringBuilder();
            String[] nodeNames = internalCluster().getNodeNames();
            for (String nodeName : nodeNames) {
                JobContextService jobContextService = internalCluster().getInstance(JobContextService.class, nodeName);
                try {
                    //noinspection unchecked
                    Map<UUID, JobExecutionContext> contexts = (Map<UUID, JobExecutionContext>) activeContexts.get(jobContextService);
                    String contextsString = contexts.toString();
                    if (!"{}".equals(contextsString)) {
                        errorMessageBuilder.append("## node: ");
                        errorMessageBuilder.append(nodeName);
                        errorMessageBuilder.append("\n");
                        errorMessageBuilder.append(contextsString);
                        errorMessageBuilder.append("\n");
                    }
                    contexts.clear();
                } catch (IllegalAccessException ex) {
                    throw Throwables.propagate(e);
                }
            }
            throw new AssertionError(errorMessageBuilder.toString(), e);
        }
    }

    public void waitUntilShardOperationsFinished() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                try {
                    Iterable<IndicesService> indexServices = internalCluster().getInstances(IndicesService.class);
                    for (IndicesService indicesService : indexServices) {
                        for (IndexService indexService : indicesService) {
                            for (IndexShard indexShard : indexService) {
                                assertThat(indexShard.getOperationsCount(), Matchers.equalTo(0));
                            }
                        }
                    }
                } catch (Throwable t) {
                    throw Throwables.propagate(t);
                }
            }
        }, 5, TimeUnit.SECONDS);
    }

    public void waitUntilThreadPoolTasksFinished(final String name) throws Exception{
        assertBusy(new Runnable() {
            @Override
            public void run() {
                Iterable<ThreadPool> threadPools = internalCluster().getInstances(ThreadPool.class);
                for (ThreadPool threadPool : threadPools) {
                    ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(name);
                    assertThat(executor.getActiveCount(), is(0));
                }
            }
        }, 5, TimeUnit.SECONDS);

    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @param args the arguments to replace placeholders ("?") in the statement
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, Object[] args) {
        response = sqlExecutor.exec(stmt, args);
        return response;
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @param args the arguments to replace placeholders ("?") in the statement
     * @param timeout the timeout for this request
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, Object[] args, TimeValue timeout) {
        response = sqlExecutor.exec(stmt, args, timeout);
        return response;
    }

    public static class PlanForNode {
        private final Plan plan;
        private final String nodeName;

        private PlanForNode(Plan plan, String nodeName) {
            this.plan = plan;
            this.nodeName = nodeName;
        }
    }

    public PlanForNode plan(String stmt) {
        String[] nodeNames = internalCluster().getNodeNames();
        String nodeName = nodeNames[randomIntBetween(1, nodeNames.length) - 1];

        Analyzer analyzer = internalCluster().getInstance(Analyzer.class, nodeName);
        Planner planner = internalCluster().getInstance(Planner.class, nodeName);

        ParameterContext parameterContext = new ParameterContext(new Object[0], new Object[0][], null);
        Plan plan = planner.plan(analyzer.analyze(SqlParser.createStatement(stmt), parameterContext), UUID.randomUUID());
        return new PlanForNode(plan, nodeName);
    }

    public ListenableFuture<TaskResult> execute(PlanForNode planForNode) {
        TransportExecutor transportExecutor = internalCluster().getInstance(TransportExecutor.class, planForNode.nodeName);
        return transportExecutor.execute(planForNode.plan);
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @param bulkArgs the bulk arguments of the statement
     * @return the SQLResponse
     */
    public SQLBulkResponse execute(String stmt, Object[][] bulkArgs) {
        return sqlExecutor.execBulk(stmt, bulkArgs);
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @param bulkArgs the bulk arguments of the statement
     * @return the SQLResponse
     */
    public SQLBulkResponse execute(String stmt, Object[][] bulkArgs, TimeValue timeout) {
        return sqlExecutor.execBulk(stmt, bulkArgs, timeout);
    }


    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt) {
        return execute(stmt, SQLRequest.EMPTY_ARGS);
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @param timeout the timeout for this query
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, TimeValue timeout) {
        return execute(stmt, SQLRequest.EMPTY_ARGS, timeout);
    }

    /**
     * Get all mappings from an index as JSON String
     *
     * @param index the name of the index
     * @return the index mapping as String
     * @throws IOException
     */
    protected String getIndexMapping(String index) throws IOException {
        ClusterStateRequest request = Requests.clusterStateRequest()
                .routingTable(false)
                .nodes(false)
                .metaData(true)
                .indices(index);
        ClusterStateResponse response = client().admin().cluster().state(request)
                .actionGet();

        MetaData metaData = response.getState().metaData();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        IndexMetaData indexMetaData = metaData.iterator().next();
        for (ObjectCursor<MappingMetaData> cursor: indexMetaData.getMappings().values()) {
            builder.field(cursor.value.type());
            builder.map(cursor.value.sourceAsMap());
        }
        builder.endObject();

        return builder.string();
    }

    public void waitForMappingUpdateOnAll(final TableIdent tableIdent, final String... fieldNames) throws Exception{
        assertBusy(new Runnable() {
            @Override
            public void run() {
                Iterable<Schemas> referenceInfosIterable = internalCluster().getInstances(Schemas.class);
                for (Schemas schemas : referenceInfosIterable) {
                    TableInfo tableInfo = schemas.getTableInfo(tableIdent);
                    assertThat(tableInfo, Matchers.notNullValue());
                    for (String fieldName : fieldNames) {
                        ColumnIdent columnIdent = ColumnIdent.fromPath(fieldName);
                        assertThat(tableInfo.getReferenceInfo(columnIdent), Matchers.notNullValue());
                    }
                }
            }
        }, 20L, TimeUnit.SECONDS);
    }
    public void waitForMappingUpdateOnAll(final String tableOrPartition, final String... fieldNames) throws Exception {
        waitForMappingUpdateOnAll(new TableIdent(null, tableOrPartition), fieldNames);
    }

    /**
     * Get the IndexSettings as JSON String
     *
     * @param index the name of the index
     * @return the IndexSettings as JSON String
     * @throws IOException
     */
    protected String getIndexSettings(String index) throws IOException {
        ClusterStateRequest request = Requests.clusterStateRequest()
                .routingTable(false)
                .nodes(false)
                .metaData(true)
                .indices(index);
        ClusterStateResponse response = client().admin().cluster().state(request)
                .actionGet();

        MetaData metaData = response.getState().metaData();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        for (IndexMetaData indexMetaData : metaData) {
            builder.startObject(indexMetaData.getIndex(), XContentBuilder.FieldCaseConversion.NONE);
            builder.startObject("settings");
            Settings settings = indexMetaData.getSettings();
            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();

            builder.endObject();
        }

        builder.endObject();

        return builder.string();
    }

    /**
     * Execute an SQLRequest on a random client of the cluster like it would
     * be executed by an HTTP REST Request
     *
     * @param source the body of the statement, a JSON String containing the "stmt" and the "args"
     * @param includeTypes include data types in response
     * @param schema default schema
     * @return the Response as JSON String
     * @throws IOException
     */
    protected String restSQLExecute(String source, boolean includeTypes, @Nullable String schema) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.generator().usePrettyPrint();
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        parser.parseSource(new BytesArray(source));

        SQLBaseResponse sqlResponse;
        Object[][] bulkArgs = context.bulkArgs();
        if (bulkArgs != null && bulkArgs.length > 0) {
            SQLBulkRequestBuilder requestBuilder = new SQLBulkRequestBuilder(client(), SQLBulkAction.INSTANCE);
            requestBuilder.bulkArgs(context.bulkArgs());
            requestBuilder.stmt(context.stmt());
            requestBuilder.includeTypesOnResponse(includeTypes);
            requestBuilder.setSchema(schema);
            sqlResponse = requestBuilder.execute().actionGet();
        } else {
            SQLRequestBuilder requestBuilder = new SQLRequestBuilder(client(), SQLAction.INSTANCE);
            requestBuilder.args(context.args());
            requestBuilder.stmt(context.stmt());
            requestBuilder.includeTypesOnResponse(includeTypes);
            requestBuilder.setSchema(schema);
            sqlResponse = requestBuilder.execute().actionGet();
        }
        sqlResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        responseDuration = sqlResponse.duration();
        return builder.string();
    }

    protected String restSQLExecute(String source, boolean includeTypes) throws IOException {
        return restSQLExecute(source, includeTypes, null);
    }

    protected String restSQLExecute(String source) throws IOException {
        return restSQLExecute(source, false);
    }
}
