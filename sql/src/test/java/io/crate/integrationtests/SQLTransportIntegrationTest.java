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
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import io.crate.action.sql.Option;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.data.Row;
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
import io.crate.operation.Paging;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.plugin.BlobPlugin;
import io.crate.plugin.CrateCorePlugin;
import io.crate.plugin.SQLPlugin;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.sql.parser.SqlParser;
import io.crate.test.GroovyTestSanitizer;
import io.crate.test.integration.SystemPropsTestLoggingListener;
import io.crate.testing.SQLBulkResponse;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.TestingBatchConsumer;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.SQLTransportExecutor.DEFAULT_SOFT_LIMIT;
import static org.hamcrest.Matchers.is;

@Listeners({SystemPropsTestLoggingListener.class})
public abstract class SQLTransportIntegrationTest extends ESIntegTestCase {

    @Rule
    public Timeout globalTimeout = new Timeout(120000); // 2 minutes timeout

    private static final int ORIGINAL_PAGE_SIZE = Paging.PAGE_SIZE;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        GroovyTestSanitizer.isGroovySanitized();
    }

    protected final SQLTransportExecutor sqlExecutor;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(BlobPlugin.class, SQLPlugin.class, CrateCorePlugin.class);
    }

    protected SQLResponse response;

    public SQLTransportIntegrationTest() {
        this(new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    return ESIntegTestCase.client();
                }

                @Override
                public String pgUrl() {
                    PostgresNetty postgresNetty = internalCluster().getDataNodeInstance(PostgresNetty.class);
                    Iterator<InetSocketTransportAddress> addressIter = postgresNetty.boundAddresses().iterator();
                    if (addressIter.hasNext()) {
                        InetSocketTransportAddress address = addressIter.next();
                        return String.format(Locale.ENGLISH, "jdbc:crate://%s:%d/",
                            address.getHost(), address.getPort());
                    }
                    return null;
                }

                @Override
                public SQLOperations sqlOperations() {
                    return internalCluster().getInstance(SQLOperations.class);
                }
            }));
    }

    @After
    public void resetPageSize() throws Exception {
        Paging.PAGE_SIZE = ORIGINAL_PAGE_SIZE;
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

    public void waitUntilThreadPoolTasksFinished(final String name) throws Exception {
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
     * @param stmt    the SQL statement
     * @param schema  the schema that should be used for this statement
     *                schema is nullable, which means default schema ("doc") is used
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, @Nullable String schema) {
        return execute(stmt, null, createSession(schema, Option.NONE));
    }

    public static class PlanForNode {
        final Plan plan;
        final String nodeName;

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

        ParameterContext parameterContext = new ParameterContext(Row.EMPTY, Collections.<Row>emptyList());
        Plan plan = planner.plan(analyzer.boundAnalyze(SqlParser.createStatement(stmt), SessionContext.SYSTEM_SESSION, parameterContext), UUID.randomUUID(), 0, 0);
        return new PlanForNode(plan, nodeName);
    }

    public TestingBatchConsumer execute(PlanForNode planForNode) {
        TransportExecutor transportExecutor = internalCluster().getInstance(TransportExecutor.class, planForNode.nodeName);
        TestingBatchConsumer downstream = new TestingBatchConsumer();
        transportExecutor.execute(planForNode.plan, downstream, Row.EMPTY);
        return downstream;
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt     the SQL Statement
     * @param bulkArgs the bulk arguments of the statement
     * @return the SQLResponse
     */
    public SQLBulkResponse execute(String stmt, Object[][] bulkArgs) {
        return sqlExecutor.execBulk(stmt, bulkArgs);
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt     the SQL Statement
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
        return execute(stmt, (Object[])null);
    }


    /**
     * Execute an SQL Statement using a specific {@link SQLOperations.Session}
     * This is useful to execute a query on a specific node or to test using
     * session options like default schema.
     *
     * @param stmt the SQL Statement
     * @param session the Session to use
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, Object[] args, SQLOperations.Session session) {
        response = SQLTransportExecutor.execute(stmt, args, session)
            .actionGet(SQLTransportExecutor.REQUEST_TIMEOUT);
        return response;
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
        for (ObjectCursor<MappingMetaData> cursor : indexMetaData.getMappings().values()) {
            builder.field(cursor.value.type());
            builder.map(cursor.value.sourceAsMap());
        }
        builder.endObject();

        return builder.string();
    }

    public void waitForMappingUpdateOnAll(final TableIdent tableIdent, final String... fieldNames) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                Iterable<Schemas> referenceInfosIterable = internalCluster().getInstances(Schemas.class);
                for (Schemas schemas : referenceInfosIterable) {
                    TableInfo tableInfo = schemas.getTableInfo(tableIdent);
                    assertThat(tableInfo, Matchers.notNullValue());
                    for (String fieldName : fieldNames) {
                        ColumnIdent columnIdent = ColumnIdent.fromPath(fieldName);
                        assertThat(tableInfo.getReference(columnIdent), Matchers.notNullValue());
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
     * Creates an {@link SQLOperations.Session} on a specific node.
     * This can be used to ensure that a request is performed on a specific node.
     *
     * @param nodeName The name of the node to create the session
     * @return The created session
     */
    SQLOperations.Session createSessionOnNode(String nodeName) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, nodeName);
        return sqlOperations.createSession(null, Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    /**
     * Creates a {@link SQLOperations.Session} with the given default schema
     * and an options list. This is useful if you require a session which differs
     * from the default one.
     *
     * @param defaultSchema The default schema to use. Can be null
     * @param options Session options. If no specific options are required, use {@link Option#NONE}
     * @return The created session
     */
    SQLOperations.Session createSession(@Nullable String defaultSchema, Set<Option> options) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        return sqlOperations.createSession(defaultSchema, options, DEFAULT_SOFT_LIMIT);
    }

}
