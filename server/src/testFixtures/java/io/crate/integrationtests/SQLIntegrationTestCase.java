/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static io.crate.protocols.postgres.PostgresNetty.PSQL_PORT_SETTING;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.ConfigurationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Netty4Plugin;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import io.crate.Constants;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParamTypeHints;
import io.crate.common.collections.Lists2;
import io.crate.common.unit.TimeValue;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.execution.dml.TransportShardAction;
import io.crate.execution.dml.delete.TransportShardDeleteAction;
import io.crate.execution.dml.upsert.TransportShardUpsertAction;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillableCallable;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.SessionSettings;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.sql.Identifiers;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.SystemPropsTestLoggingListener;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.TestExecutionConfig;
import io.crate.testing.TestingRowConsumer;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedSchema;
import io.crate.types.DataType;
import io.crate.user.User;
import io.crate.user.UserLookup;

@Listeners({SystemPropsTestLoggingListener.class})
@UseJdbc
@UseHashJoins
@UseRandomizedSchema
public abstract class SQLIntegrationTestCase extends ESIntegTestCase {

    private static final Logger LOGGER = LogManager.getLogger(SQLIntegrationTestCase.class);
    private static final int ORIGINAL_PAGE_SIZE = Paging.PAGE_SIZE;

    protected static SessionSettings DUMMY_SESSION_INFO = new SessionSettings(
        "dummyUser",
        SearchPath.createSearchPathFrom("dummySchema"));

    /**
     * Annotation for tests that are slow. Slow tests do not run by default but can be
     * enabled.
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = RUN_SLOW_TESTS_PROP)
    public @interface Slow {}

    public static final String RUN_SLOW_TESTS_PROP = "tests.crate.slow";

    @Rule
    public Timeout globalTimeout = new Timeout(5, TimeUnit.MINUTES);

    @Rule
    public TestName testName = new TestName();

    protected final SQLTransportExecutor sqlExecutor;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SETTING_HTTP_COMPRESSION.getKey(), false)
            .put(PSQL_PORT_SETTING.getKey(), 0);
        if (randomBoolean()) {
            builder.put("memory.allocation.type", "off-heap");
        }
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Netty4Plugin.class);
    }

    protected SQLResponse response;

    public SQLIntegrationTestCase() {
        this(false);
    }

    public SQLIntegrationTestCase(boolean useSSL) {
        this(new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    return ESIntegTestCase.client();
                }

                @Override
                public String pgUrl() {
                    PostgresNetty postgresNetty = internalCluster().getInstance(PostgresNetty.class);
                    BoundTransportAddress boundTransportAddress = postgresNetty.boundAddress();
                    if (boundTransportAddress != null) {
                        InetSocketAddress address = boundTransportAddress.publishAddress().address();
                        return String.format(Locale.ENGLISH, "jdbc:postgresql://%s:%d/?ssl=%s&sslmode=%s",
                            address.getHostName(), address.getPort(), useSSL, useSSL ? "require" : "disable");
                    }
                    return null;
                }

                @Override
                public SQLOperations sqlOperations() {
                    return internalCluster().getInstance(SQLOperations.class);
                }
            }));
    }

    public static SQLTransportExecutor executor(String nodeName) {
        return executor(nodeName, false);
    }

    public static SQLTransportExecutor executor(String nodeName, boolean useSSL) {
        return new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    return ESIntegTestCase.client(nodeName);
                }

                @Override
                public String pgUrl() {
                    PostgresNetty postgresNetty = internalCluster().getInstance(PostgresNetty.class, nodeName);
                    BoundTransportAddress boundTransportAddress = postgresNetty.boundAddress();
                    if (boundTransportAddress != null) {
                        InetSocketAddress address = boundTransportAddress.publishAddress().address();
                        return String.format(Locale.ENGLISH, "jdbc:postgresql://%s:%d/?ssl=%s&sslmode=%s",
                                             address.getHostName(), address.getPort(), useSSL, useSSL ? "require" : "disable");
                    }
                    return null;
                }

                @Override
                public SQLOperations sqlOperations() {
                    return internalCluster().getInstance(SQLOperations.class);
                }
            });
    }

    public String getFqn(String schema, String tableName) {
        if (schema.equals(Schemas.DOC_SCHEMA_NAME)) {
            return tableName;
        }
        return String.format(Locale.ENGLISH, "%s.%s", schema, tableName);
    }

    public String getFqn(String tableName) {
        return getFqn(sqlExecutor.getCurrentSchema(), tableName);
    }

    @Before
    public void setSearchPath() throws Exception {
        sqlExecutor.setSearchPath(RandomizedSchema());
    }

    @After
    public void resetPageSize() throws Exception {
        Paging.PAGE_SIZE = ORIGINAL_PAGE_SIZE;
    }

    public SQLIntegrationTestCase(SQLTransportExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    @Override
    public Settings indexSettings() {
        // set number of replicas to 0 for getting a green cluster when using only one node
        return Settings.builder().put("number_of_replicas", 0).build();
    }

    @After
    public void assertNoTasksAreLeftOpen() throws Exception {
        final Field activeTasks = TasksService.class.getDeclaredField("activeTasks");
        final Field activeOperationsSb = TransportShardAction.class.getDeclaredField("activeOperations");

        activeTasks.setAccessible(true);
        activeOperationsSb.setAccessible(true);
        try {
            assertBusy(() -> {
                for (TasksService tasksService : internalCluster().getInstances(TasksService.class)) {
                    try {
                        //noinspection unchecked
                        Map<UUID, RootTask> contexts = (Map<UUID, RootTask>) activeTasks.get(tasksService);
                        assertThat(contexts.size(), is(0));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
                for (TransportShardUpsertAction action : internalCluster().getInstances(TransportShardUpsertAction.class)) {
                    try {
                        ConcurrentHashMap<TaskId, KillableCallable<?>> operations = (ConcurrentHashMap<TaskId, KillableCallable<?>>) activeOperationsSb.get(action);
                        assertThat(operations.size(), is(0));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
                for (TransportShardDeleteAction action : internalCluster().getInstances(TransportShardDeleteAction.class)) {
                    try {
                        ConcurrentHashMap<TaskId, KillableCallable<?>> operations = (ConcurrentHashMap<TaskId, KillableCallable<?>>) activeOperationsSb.get(action);
                        assertThat(operations.size(), is(0));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, 10L, TimeUnit.SECONDS);
        } catch (AssertionError e) {
            StringBuilder errorMessageBuilder = new StringBuilder();
            errorMessageBuilder.append("Open jobs:\n");
            for (var jobsLogService : internalCluster().getInstances(JobsLogService.class)) {
                JobsLogs jobsLogs = jobsLogService.get();
                for (var jobContent : jobsLogs.activeJobs()) {
                    errorMessageBuilder.append(jobContent.toString()).append("\n");
                }
            }
            errorMessageBuilder.append("Active tasks:\n");
            String[] nodeNames = internalCluster().getNodeNames();
            for (String nodeName : nodeNames) {
                TasksService tasksService = internalCluster().getInstance(TasksService.class, nodeName);
                try {
                    //noinspection unchecked
                    Map<UUID, RootTask> contexts = (Map<UUID, RootTask>) activeTasks.get(tasksService);
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
                    throw new RuntimeException(ex);
                }
            }
            throw new AssertionError(errorMessageBuilder.toString(), e);
        }
    }

    @After
    public void ensureNoInflightRequestsLeft() throws Exception {
        assertBusy(() -> {
            for (var nodeLimits : internalCluster().getInstances(NodeLimits.class)) {
                assertThat(nodeLimits.totalNumInflight(), is(0L));
            }
        });
    }

    @After
    public void ensure_one_node_limit_instance_per_node() throws Exception {
        Iterable<NodeLimits> nodeLimitsInstances = internalCluster().getInstances(NodeLimits.class);
        int numInstances = 0;
        for (var nodeLimits : nodeLimitsInstances) {
            numInstances++;
        }
        assertThat(
            "There must only be as many NodeLimits instances as there are nodes in the cluster",
            numInstances,
            is(internalCluster().numNodes())
        );
    }

    public void waitUntilShardOperationsFinished() throws Exception {
        assertBusy(() -> {
            Iterable<IndicesService> indexServices = internalCluster().getInstances(IndicesService.class);
            for (IndicesService indicesService : indexServices) {
                for (IndexService indexService : indicesService) {
                    for (IndexShard indexShard : indexService) {
                        assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
                    }
                }
            }
        }, 5, TimeUnit.SECONDS);
    }

    public void waitUntilThreadPoolTasksFinished(final String name) throws Exception {
        assertBusy(() -> {
            Iterable<ThreadPool> threadPools = internalCluster().getInstances(ThreadPool.class);
            for (ThreadPool threadPool : threadPools) {
                ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(name);
                assertThat(executor.getActiveCount(), is(0));
            }
        }, 5, TimeUnit.SECONDS);
    }

    /**
     * Execute a SQL statement as system query on a specific node in the cluster
     *
     * @param stmt      the SQL statement
     * @param schema    the schema that should be used for this statement
     *                  schema is nullable, which means the default schema ("doc") is used
     * @param node      the name of the node on which the stmt is executed
     * @return          the SQL Response
     */
    public SQLResponse systemExecute(String stmt, @Nullable String schema, String node) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, node);
        UserLookup userLookup;
        try {
            userLookup = internalCluster().getInstance(UserLookup.class, node);
        } catch (ConfigurationException ignored) {
            // If enterprise is not enabled there is no UserLookup instance bound in guice
            userLookup = userName -> User.CRATE_USER;
        }
        try (Session session = sqlOperations.createSession(schema, userLookup.findUser("crate"))) {
            response = sqlExecutor.exec(stmt, session);
        }
        return response;
    }

    private void dumpActiveTasks() {
        for (var tasksService : internalCluster().getInstances(TasksService.class)) {
            tasksService.logActiveTasksToError();
        }
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @param args the arguments to replace placeholders ("?") in the statement
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, Object[] args) {
        try {
            SQLResponse response = sqlExecutor.exec(new TestExecutionConfig(isJdbcEnabled(), isHashJoinEnabled()), stmt, args);
            this.response = response;
            return response;
        } catch (ElasticsearchTimeoutException e) {
            LOGGER.error("Timeout on SQL statement: {} {}", stmt, e);
            dumpActiveTasks();
            throw e;
        }
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt    the SQL Statement
     * @param args    the arguments of the statement
     * @param timeout internal timeout of the statement
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, Object[] args, TimeValue timeout) {
        try {
            SQLResponse response = sqlExecutor.exec(new TestExecutionConfig(isJdbcEnabled(), isHashJoinEnabled()), stmt, args, timeout);
            this.response = response;
            return response;
        } catch (ElasticsearchTimeoutException e) {
            LOGGER.error("Timeout on SQL statement: {} {}", stmt, e);
            dumpActiveTasks();
            throw e;
        }
    }

    /**
     * Executes {@code statement} once for each entry in {@code setSessionStatementsList}
     *
     * The inner lists of {@code setSessionStatementsList} will be executed before the statement is executed.
     * This is intended to change session settings using `SET ..` statements
     *
     * @param matcher matcher used to assert the result of {@code statement}
     */
    public void executeWith(List<List<String>> setSessionStatementsList,
                            String statement,
                            Matcher<SQLResponse> matcher) {
        for (List<String> setSessionStatements : setSessionStatementsList) {
            try (Session session = sqlExecutor.newSession()) {

                for (String setSessionStatement : setSessionStatements) {
                    sqlExecutor.exec(setSessionStatement, session);
                }

                SQLResponse resp = sqlExecutor.exec(statement, session);
            assertThat(
                "The query:\n\t" + statement + "\nwith session statements: [" +
                String.join(", ", setSessionStatements) + "] must produce correct result",
                resp,
                matcher);
            }
        }
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
        return execute(stmt, null, createSession(schema));
    }

    public static class PlanForNode {
        public final Plan plan;
        final String nodeName;
        public final PlannerContext plannerContext;

        private PlanForNode(Plan plan, String nodeName, PlannerContext plannerContext) {
            this.plan = plan;
            this.nodeName = nodeName;
            this.plannerContext = plannerContext;
        }
    }

    public PlanForNode plan(String stmt) {
        String[] nodeNames = internalCluster().getNodeNames();
        String nodeName = nodeNames[randomIntBetween(1, nodeNames.length) - 1];

        Analyzer analyzer = internalCluster().getInstance(Analyzer.class, nodeName);
        Planner planner = internalCluster().getInstance(Planner.class, nodeName);
        NodeContext nodeCtx = internalCluster().getInstance(NodeContext.class, nodeName);

        SessionContext sessionContext = new SessionContext(
            User.CRATE_USER,
            sqlExecutor.getCurrentSchema()
        );
        CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(sessionContext);
        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            UUID.randomUUID(),
            coordinatorTxnCtx,
            nodeCtx,
            0,
            null
        );
        Plan plan = planner.plan(
            analyzer.analyze(
                SqlParser.createStatement(stmt),
                coordinatorTxnCtx.sessionContext(),
                ParamTypeHints.EMPTY),
            plannerContext);
        return new PlanForNode(plan, nodeName, plannerContext);
    }

    public TestingRowConsumer execute(PlanForNode planForNode) {
        DependencyCarrier dependencyCarrier = internalCluster().getInstance(DependencyCarrier.class, planForNode.nodeName);
        TestingRowConsumer downstream = new TestingRowConsumer();
        planForNode.plan.execute(
            dependencyCarrier,
            planForNode.plannerContext,
            downstream,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        return downstream;
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt     the SQL Statement
     * @param bulkArgs the bulk arguments of the statement
     * @return the SQLResponse
     */
    public long[] execute(String stmt, Object[][] bulkArgs) {
        return sqlExecutor.execBulk(stmt, bulkArgs);
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt     the SQL Statement
     * @param bulkArgs the bulk arguments of the statement
     * @return the SQLResponse
     */
    public long[] execute(String stmt, Object[][] bulkArgs, TimeValue timeout) {
        return sqlExecutor.execBulk(stmt, bulkArgs, timeout);
    }


    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt) {
        return execute(stmt, (Object[]) null);
    }

    /**
     * Execute an SQL Statement using a specific {@link Session}
     * This is useful to execute a query on a specific node or to test using
     * session options like default schema.
     *
     * @param stmt    the SQL Statement
     * @param session the Session to use
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, Session session) {
        return execute(stmt, null, session);
    }

    /**
     * Execute an SQL Statement using a specific {@link Session}
     * This is useful to execute a query on a specific node or to test using
     * session options like default schema.
     *
     * @param stmt    the SQL Statement
     * @param session the Session to use
     * @return the SQLResponse
     */
    public SQLResponse execute(String stmt, Object[] args, Session session) {
        var response = sqlExecutor.exec(stmt, args, session);
        this.response = response;
        return response;
    }

    public SQLResponse execute(String stmt, Object[] args, String node) {
        return execute(stmt, args, node, SQLTransportExecutor.REQUEST_TIMEOUT);
    }

    public SQLResponse execute(String stmt, Object[] args, String node, TimeValue timeout) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, node);
        try (Session session = sqlOperations.createSession(sqlExecutor.getCurrentSchema(), User.CRATE_USER)) {
            SQLResponse response = sqlExecutor.exec(stmt, args, session, timeout);
            this.response = response;
            return response;
        }
    }

    /**
     * Get all mappings from an index as JSON String
     *
     * @param index the name of the index
     * @return the index mapping as String
     * @throws IOException
     */
    protected String getIndexMapping(String index) throws IOException {
        ClusterStateRequest request = new ClusterStateRequest()
            .routingTable(false)
            .nodes(false)
            .metadata(true)
            .indices(index);
        ClusterStateResponse response = client().admin().cluster().state(request)
            .actionGet();

        Metadata metadata = response.getState().metadata();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        IndexMetadata indexMetadata = metadata.iterator().next();
        builder.field(Constants.DEFAULT_MAPPING_TYPE);
        builder.map(indexMetadata.mapping().sourceAsMap());
        builder.endObject();

        return Strings.toString(builder);
    }

    public void waitForMappingUpdateOnAll(final RelationName relationName, final String... fieldNames) throws Exception {
        assertBusy(() -> {
            Iterable<Schemas> referenceInfosIterable = internalCluster().getInstances(Schemas.class);
            for (Schemas schemas : referenceInfosIterable) {
                TableInfo tableInfo = schemas.getTableInfo(relationName);
                assertThat(tableInfo, Matchers.notNullValue());
                for (String fieldName : fieldNames) {
                    ColumnIdent columnIdent = ColumnIdent.fromPath(fieldName);
                    assertThat(tableInfo.getReference(columnIdent), Matchers.notNullValue());
                }
            }
        }, 20L, TimeUnit.SECONDS);
    }

    public void assertFunctionIsCreatedOnAll(String schema, String name, List<DataType<?>> argTypes) throws Exception {
        SearchPath searchPath = SearchPath.pathWithPGCatalogAndDoc();
        assertBusy(() -> {
            Iterable<Functions> functions = internalCluster().getInstances(Functions.class);
            for (Functions function : functions) {
                FunctionImplementation func = function.get(
                    schema,
                    name,
                    Lists2.map(argTypes, t -> Literal.of(t, null)),
                    searchPath);
                assertThat(func, is(not(nullValue())));
                assertThat(func.info().ident().argumentTypes(), is(equalTo(argTypes)));
            }
        }, 20L, TimeUnit.SECONDS);
    }

    public void assertFunctionIsDeletedOnAll(String schema, String name, List<Symbol> arguments) throws Exception {
        assertBusy(() -> {
            Iterable<Functions> functions = internalCluster().getInstances(Functions.class);
            for (Functions function : functions) {
                try {
                    var func = function.get(schema, name, arguments, SearchPath.createSearchPathFrom(schema));
                    if (func != null) {
                        // if no exact function match is found for given arguments,
                        // the function with arguments that can be casted to provided
                        // arguments will be returned. Therefore, we have to assert that
                        // the provided arguments do not match the arguments of the resolved
                        // function if the function was deleted.
                        assertThat(func.info().ident().argumentTypes(), not(equalTo(Symbols.typeView(arguments))));
                    }
                } catch (UnsupportedOperationException e) {
                    assertThat(e.getMessage().startsWith("Unknown function"), is(true));
                }

            }
        }, 20L, TimeUnit.SECONDS);
    }

    public void waitForMappingUpdateOnAll(final String tableOrPartition, final String... fieldNames) throws Exception {
        waitForMappingUpdateOnAll(new RelationName(sqlExecutor.getCurrentSchema(), tableOrPartition), fieldNames);
    }

    /**
     * Get the IndexSettings as JSON String
     *
     * @param index the name of the index
     * @return the IndexSettings as JSON String
     * @throws IOException
     */
    protected String getIndexSettings(String index) throws IOException {
        ClusterStateRequest request = new ClusterStateRequest()
            .routingTable(false)
            .nodes(false)
            .metadata(true)
            .indices(index);
        ClusterStateResponse response = client().admin().cluster().state(request)
            .actionGet();

        Metadata metadata = response.getState().metadata();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        for (IndexMetadata indexMetadata : metadata) {
            builder.startObject(indexMetadata.getIndex().getName());
            builder.startObject("settings");
            Settings settings = indexMetadata.getSettings();
            for (String settingName : settings.keySet()) {
                builder.field(settingName, settings.get(settingName));
            }
            builder.endObject();

            builder.endObject();
        }

        builder.endObject();

        return Strings.toString(builder);
    }

    /**
     * Creates an {@link Session} on a specific node.
     * This can be used to ensure that a request is performed on a specific node.
     *
     * @param nodeName The name of the node to create the session
     * @return The created session
     */
    Session createSessionOnNode(String nodeName) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, nodeName);
        return sqlOperations.createSession(
            sqlExecutor.getCurrentSchema(), User.CRATE_USER);
    }

    /**
     * Creates a {@link Session} with the given default schema
     * and an options list. This is useful if you require a session which differs
     * from the default one.
     *
     * @param defaultSchema The default schema to use. Can be null.
     * @return The created session
     */
    Session createSession(@Nullable String defaultSchema) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        return sqlOperations.createSession(defaultSchema, User.CRATE_USER);
    }

    /**
     * If the Test class or method contains a @UseJdbc annotation then,
     * based on the ratio provided, a random value of true or false is returned.
     * For more details on the ratio see {@link UseJdbc}
     * <p>
     * Method annotations have higher priority than class annotations.
     */
    private boolean isJdbcEnabled() {
        UseJdbc useJdbc = getTestAnnotation(UseJdbc.class);
        if (useJdbc == null) {
            return false;
        }
        return isFeatureEnabled(useJdbc.value());
    }

    /**
     * If the Test class or method is annotated with {@link UseHashJoins} then,
     * based on the provided ratio, a random value of true or false is returned.
     * For more details on the ratio see {@link UseHashJoins}
     * <p>
     * Method annotations have higher priority than class annotations.
     */
    private boolean isHashJoinEnabled() {
        UseHashJoins useHashJoins = getTestAnnotation(UseHashJoins.class);
        if (useHashJoins == null) {
            return false;
        }
        return isFeatureEnabled(useHashJoins.value());
    }

    /**
     * Checks if the current test method or test class is annotated with the provided {@param annotationClass}
     *
     * @return the annotation if one is present or null otherwise
     */
    @Nullable
    private <T extends Annotation> T getTestAnnotation(Class<T> annotationClass) {
        try {
            Class<?> clazz = this.getClass();
            String testMethodName = testName.getMethodName();
            String[] split = testName.getMethodName().split(" ");
            if (split.length > 1) {
                // When we annotate tests with @Repeat the test method name gets augmented with a seed and we won't
                // be able to find it in the class methods, so just grab the method name.
                testMethodName = split[0];
            }

            Method method = clazz.getMethod(testMethodName);
            T annotation = method.getAnnotation(annotationClass);
            if (annotation == null) {
                annotation = clazz.getAnnotation(annotationClass);
            }
            return annotation;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * We sometimes randomize the use of features in tests.
     * This method verifies the provided ratio and based on it and a random number
     * indicates if a feature should be enabled or not.
     *
     * @param ratio a number between [0, 1] that indicates a "randomness factor"
     *              (1 = always enabled, 0 = always disabled)
     *
     * @return true if a feature should be active/used and false otherwise
     */
    private boolean isFeatureEnabled(double ratio) {
        if (ratio == 0) {
            return false;
        }

        assert ratio >= 0.0 && ratio <= 1.0;
        return ratio == 1 || RandomizedContext.current().getRandom().nextDouble() < ratio;
    }


    /**
     * If the Test class or method contains a @UseRandomizedSchema annotation then,
     * based on the schema argument, a random (unquoted) schema name is returned. The schema name consists
     * of a 1-20 character long ASCII string.
     * For more details on the schema parameter see {@link UseRandomizedSchema}
     * <p>
     * Method annotations have higher priority than class annotations.
     */
    private String RandomizedSchema() {
        UseRandomizedSchema annotation = getTestAnnotation(UseRandomizedSchema.class);
        if (annotation == null || annotation.random() == false) {
            return Schemas.DOC_SCHEMA_NAME;
        }

        Random random = RandomizedContext.current().getRandom();
        while (true) {
            String schemaName = RandomStrings.randomAsciiLettersOfLengthBetween(random, 1, 20).toLowerCase();
            if (!Schemas.READ_ONLY_SYSTEM_SCHEMAS.contains(schemaName) &&
                !Identifiers.isKeyWord(schemaName) &&
                !containsExtendedAsciiChars(schemaName)) {
                return schemaName;
            }
        }
    }

    private boolean containsExtendedAsciiChars(String value) {
        for (char c : value.toCharArray()) {
            if ((short) c > 127) {
                return true;
            }
        }
        return false;
    }

    public static Index resolveIndex(String index) {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        IndexMetadata indexMetadata = clusterService.state().metadata().index(index);
        return new Index(index, indexMetadata.getIndexUUID());
    }
}
