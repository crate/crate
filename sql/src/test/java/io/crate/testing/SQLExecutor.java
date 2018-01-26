/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.testing;

import com.google.common.base.Preconditions;
import io.crate.Constants;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.analyze.CreateTableAnalyzedStatement;
import io.crate.analyze.CreateTableStatementAnalyzer;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.analyze.relations.SubselectRewriter;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.expression.symbol.Symbol;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.Rows;
import io.crate.execution.ddl.RepositoryService;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.TestingDocTableInfoFactory;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.TableStats;
import io.crate.planner.operators.LogicalPlan;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.QualifiedName;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static io.crate.analyze.TableDefinitions.DEEPLY_NESTED_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.NESTED_PK_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_CLUSTER_BY_STRING_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_DOC_TRANSACTIONS_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_MULTIPLE_PARTITIONED_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_NESTED_PARTITIONED_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO_CLUSTERED_BY_ONLY;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO_MULTI_PK;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO_REFRESH_INTERVAL_BY_ONLY;
import static io.crate.testing.TestingHelpers.getFunctions;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.mockito.Mockito.mock;

/**
 * Lightweight alternative to {@link SQLTransportExecutor}.
 *
 * Can be used for unit-tests testss which don't require the full execution-layer/nodes to be started.
 */
public class SQLExecutor {

    private static final Logger LOGGER = Loggers.getLogger(SQLExecutor.class);

    private final Functions functions;
    public final Analyzer analyzer;
    public final Planner planner;
    private final RelationAnalyzer relAnalyzer;
    private final SessionContext sessionContext;
    private final TransactionContext transactionContext;
    private final Random random;

    /**
     * Shortcut for {@link #getPlannerContext(ClusterState, Random)}
     * This can only be used if {@link com.carrotsearch.randomizedtesting.RandomizedContext} is available
     * (e.g. TestCase using {@link com.carrotsearch.randomizedtesting.RandomizedRunner}
     */
    public PlannerContext getPlannerContext(ClusterState clusterState) {
        return getPlannerContext(clusterState, Randomness.get());
    }

    public PlannerContext getPlannerContext(ClusterState clusterState, Random random) {
        return new PlannerContext(
            clusterState,
            new RoutingProvider(random.nextInt(), new String[0]),
            UUID.randomUUID(),
            functions,
            new TransactionContext(sessionContext),
            -1,
            -1
        );
    }

    public static class Builder {

        private final ClusterService clusterService;
        private final Map<String, SchemaInfo> schemaInfoByName = new HashMap<>();
        private final Map<TableIdent, DocTableInfo> docTables = new HashMap<>();
        private final Map<TableIdent, BlobTableInfo> blobTables = new HashMap<>();
        private final Functions functions;
        private final TestingDocTableInfoFactory testingDocTableInfoFactory;
        private final AnalysisRegistry analysisRegistry;
        private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
        private final AllocationService allocationService;
        private final UserDefinedFunctionService udfService;
        private final Random random;
        private String defaultSchema = Schemas.DOC_SCHEMA_NAME;

        private TableStats tableStats = new TableStats();

        private Builder(ClusterService clusterService, int numNodes, Random random) {
            this.random = random;
            Preconditions.checkArgument(numNodes >= 1, "Must have at least 1 node");
            addNodesToClusterState(clusterService, numNodes);
            this.clusterService = clusterService;
            schemaInfoByName.put("sys", new SysSchemaInfo());
            schemaInfoByName.put("information_schema", new InformationSchemaInfo());
            functions = getFunctions();

            testingDocTableInfoFactory = new TestingDocTableInfoFactory(
                docTables, functions, new IndexNameExpressionResolver(Settings.EMPTY));
            udfService = new UserDefinedFunctionService(clusterService, functions);
            schemaInfoByName.put(
                defaultSchema,
                new DocSchemaInfo(defaultSchema, clusterService, functions, udfService, testingDocTableInfoFactory)
            );
            Schemas schemas = new Schemas(
                Settings.EMPTY,
                schemaInfoByName,
                clusterService,
                new DocSchemaInfoFactory(testingDocTableInfoFactory, functions, udfService)
            );
            File tempDir = createTempDir();
            analysisRegistry = new AnalysisRegistry(
                new Environment(Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), tempDir.toString())
                    .build()),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            createTableStatementAnalyzer = new CreateTableStatementAnalyzer(
                schemas,
                new FulltextAnalyzerResolver(clusterService, analysisRegistry),
                functions,
                new NumberOfShards(clusterService)
            );
            allocationService = new AllocationService(
                Settings.EMPTY,
                new AllocationDeciders(
                    Settings.EMPTY,
                    singletonList(new SameShardAllocationDecider(Settings.EMPTY, clusterService.getClusterSettings()))
                ),
                new TestGatewayAllocator(),
                new BalancedShardsAllocator(Settings.EMPTY),
                EmptyClusterInfoService.INSTANCE
            );
        }

        private void addNodesToClusterState(ClusterService clusterService, int numNodes) {
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
            for (int i = 1; i <= numNodes; i++) {
                builder.add(new DiscoveryNode("n" + i, LocalTransportAddress.buildUnique(), Version.CURRENT));
            }
            builder.localNodeId("n1");
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(clusterService.state()).nodes(builder).build());
        }

        public Builder setDefaultSchema(String schema) {
            this.defaultSchema = schema;
            return this;
        }

        /**
         * Adds a couple of tables which are defined in {@link T3} and {@link io.crate.analyze.TableDefinitions}.
         * <p>
         * Note that these tables do have a static routing which doesn't necessarily match the nodes that
         * are part of the clusterState of the {@code clusterService} provided to the builder.
         *
         * Note that these tables are not part of the clusterState and rely on a stubbed getRouting
         * Using {@link #addTable(String)} is preferred for this reason.
         * </p>
         */
        public Builder enableDefaultTables() {
            // we should try to reduce the number of tables here eventually...
            addDocTable(USER_TABLE_INFO);
            addDocTable(USER_TABLE_INFO_CLUSTERED_BY_ONLY);
            addDocTable(USER_TABLE_INFO_MULTI_PK);
            addDocTable(DEEPLY_NESTED_TABLE_INFO);
            addDocTable(NESTED_PK_TABLE_INFO);
            addDocTable(TEST_PARTITIONED_TABLE_INFO);
            addDocTable(TEST_NESTED_PARTITIONED_TABLE_INFO);
            addDocTable(TEST_MULTIPLE_PARTITIONED_TABLE_INFO);
            addDocTable(TEST_DOC_TRANSACTIONS_TABLE_INFO);
            addDocTable(TEST_DOC_LOCATIONS_TABLE_INFO);
            addDocTable(TEST_CLUSTER_BY_STRING_TABLE_INFO);
            addDocTable(USER_TABLE_INFO_REFRESH_INTERVAL_BY_ONLY);
            addDocTable(T3.T1_INFO);
            addDocTable(T3.T2_INFO);
            addDocTable(T3.T3_INFO);
            return this;
        }

        public SQLExecutor build() {
            if (!blobTables.isEmpty()) {
                schemaInfoByName.put(
                    BlobSchemaInfo.NAME,
                    new BlobSchemaInfo(clusterService, new TestingBlobTableInfoFactory(blobTables)));
            }
            // Can't use the schemas instance from the constructor because
            // schemaInfoByName can receive new items and Schemas creates a new map internally; so mutations are not visible
            Schemas schemas = new Schemas(
                Settings.EMPTY,
                schemaInfoByName,
                clusterService,
                new DocSchemaInfoFactory(testingDocTableInfoFactory, functions, udfService)
            );
            return new SQLExecutor(
                functions,
                new Analyzer(
                    schemas,
                    functions,
                    clusterService,
                    analysisRegistry,
                    new RepositoryService(
                        clusterService,
                        mock(TransportDeleteRepositoryAction.class),
                        mock(TransportPutRepositoryAction.class)
                    ),
                    new ModulesBuilder().add(new RepositorySettingsModule())
                        .createInjector()
                        .getInstance(RepositoryParamValidator.class)
                ),
                new Planner(
                    Settings.EMPTY,
                    clusterService,
                    functions,
                    tableStats
                ),
                new RelationAnalyzer(functions, schemas),
                new SessionContext(defaultSchema, null, s -> {}, t -> {}),
                random
            );
        }

        private static File createTempDir() {
            int attempt = 0;
            while (attempt < 3) {
                try {
                    attempt++;
                    File tempDir = File.createTempFile("temp", Long.toString(System.nanoTime()));
                    tempDir.deleteOnExit();
                    return tempDir;
                } catch (IOException e) {
                    LOGGER.warn("Unable to create temp dir on attempt {} due to {}", attempt, e.getMessage());
                }
            }
            throw new IllegalStateException("Cannot create temp dir");
        }

        public Builder addSchema(SchemaInfo schema) {
            schemaInfoByName.put(schema.name(), schema);
            return this;
        }

        /**
         * Adds a table to the schemas.
         * Note that these tables won't be part of the clusterState.
         * Using {@link #addTable(String)} is preferred for this reason
         */
        public Builder addDocTable(DocTableInfo table) {
            docTables.put(table.ident(), table);
            return this;
        }

        /**
         * Add a table to the clusterState
         */
        public Builder addTable(String createTableStmt) throws IOException {
            CreateTable stmt = (CreateTable) SqlParser.createStatement(createTableStmt);
            CreateTableAnalyzedStatement analyzedStmt = createTableStatementAnalyzer.analyze(
                stmt, ParameterContext.EMPTY, new TransactionContext(SessionContext.create()));

            if (analyzedStmt.isPartitioned()) {
                // this is simply not implemented: We'd have to add the template instead of adding indexMetaData
                throw new UnsupportedOperationException("Cannot add partitioned table");
            }
            ClusterState prevState = clusterService.state();
            IndexMetaData indexMetaData = IndexMetaData.builder(analyzedStmt.tableIdent().name())
                .settings(
                    Settings.builder()
                        .put(analyzedStmt.tableParameter().settings())
                        .put(SETTING_VERSION_CREATED, prevState.nodes().getSmallestNonClientNodeVersion())
                        .build()
                )
                .putMapping(new MappingMetaData(Constants.DEFAULT_MAPPING_TYPE, analyzedStmt.mapping()))
                .build();

            ClusterState state = ClusterState.builder(prevState)
                .metaData(MetaData.builder(prevState.metaData()).put(indexMetaData, true))
                .routingTable(RoutingTable.builder(prevState.routingTable()).addAsNew(indexMetaData).build())
                .build();

            ClusterServiceUtils.setState(clusterService, allocationService.reroute(state, "assign shards"));
            return this;
        }

        public Builder addDocTable(TestingTableInfo.Builder builder) {
            return addDocTable(builder.build(functions));
        }

        public Builder addBlobTable(BlobTableInfo tableInfo) {
            blobTables.put(tableInfo.ident(), tableInfo);
            return this;
        }

        public Builder setTableStats(TableStats tableStats) {
            this.tableStats = tableStats;
            return this;
        }
    }

    public static Builder builder(ClusterService clusterService) {
        return new Builder(clusterService, 1, Randomness.get());
    }

    public static Builder builder(ClusterService clusterService, int numNodes, Random random) {
        return new Builder(clusterService, numNodes, random);
    }

    private SQLExecutor(Functions functions,
                        Analyzer analyzer,
                        Planner planner,
                        RelationAnalyzer relAnalyzer,
                        SessionContext sessionContext,
                        Random random) {
        this.functions = functions;
        this.analyzer = analyzer;
        this.planner = planner;
        this.relAnalyzer = relAnalyzer;
        this.sessionContext = sessionContext;
        this.transactionContext = new TransactionContext(sessionContext);
        this.random = random;
    }

    public Functions functions() {
        return functions;
    }

    private <T extends AnalyzedStatement> T analyze(String stmt, ParameterContext parameterContext) {
        Analysis analysis = analyzer.boundAnalyze(
            SqlParser.createStatement(stmt), transactionContext, parameterContext);
        //noinspection unchecked
        return (T) analysis.analyzedStatement();
    }

    public <T extends AnalyzedStatement> T analyze(String statement) {
        return analyze(statement, ParameterContext.EMPTY);
    }

    public <T extends AnalyzedStatement> T analyze(String statement, Object[] arguments) {
        return analyze(
            statement,
            arguments.length == 0
                ? ParameterContext.EMPTY
                : new ParameterContext(new RowN(arguments), Collections.emptyList()));
    }

    public <T extends AnalyzedStatement> T analyze(String statement, Object[][] bulkArgs) {
        return analyze(statement, new ParameterContext(Row.EMPTY, Rows.of(bulkArgs)));
    }

    /**
     * Convert a expression to a symbol
     *
     * @param sources The relations which are accessible in the expression.
     *                Think of this as the FROM clause.
     *                If tables are used here they must also be registered in the SQLExecutor having used {@link Builder#addDocTable(DocTableInfo)}
     */
    public Symbol asSymbol(Map<QualifiedName, AnalyzedRelation> sources, String expression) {
        TransactionContext transactionContext = new TransactionContext(sessionContext);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            transactionContext,
            ParamTypeHints.EMPTY,
            new FullQualifiedNameFieldProvider(sources, ParentRelations.NO_PARENTS, sessionContext.defaultSchema()),
            new SubqueryAnalyzer(
                relAnalyzer,
                new StatementAnalysisContext(ParamTypeHints.EMPTY, Operation.READ, transactionContext)
            )
        );
        return expressionAnalyzer.convert(
            SqlParser.createExpression(expression), new ExpressionAnalysisContext());
    }


    public <T> T plan(String statement, UUID jobId, int softLimit, int fetchSize) {
        Analysis analysis = analyzer.boundAnalyze(
            SqlParser.createStatement(statement), transactionContext, ParameterContext.EMPTY);
        RoutingProvider routingProvider = new RoutingProvider(random.nextInt(), new String[0]);
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            jobId,
            functions,
            transactionContext,
            softLimit,
            fetchSize
        );
        Plan plan = planner.plan(analysis.analyzedStatement(), plannerContext);
        if (plan instanceof LogicalPlan) {
            return (T) ((LogicalPlan) plan).build(
                plannerContext,
                new ProjectionBuilder(functions),
                TopN.NO_LIMIT,
                0,
                null,
                null,
                Row.EMPTY,
                Collections.emptyMap()
            );
        }
        return (T) plan;
    }

    public <T extends LogicalPlan> T logicalPlan(String statement) {
        AnalyzedStatement stmt = analyzer.unboundAnalyze(
            SqlParser.createStatement(statement), sessionContext, ParamTypeHints.EMPTY);
        if (stmt instanceof AnalyzedRelation) {
            // unboundAnalyze currently doesn't normalize; which breaks LogicalPlan building for joins
            // because the subRelations are not yet QueriedRelations.
            // we eventually should integrate normalize into unboundAnalyze.
            // But then we have to remove the whereClauseAnalyzer calls because they depend on all values being available
            AnalyzedRelation rewrittenRelation = SubselectRewriter.rewrite(((AnalyzedRelation) stmt));
            stmt = (AnalyzedStatement) new RelationNormalizer(functions)
                .normalize(rewrittenRelation, transactionContext);
        }
        return (T) planner.plan(stmt, getPlannerContext(planner.currentClusterState()));
    }

    public <T> T plan(String stmt, Row row) {
        AnalyzedStatement analyzedStatement = analyzer.unboundAnalyze(
            SqlParser.createStatement(stmt),
            sessionContext,
            ParamTypeHints.EMPTY
        );
        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), new String[0]);
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            UUID.randomUUID(),
            functions,
            transactionContext,
            0,
            0
        );
        Plan plan = planner.plan(analyzedStatement, plannerContext);
        if (plan instanceof LogicalPlan) {
            return (T) ((LogicalPlan) plan).build(
                plannerContext,
                new ProjectionBuilder(functions),
                TopN.NO_LIMIT,
                0,
                null,
                null,
                Row.EMPTY,
                Collections.emptyMap()
            );
        }
        return (T) plan;
    }


    public <T> T plan(String statement) {
        return plan(statement, UUID.randomUUID(), 0, 0);
    }
}
