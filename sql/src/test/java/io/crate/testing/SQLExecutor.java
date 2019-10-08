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
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedCreateBlobTable;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.analyze.CreateBlobTableAnalyzer;
import io.crate.analyze.CreateTableAnalyzedStatement;
import io.crate.analyze.CreateTableStatementAnalyzer;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.auth.user.User;
import io.crate.auth.user.UserManager;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.Rows;
import io.crate.execution.ddl.RepositoryService;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.doc.TestingDocTableInfoFactory;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.metadata.view.ViewsMetaData;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.TableStats;
import io.crate.planner.node.ddl.CreateBlobTablePlan;
import io.crate.planner.node.ddl.CreateTablePlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;
import io.crate.user.StubUserManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

import static io.crate.analyze.TableDefinitions.DEEPLY_NESTED_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.NESTED_PK_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_CLUSTER_BY_STRING_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_DOC_TRANSACTIONS_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS;
import static io.crate.analyze.TableDefinitions.USER_TABLE_CLUSTERED_BY_ONLY_DEFINITION;
import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.USER_TABLE_MULTI_PK_DEFINITION;
import static io.crate.analyze.TableDefinitions.USER_TABLE_REFRESH_INTERVAL_BY_ONLY_DEFINITION;
import static io.crate.blob.v2.BlobIndex.fullIndexName;
import static io.crate.testing.DiscoveryNodes.newFakeAddress;
import static io.crate.testing.TestingHelpers.getFunctions;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.mockito.Mockito.mock;

/**
 * Lightweight alternative to {@link SQLTransportExecutor}.
 *
 * Can be used for unit-tests testss which don't require the full execution-layer/nodes to be started.
 */
public class SQLExecutor {

    private static final Logger LOGGER = LogManager.getLogger(SQLExecutor.class);

    private final Functions functions;
    public final Analyzer analyzer;
    public final Planner planner;
    private final RelationNormalizer relationNormalizer;
    private final RelationAnalyzer relAnalyzer;
    private final SessionContext sessionContext;
    private final CoordinatorTxnCtx coordinatorTxnCtx;
    private final Random random;
    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

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
            new RoutingProvider(random.nextInt(), emptyList()),
            UUID.randomUUID(),
            functions,
            new CoordinatorTxnCtx(sessionContext),
            -1
        );
    }

    public <T extends AnalyzedRelation> T normalize(AnalyzedRelation relation, CoordinatorTxnCtx txnCtx) {
        //noinspection unchecked
        return (T) relationNormalizer.normalize(relation, txnCtx);
    }

    public <T extends AnalyzedRelation> T normalize(String statement) {
        return normalize(
            analyze(statement),
            new CoordinatorTxnCtx(SessionContext.systemSessionContext())
        );
    }

    public static class Builder {

        private final ClusterService clusterService;
        private final Functions functions;
        private final AnalysisRegistry analysisRegistry;
        private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
        private final CreateBlobTableAnalyzer createBlobTableAnalyzer;
        private final AllocationService allocationService;
        private final Random random;
        private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
        private String[] searchPath = new String[]{Schemas.DOC_SCHEMA_NAME};
        private User user = User.CRATE_USER;
        private UserManager userManager = new StubUserManager();

        private TableStats tableStats = new TableStats();
        private boolean hasValidLicense = true;
        private Schemas schemas;

        private Builder(ClusterService clusterService, int numNodes, Random random) {
            Preconditions.checkArgument(numNodes >= 1, "Must have at least 1 node");
            this.random = random;
            this.clusterService = clusterService;
            addNodesToClusterState(numNodes);
            functions = getFunctions();
            UserDefinedFunctionService udfService = new UserDefinedFunctionService(clusterService, functions);
            Map<String, SchemaInfo> schemaInfoByName = new HashMap<>();
            schemaInfoByName.put("sys", new SysSchemaInfo(clusterService));
            schemaInfoByName.put("information_schema", new InformationSchemaInfo());
            schemaInfoByName.put(PgCatalogSchemaInfo.NAME, new PgCatalogSchemaInfo(udfService));
            IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
            schemaInfoByName.put(
                BlobSchemaInfo.NAME,
                new BlobSchemaInfo(
                    clusterService,
                    new TestingBlobTableInfoFactory(
                        Collections.emptyMap(), indexNameExpressionResolver, createTempDir())));


            Map<RelationName, DocTableInfo> docTables = new HashMap<>();
            DocTableInfoFactory tableInfoFactory = new TestingDocTableInfoFactory(
                docTables, functions, indexNameExpressionResolver);
            ViewInfoFactory testingViewInfoFactory = (ident, state) -> null;

            schemas = new Schemas(
                schemaInfoByName,
                clusterService,
                new DocSchemaInfoFactory(tableInfoFactory, testingViewInfoFactory, functions, udfService)
            );
            schemas.start();  // start listen to cluster state changes

            File homeDir = createTempDir();
            Environment environment = new Environment(
                Settings.builder().put(PATH_HOME_SETTING.getKey(), homeDir.getAbsolutePath()).build(),
                homeDir.toPath().resolve("config")
            );
            try {
                analysisRegistry = new AnalysisModule(environment, singletonList(new CommonAnalysisPlugin()))
                    .getAnalysisRegistry();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fulltextAnalyzerResolver = new FulltextAnalyzerResolver(clusterService, analysisRegistry);
            createTableStatementAnalyzer = new CreateTableStatementAnalyzer(functions);
            createBlobTableAnalyzer = new CreateBlobTableAnalyzer(
                schemas,
                functions
            );
            allocationService = new AllocationService(
                new AllocationDeciders(
                    Arrays.asList(
                        new SameShardAllocationDecider(Settings.EMPTY, clusterService.getClusterSettings()),
                        new ReplicaAfterPrimaryActiveAllocationDecider()
                    )
                ),
                new TestGatewayAllocator(),
                new BalancedShardsAllocator(Settings.EMPTY),
                EmptyClusterInfoService.INSTANCE
            );

            publishInitialClusterState();
        }

        private void addNodesToClusterState(int numNodes) {
            ClusterState prevState = clusterService.state();
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder(prevState.nodes());
            for (int i = 1; i <= numNodes; i++) {
                if (builder.get("n" + i) == null) {
                    builder.add(new DiscoveryNode("n" + i, newFakeAddress(), Version.CURRENT));
                }
            }
            builder.localNodeId("n1");
            builder.masterNodeId("n1");
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(prevState).nodes(builder).build());
        }

        /**
         * Publish the current {@link ClusterState} so new instances of
         * {@link org.elasticsearch.cluster.ClusterStateListener} like e.g. {@link Schemas} will consume it and
         * build current schema/table infos.
         *
         * The {@link MetaData#version()} must be increased to trigger a {@link ClusterChangedEvent#metaDataChanged()}.
         */
        private void publishInitialClusterState() {
            ClusterState currentState = clusterService.state();
            ClusterState newState = ClusterState.builder(currentState)
                .metaData(MetaData.builder(currentState.metaData())
                              .version(currentState.metaData().version() + 1)
                              .build())
                .build();
            ClusterServiceUtils.setState(clusterService, newState);
        }

        public Builder setSearchPath(String... schemas) {
            Objects.requireNonNull(schemas, "Search path must not be set to null");
            this.searchPath = schemas;
            return this;
        }

        public Builder setUser(User user) {
            this.user = user;
            return this;
        }

        public Builder setHasValidLicense(boolean hasValidLicense) {
            this.hasValidLicense = hasValidLicense;
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
        public Builder enableDefaultTables() throws IOException {
            // we should try to reduce the number of tables here eventually...
            addTable(USER_TABLE_DEFINITION);
            addTable(USER_TABLE_CLUSTERED_BY_ONLY_DEFINITION);
            addTable(USER_TABLE_MULTI_PK_DEFINITION);
            addTable(DEEPLY_NESTED_TABLE_DEFINITION);
            addTable(NESTED_PK_TABLE_DEFINITION);
            addPartitionedTable(TEST_PARTITIONED_TABLE_DEFINITION, TEST_PARTITIONED_TABLE_PARTITIONS);
            addTable(TEST_DOC_TRANSACTIONS_TABLE_DEFINITION);
            addTable(TEST_DOC_LOCATIONS_TABLE_DEFINITION);
            addTable(TEST_CLUSTER_BY_STRING_TABLE_DEFINITION);
            addTable(USER_TABLE_REFRESH_INTERVAL_BY_ONLY_DEFINITION);
            addTable(T3.T1_DEFINITION);
            addTable(T3.T2_DEFINITION);
            addTable(T3.T3_DEFINITION);

            RelationName multiPartName = new RelationName("doc", "multi_parted");
            addPartitionedTable(
                "create table doc.multi_parted (" +
                "   id int," +
                "   date timestamp with time zone," +
                "   num long," +
                "   obj object as (name string)" +
                ") partitioned by (date, obj['name'])",
                new PartitionName(multiPartName, Arrays.asList("1395874800000", "0")).toString(),
                new PartitionName(multiPartName, Arrays.asList("1395961200000", "-100")).toString(),
                new PartitionName(multiPartName, Arrays.asList(null, "-100")).toString()
            );
            return this;
        }

        public SQLExecutor build() {
            RelationAnalyzer relationAnalyzer = new RelationAnalyzer(functions, schemas);
            return new SQLExecutor(
                functions,
                new Analyzer(
                    schemas,
                    functions,
                    relationAnalyzer,
                    clusterService,
                    analysisRegistry,
                    new RepositoryService(
                        clusterService,
                        mock(TransportDeleteRepositoryAction.class),
                        mock(TransportPutRepositoryAction.class)
                    ),
                    userManager
                ),
                new Planner(
                    Settings.EMPTY,
                    clusterService,
                    functions,
                    tableStats,
                    null,
                    null,
                    schemas,
                    userManager,
                    () -> hasValidLicense
                ),
                relationAnalyzer,
                new SessionContext(Option.NONE, user, searchPath),
                schemas,
                random,
                fulltextAnalyzerResolver
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

        public Builder addPartitionedTable(String createTableStmt, String... partitions) throws IOException {
            CreateTable<Expression> stmt = (CreateTable<Expression>) SqlParser.createStatement(createTableStmt);
            CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(SessionContext.systemSessionContext());
            AnalyzedCreateTable analyzedCreateTable = createTableStatementAnalyzer.analyze(
                stmt, ParameterContext.EMPTY, txnCtx);

            CreateTableAnalyzedStatement analyzedStmt = CreateTablePlan.createStatement(
                analyzedCreateTable,
                txnCtx,
                functions,
                Row.EMPTY,
                SubQueryResults.EMPTY,
                new NumberOfShards(clusterService),
                schemas,
                fulltextAnalyzerResolver
            );
            if (!analyzedStmt.isPartitioned()) {
                throw new IllegalArgumentException("use addTable(..) to add non partitioned tables");
            }
            ClusterState prevState = clusterService.state();

            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().map(analyzedStmt.mapping());
            CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(mappingBuilder));
            Settings settings = analyzedStmt.tableParameter().settings();
            AliasMetaData.Builder alias = AliasMetaData.builder(analyzedStmt.tableIdent().indexNameOrAlias());
            IndexTemplateMetaData.Builder template = IndexTemplateMetaData.builder(analyzedStmt.templateName())
                .patterns(singletonList(analyzedStmt.templatePrefix()))
                .order(100)
                .putMapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
                .settings(settings)
                .putAlias(alias);

            MetaData.Builder mdBuilder = MetaData.builder(prevState.metaData())
                .put(template);

            RoutingTable.Builder routingBuilder = RoutingTable.builder(prevState.routingTable());
            for (String partition : partitions) {
                IndexMetaData indexMetaData= getIndexMetaData(
                    partition,
                    analyzedStmt.tableParameter().settings(),
                    analyzedStmt.mapping(),
                    prevState.nodes().getSmallestNonClientNodeVersion())
                    .putAlias(alias)
                    .build();
                mdBuilder.put(indexMetaData, true);
                routingBuilder.addAsNew(indexMetaData);
            }
            ClusterState newState = ClusterState.builder(prevState)
                .metaData(mdBuilder.build())
                .routingTable(routingBuilder.build())
                .build();

            ClusterServiceUtils.setState(clusterService, allocationService.reroute(newState, "assign shards"));
            return this;
        }

        /**
         * Add a table to the clusterState
         */
        public Builder addTable(String createTableStmt) throws IOException {
            CreateTable<Expression> stmt = (CreateTable<Expression>) SqlParser.createStatement(createTableStmt);
            CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(SessionContext.systemSessionContext());
            AnalyzedCreateTable analyzedCreateTable = createTableStatementAnalyzer.analyze(
                stmt, ParameterContext.EMPTY, txnCtx);
            CreateTableAnalyzedStatement analyzedStmt = CreateTablePlan.createStatement(
                analyzedCreateTable,
                txnCtx,
                functions,
                Row.EMPTY,
                SubQueryResults.EMPTY,
                new NumberOfShards(clusterService),
                schemas,
                fulltextAnalyzerResolver
            );

            if (analyzedStmt.isPartitioned()) {
                throw new IllegalArgumentException("use addPartitionedTable(..) to add partitioned tables");
            }
            ClusterState prevState = clusterService.state();
            RelationName relationName = analyzedStmt.tableIdent();
            IndexMetaData indexMetaData = getIndexMetaData(
                relationName.indexNameOrAlias(),
                analyzedStmt.tableParameter().settings(),
                analyzedStmt.mapping(),
                prevState.nodes().getSmallestNonClientNodeVersion()
            ).build();

            ClusterState state = ClusterState.builder(prevState)
                .metaData(MetaData.builder(prevState.metaData()).put(indexMetaData, true))
                .routingTable(RoutingTable.builder(prevState.routingTable()).addAsNew(indexMetaData).build())
                .build();

            ClusterServiceUtils.setState(clusterService, allocationService.reroute(state, "assign shards"));
            return this;
        }

        private static IndexMetaData.Builder getIndexMetaData(String indexName,
                                                              Settings settings,
                                                              @Nullable Map<String, Object> mapping,
                                                              Version smallestNodeVersion) throws IOException {
            Settings.Builder builder = Settings.builder()
                .put(settings)
                .put(SETTING_VERSION_CREATED, smallestNodeVersion)
                .put(SETTING_CREATION_DATE, Instant.now().toEpochMilli())
                .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

            Settings indexSettings = builder.build();
            IndexMetaData.Builder metaBuilder = IndexMetaData.builder(indexName)
                .settings(indexSettings);
            if (mapping != null) {
                metaBuilder.putMapping(new MappingMetaData(
                    Constants.DEFAULT_MAPPING_TYPE,
                    mapping));
            }

            return metaBuilder;
        }

        /**
         * Add a view definition to the metaData.
         * Note that this by-passes the analyzer step and directly operates on the clusterState. So there is no
         * resolve logic for columns (`*` is not resolved to the column names)
         */
        public Builder addView(RelationName name, String query) {
            ClusterState prevState = clusterService.state();
            ViewsMetaData newViews = ViewsMetaData.addOrReplace(
                prevState.metaData().custom(ViewsMetaData.TYPE), name, query, user == null ? null : user.name());

            MetaData newMetaData = MetaData.builder(prevState.metaData()).putCustom(ViewsMetaData.TYPE, newViews).build();
            ClusterState newState = ClusterState.builder(prevState)
                .metaData(newMetaData)
                .build();

            ClusterServiceUtils.setState(clusterService, newState);
            return this;
        }

        public Builder addBlobTable(String createBlobTableStmt) throws IOException {
            CreateBlobTable<Expression> stmt = (CreateBlobTable<Expression>) SqlParser.createStatement(createBlobTableStmt);
            CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(SessionContext.systemSessionContext());
            AnalyzedCreateBlobTable analyzedStmt = createBlobTableAnalyzer.analyze(
                stmt, ParameterContext.EMPTY, txnCtx);
            Settings settings = CreateBlobTablePlan.buildSettings(
                analyzedStmt.createBlobTable(),
                txnCtx,
                functions,
                Row.EMPTY,
                SubQueryResults.EMPTY,
                new NumberOfShards(clusterService));

            ClusterState prevState = clusterService.state();
            IndexMetaData indexMetaData = getIndexMetaData(
                fullIndexName(analyzedStmt.relationName().name()),
                settings,
                Collections.emptyMap(),
                prevState.nodes().getSmallestNonClientNodeVersion()
            ).build();

            ClusterState state = ClusterState.builder(prevState)
                .metaData(MetaData.builder(prevState.metaData()).put(indexMetaData, true))
                .routingTable(RoutingTable.builder(prevState.routingTable()).addAsNew(indexMetaData).build())
                .build();

            ClusterServiceUtils.setState(clusterService, allocationService.reroute(state, "assign shards"));
            return this;
        }

        public Builder setTableStats(TableStats tableStats) {
            this.tableStats = tableStats;
            return this;
        }

        public Builder setUserManager(UserManager userManager) {
            this.userManager = userManager;
            return this;
        }
    }

    public static Builder builder(ClusterService clusterService) {
        return new Builder(clusterService, 1, Randomness.get());
    }

    public static Builder builder(ClusterService clusterService, int numNodes, Random random) {
        return new Builder(clusterService, numNodes, random);
    }

    /**
     * This will build a cluster state containing the given table definition before returning a fully resolved
     * {@link DocTableInfo}.
     *
     * <p>Building a cluster state is a rather expensive operation thus this method should NOT be used when multiple
     * tables are needed (e.g. inside a loop) but instead building the cluster state once containing all tables
     * using {@link #builder(ClusterService)} and afterwards resolve all table infos manually.</p>
     */
    public static DocTableInfo tableInfo(RelationName name, String stmt, ClusterService clusterService) {
        try {
            SQLExecutor executor = builder(clusterService)
                .addTable(stmt)
                .build();
            return executor.schemas().getTableInfo(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Variant of {@link #tableInfo(RelationName, String, ClusterService)} for partitioned tables.
     */
    public static DocTableInfo partitionedTableInfo(RelationName name, String stmt, ClusterService clusterService) {
        try {
            SQLExecutor executor = builder(clusterService)
                .addPartitionedTable(stmt)
                .build();
            return executor.schemas().getTableInfo(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SQLExecutor(Functions functions,
                        Analyzer analyzer,
                        Planner planner,
                        RelationAnalyzer relAnalyzer,
                        SessionContext sessionContext,
                        Schemas schemas,
                        Random random,
                        FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        this.functions = functions;
        this.relationNormalizer = new RelationNormalizer(functions);
        this.analyzer = analyzer;
        this.planner = planner;
        this.relAnalyzer = relAnalyzer;
        this.sessionContext = sessionContext;
        this.coordinatorTxnCtx = new CoordinatorTxnCtx(sessionContext);
        this.schemas = schemas;
        this.random = random;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
    }

    public Functions functions() {
        return functions;
    }

    public FulltextAnalyzerResolver fulltextAnalyzerResolver() {
        return fulltextAnalyzerResolver;
    }

    private <T extends AnalyzedStatement> T analyzeInternal(String stmt, ParameterContext parameterContext) {
        Analysis analysis = analyzer.boundAnalyze(
            SqlParser.createStatement(stmt), coordinatorTxnCtx, parameterContext);
        //noinspection unchecked
        return (T) analysis.analyzedStatement();
    }

    private <T extends AnalyzedStatement> T analyze(String stmt, ParameterContext parameterContext) {
        return analyzeInternal(stmt, parameterContext);
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
     * If tables are used here they must also be registered in the SQLExecutor having used {@link Builder#addTable(String)}
     */
    public Symbol asSymbol(String expression) {
        ImmutableMap.Builder<QualifiedName, AnalyzedRelation> sources = ImmutableMap.builder();
        for (SchemaInfo schemaInfo : schemas) {
            for (TableInfo tableInfo : schemaInfo.getTables()) {
                if (tableInfo instanceof DocTableInfo) {
                    RelationName relationName = tableInfo.ident();
                    sources.put(
                        new QualifiedName(Arrays.asList(relationName.schema(), relationName.name())),
                        new DocTableRelation(schemas.getTableInfo(relationName)));
                }
            }
        }

        CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(sessionContext);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            coordinatorTxnCtx,
            ParamTypeHints.EMPTY,
            new FullQualifiedNameFieldProvider(sources.build(), ParentRelations.NO_PARENTS, sessionContext.searchPath().currentSchema()),
            new SubqueryAnalyzer(
                relAnalyzer,
                new StatementAnalysisContext(ParamTypeHints.EMPTY, Operation.READ, coordinatorTxnCtx)
            )
        );
        return expressionAnalyzer.convert(
            SqlParser.createExpression(expression), new ExpressionAnalysisContext());
    }

    public <T> T plan(String statement, UUID jobId, int fetchSize) {
        AnalyzedStatement analyzedStatement = analyze(statement, ParameterContext.EMPTY);
        return planInternal(analyzedStatement, jobId, fetchSize);
    }

    private <T> T planInternal(AnalyzedStatement analyzedStatement, UUID jobId, int fetchSize) {
        RoutingProvider routingProvider = new RoutingProvider(random.nextInt(), emptyList());
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            jobId,
            functions,
            coordinatorTxnCtx,
            fetchSize
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
                new SubQueryResults(emptyMap()) {
                    @Override
                    public Object getSafe(SelectSymbol key) {
                        return Literal.of(key.valueType(), null);
                    }
                }
            );
        }
        return (T) plan;
    }

    public <T extends LogicalPlan> T logicalPlan(String statement) {
        AnalyzedStatement stmt = analyze(statement, ParameterContext.EMPTY);
        if (stmt instanceof AnalyzedRelation) {
            // unboundAnalyze currently doesn't normalize; which breaks LogicalPlan building for joins
            // because the subRelations are not yet AnalyzedRelations.
            // we eventually should integrate normalize into unboundAnalyze.
            // But then we have to remove the whereClauseAnalyzer calls because they depend on all values being available
            stmt = new RelationNormalizer(functions)
                .normalize((AnalyzedRelation) stmt, coordinatorTxnCtx);
        }
        return (T) planner.plan(stmt, getPlannerContext(planner.currentClusterState()));
    }

    public <T> T plan(String statement) {
        return plan(statement, UUID.randomUUID(), 0);
    }

    public Schemas schemas() {
        return schemas;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public <T extends TableInfo> T resolveTableInfo(String tableName) {
        IndexParts indexParts = new IndexParts(tableName);
        QualifiedName qualifiedName = QualifiedName.of(indexParts.getSchema(), indexParts.getTable());
        return (T) schemas.resolveTableInfo(qualifiedName, Operation.READ, sessionContext.user(), sessionContext.searchPath());
    }
}
