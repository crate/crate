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

package io.crate.testing;

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
import static io.crate.testing.TestingHelpers.createNodeContext;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_CLOSED_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
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
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;

import io.crate.Constants;
import io.crate.analyze.AnalyzedCreateBlobTable;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.CreateBlobTableAnalyzer;
import io.crate.analyze.CreateTableStatementAnalyzer;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.common.collections.MapBuilder;
import io.crate.data.Row;
import io.crate.execution.ddl.RepositoryService;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfoFactory;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateBlobTablePlan;
import io.crate.planner.node.ddl.CreateTablePlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.optimizer.LoadedRules;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.LogicalReplicationSettings;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;
import io.crate.statistics.TableStats;
import io.crate.user.StubUserManager;
import io.crate.user.User;
import io.crate.user.UserManager;

/**
 * Lightweight alternative to {@link SQLTransportExecutor}.
 *
 * Can be used for unit-tests tests which don't require the full execution-layer/nodes to be started.
 */
public class SQLExecutor {

    private static final Logger LOGGER = LogManager.getLogger(SQLExecutor.class);

    public final Analyzer analyzer;
    public final Planner planner;
    private final RelationAnalyzer relAnalyzer;
    private final CoordinatorSessionSettings sessionSettings;
    private final CoordinatorTxnCtx coordinatorTxnCtx;
    public final NodeContext nodeCtx;
    private final Random random;
    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final UserDefinedFunctionService udfService;

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
            new CoordinatorTxnCtx(sessionSettings),
            nodeCtx,
            -1,
            null
        );
    }

    public static class Builder {

        private final ClusterService clusterService;
        private final NodeContext nodeCtx;
        private final AnalysisRegistry analysisRegistry;
        private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
        private final CreateBlobTableAnalyzer createBlobTableAnalyzer;
        private final AllocationService allocationService;
        private final Random random;
        private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
        private final UserDefinedFunctionService udfService;
        private final LogicalReplicationService logicalReplicationService;
        private String[] searchPath = new String[]{Schemas.DOC_SCHEMA_NAME};
        private User user = User.CRATE_USER;
        private UserManager userManager = new StubUserManager();

        private TableStats tableStats = new TableStats();
        private Schemas schemas;
        private LoadedRules loadedRules = new LoadedRules();
        private SessionSettingRegistry sessionSettingRegistry = new SessionSettingRegistry(Set.of(loadedRules));

        private Builder(ClusterService clusterService,
                        int numNodes,
                        Random random,
                        List<AnalysisPlugin> analysisPlugins,
                        AbstractModule... additionalModules) {
            if (numNodes < 1) {
                throw new IllegalArgumentException("Must have at least 1 node");
            }
            this.random = random;
            this.clusterService = clusterService;
            addNodesToClusterState(numNodes);
            nodeCtx = createNodeContext(additionalModules);
            DocTableInfoFactory tableInfoFactory = new DocTableInfoFactory(nodeCtx);
            udfService = new UserDefinedFunctionService(clusterService, tableInfoFactory, nodeCtx);
            File homeDir = createTempDir();
            Environment environment = new Environment(
                Settings.builder().put(PATH_HOME_SETTING.getKey(), homeDir.getAbsolutePath()).build(),
                homeDir.toPath().resolve("config")
            );
            Map<String, SchemaInfo> schemaInfoByName = new HashMap<>();
            schemaInfoByName.put("sys", new SysSchemaInfo(clusterService));
            schemaInfoByName.put("information_schema", new InformationSchemaInfo());
            schemaInfoByName.put(PgCatalogSchemaInfo.NAME, new PgCatalogSchemaInfo(udfService, tableStats));
            schemaInfoByName.put(
                BlobSchemaInfo.NAME,
                new BlobSchemaInfo(
                    clusterService,
                    new BlobTableInfoFactory(clusterService.getSettings(), environment)));


            AtomicReference<RelationAnalyzer> relationAnalyzerRef = new AtomicReference<>(null);
            var viewInfoFactory = new ViewInfoFactory(() -> {
                var relationAnalyzer = relationAnalyzerRef.get();
                if (relationAnalyzer == null) {
                    relationAnalyzer = new RelationAnalyzer(nodeCtx, schemas);
                    relationAnalyzerRef.set(relationAnalyzer);
                }
                return relationAnalyzer;
            });
            schemas = new Schemas(
                schemaInfoByName,
                clusterService,
                new DocSchemaInfoFactory(tableInfoFactory, viewInfoFactory, nodeCtx, udfService)
            );
            schemas.start();  // start listen to cluster state changes
            try {
                analysisRegistry = new AnalysisModule(environment, analysisPlugins).getAnalysisRegistry();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fulltextAnalyzerResolver = new FulltextAnalyzerResolver(clusterService, analysisRegistry);
            createTableStatementAnalyzer = new CreateTableStatementAnalyzer(nodeCtx);
            createBlobTableAnalyzer = new CreateBlobTableAnalyzer(
                schemas,
                nodeCtx
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
            var threadPool = mock(ThreadPool.class);
            var logicalReplicationSettings = new LogicalReplicationSettings(Settings.EMPTY, clusterService);
            logicalReplicationService = new LogicalReplicationService(
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                clusterService,
                mock(RemoteClusters.class),
                threadPool,
                new NodeClient(Settings.EMPTY, threadPool),
                allocationService,
                logicalReplicationSettings
            );
            logicalReplicationService.repositoriesService(mock(RepositoriesService.class));

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
         * The {@link Metadata#version()} must be increased to trigger a {@link ClusterChangedEvent#metadataChanged()}.
         */
        private void publishInitialClusterState() {
            ClusterState currentState = clusterService.state();
            ClusterState newState = ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentState.metadata())
                              .version(currentState.metadata().version() + 1)
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

        /**
         * Adds a couple of tables which are defined in {@link T3} and {@link io.crate.analyze.TableDefinitions}.
         * <p>
         * Note that these tables do have a static routing which doesn't necessarily match the nodes that
         * are part of the clusterState of the {@code clusterService} provided to the builder.
         *
         * Note that these tables are not part of the clusterState and rely on a stubbed getRouting
         * Using {@link #addTable(String)} is preferred for this reason.
         * </p>
         *
         * @deprecated Please only add the tables used by a test scenario
         */
        @Deprecated
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
            RelationAnalyzer relationAnalyzer = new RelationAnalyzer(nodeCtx, schemas);
            return new SQLExecutor(
                nodeCtx,
                new Analyzer(
                    schemas,
                    nodeCtx,
                    relationAnalyzer,
                    clusterService,
                    analysisRegistry,
                    new RepositoryService(
                        clusterService,
                        mock(TransportDeleteRepositoryAction.class),
                        mock(TransportPutRepositoryAction.class)
                    ),
                    userManager,
                    sessionSettingRegistry,
                    logicalReplicationService
                ),
                new Planner(
                    Settings.EMPTY,
                    clusterService,
                    nodeCtx,
                    tableStats,
                    null,
                    null,
                    schemas,
                    userManager,
                    sessionSettingRegistry
                ),
                relationAnalyzer,
                new CoordinatorSessionSettings(user, searchPath),
                schemas,
                random,
                fulltextAnalyzerResolver,
                udfService
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
            return addPartitionedTable(createTableStmt, Settings.EMPTY, partitions);
        }

        public Builder addPartitionedTable(String createTableStmt, Settings customSettings, String... partitions) throws IOException {
            CreateTable<Expression> stmt = (CreateTable<Expression>) SqlParser.createStatement(createTableStmt);
            CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
            AnalyzedCreateTable analyzedCreateTable = createTableStatementAnalyzer.analyze(
                stmt, ParamTypeHints.EMPTY, txnCtx);

            BoundCreateTable analyzedStmt = CreateTablePlan.bind(
                analyzedCreateTable,
                txnCtx,
                nodeCtx,
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
            var combinedSettings = Settings.builder()
                .put(analyzedStmt.tableParameter().settings())
                .put(customSettings)
                .build();

            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().map(analyzedStmt.mapping());
            CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(mappingBuilder));
            AliasMetadata.Builder alias = AliasMetadata.builder(analyzedStmt.tableIdent().indexNameOrAlias());
            IndexTemplateMetadata.Builder template = IndexTemplateMetadata.builder(analyzedStmt.templateName())
                .patterns(singletonList(analyzedStmt.templatePrefix()))
                .order(100)
                .putMapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
                .settings(buildSettings(false, combinedSettings, prevState.nodes().getSmallestNonClientNodeVersion()))
                .putAlias(alias);

            Metadata.Builder mdBuilder = Metadata.builder(prevState.metadata())
                .put(template);

            RoutingTable.Builder routingBuilder = RoutingTable.builder(prevState.routingTable());
            for (String partition : partitions) {
                IndexMetadata indexMetadata = getIndexMetadata(
                    partition,
                    combinedSettings,
                    analyzedStmt.mapping(),
                    prevState.nodes().getSmallestNonClientNodeVersion())
                    .putAlias(alias)
                    .build();
                mdBuilder.put(indexMetadata, true);
                routingBuilder.addAsNew(indexMetadata);
            }
            ClusterState newState = ClusterState.builder(prevState)
                .metadata(mdBuilder.build())
                .routingTable(routingBuilder.build())
                .build();

            ClusterServiceUtils.setState(clusterService, allocationService.reroute(newState, "assign shards"));
            return this;
        }

        public Builder addTable(String createTableStmt) throws IOException {
            return addTable(createTableStmt, Settings.EMPTY);
        }

        /**
         * Add a table to the clusterState
         */
        public Builder addTable(String createTableStmt, Settings settings) throws IOException {
            CreateTable<Expression> stmt = (CreateTable<Expression>) SqlParser.createStatement(createTableStmt);
            CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
            AnalyzedCreateTable analyzedCreateTable = createTableStatementAnalyzer.analyze(
                stmt, ParamTypeHints.EMPTY, txnCtx);
            BoundCreateTable analyzedStmt = CreateTablePlan.bind(
                analyzedCreateTable,
                txnCtx,
                nodeCtx,
                Row.EMPTY,
                SubQueryResults.EMPTY,
                new NumberOfShards(clusterService),
                schemas,
                fulltextAnalyzerResolver
            );

            if (analyzedStmt.isPartitioned()) {
                throw new IllegalArgumentException("use addPartitionedTable(..) to add partitioned tables");
            }

            var combinedSettings = Settings.builder()
                .put(analyzedStmt.tableParameter().settings())
                .put(settings)
                .build();

            ClusterState prevState = clusterService.state();
            RelationName relationName = analyzedStmt.tableIdent();
            IndexMetadata indexMetadata = getIndexMetadata(
                relationName.indexNameOrAlias(),
                combinedSettings,
                analyzedStmt.mapping(),
                prevState.nodes().getSmallestNonClientNodeVersion()
            ).build();

            ClusterState state = ClusterState.builder(prevState)
                .metadata(Metadata.builder(prevState.metadata()).put(indexMetadata, true))
                .routingTable(RoutingTable.builder(prevState.routingTable()).addAsNew(indexMetadata).build())
                .build();

            ClusterServiceUtils.setState(clusterService, allocationService.reroute(state, "assign shards"));
            return this;
        }

        public Builder closeTable(String tableName) throws IOException {
            String indexName = RelationName.of(QualifiedName.of(tableName), Schemas.DOC_SCHEMA_NAME).indexNameOrAlias();
            ClusterState prevState = clusterService.state();
            var metadata = prevState.metadata();
            String[] concreteIndices = IndexNameExpressionResolver
                .concreteIndexNames(metadata, IndicesOptions.lenientExpandOpen(), indexName);

            Metadata.Builder mdBuilder = Metadata.builder(clusterService.state().metadata());
            ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                .blocks(clusterService.state().blocks());

            for (String index: concreteIndices) {
                mdBuilder.put(IndexMetadata.builder(metadata.index(index)).state(IndexMetadata.State.CLOSE));
                blocksBuilder.addIndexBlock(index, INDEX_CLOSED_BLOCK);
            }

            ClusterState updatedState = ClusterState.builder(prevState).metadata(mdBuilder).blocks(blocksBuilder).build();

            ClusterServiceUtils.setState(clusterService, allocationService.reroute(updatedState, "assign shards"));
            return this;
        }

        private static IndexMetadata.Builder getIndexMetadata(String indexName,
                                                              Settings settings,
                                                              @Nullable Map<String, Object> mapping,
                                                              Version smallestNodeVersion) throws IOException {
            Settings indexSettings = buildSettings(true, settings, smallestNodeVersion);
            IndexMetadata.Builder metaBuilder = IndexMetadata.builder(indexName)
                .settings(indexSettings);
            if (mapping != null) {
                metaBuilder.putMapping(new MappingMetadata(
                    Constants.DEFAULT_MAPPING_TYPE,
                    mapping));
            }

            return metaBuilder;
        }

        private static Settings buildSettings(boolean isIndex, Settings settings, Version smallestNodeVersion) {
            Settings.Builder builder = Settings.builder()
                .put(settings);
            if (settings.get(SETTING_VERSION_CREATED) == null) {
                builder.put(SETTING_VERSION_CREATED, smallestNodeVersion);
            }
            if (settings.get(SETTING_CREATION_DATE) == null) {
                builder.put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());
            }
            if (isIndex && settings.get(SETTING_INDEX_UUID) == null) {
                builder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
            }

            return builder.build();
        }

        /**
         * Add a view definition to the metadata.
         * Note that this by-passes the analyzer step and directly operates on the clusterState. So there is no
         * resolve logic for columns (`*` is not resolved to the column names)
         */
        public Builder addView(RelationName name, String query) {
            ClusterState prevState = clusterService.state();
            ViewsMetadata newViews = ViewsMetadata.addOrReplace(
                prevState.metadata().custom(ViewsMetadata.TYPE), name, query, user == null ? null : user.name());

            Metadata newMetadata = Metadata.builder(prevState.metadata()).putCustom(ViewsMetadata.TYPE, newViews).build();
            ClusterState newState = ClusterState.builder(prevState)
                .metadata(newMetadata)
                .build();

            ClusterServiceUtils.setState(clusterService, newState);
            return this;
        }

        public Builder addPublication(String name, boolean forAllTables, RelationName... tables) {
            ClusterState prevState = clusterService.state();
            var publication = new Publication(user.name(), forAllTables, Arrays.asList(tables));
            var publicationsMetadata = new PublicationsMetadata(Map.of(name, publication));

            Metadata newMetadata = Metadata.builder(prevState.metadata())
                .putCustom(PublicationsMetadata.TYPE, publicationsMetadata)
                .build();
            ClusterState newState = ClusterState.builder(prevState)
                .metadata(newMetadata)
                .build();

            ClusterServiceUtils.setState(clusterService, newState);
            return this;
        }

        public Builder addSubscription(String name, String publication) {
            ClusterState prevState = clusterService.state();
            var subscription = new Subscription(
                user.name(),
                ConnectionInfo.fromURL("crate://localhost"),
                List.of(publication),
                Settings.EMPTY,
                Collections.emptyMap()
            );
            var subsMetadata = new SubscriptionsMetadata(Map.of(name, subscription));

            Metadata newMetadata = Metadata.builder(prevState.metadata())
                .putCustom(SubscriptionsMetadata.TYPE, subsMetadata)
                .build();
            ClusterState newState = ClusterState.builder(prevState)
                .metadata(newMetadata)
                .build();

            ClusterServiceUtils.setState(clusterService, newState);
            return this;
        }

        public Builder addBlobTable(String createBlobTableStmt) throws IOException {
            CreateBlobTable<Expression> stmt = (CreateBlobTable<Expression>) SqlParser.createStatement(createBlobTableStmt);
            CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
            AnalyzedCreateBlobTable analyzedStmt = createBlobTableAnalyzer.analyze(
                stmt, ParamTypeHints.EMPTY, txnCtx);
            Settings settings = CreateBlobTablePlan.buildSettings(
                analyzedStmt.createBlobTable(),
                txnCtx,
                nodeCtx,
                Row.EMPTY,
                SubQueryResults.EMPTY,
                new NumberOfShards(clusterService));

            ClusterState prevState = clusterService.state();
            IndexMetadata indexMetadata = getIndexMetadata(
                fullIndexName(analyzedStmt.relationName().name()),
                settings,
                Collections.emptyMap(),
                prevState.nodes().getSmallestNonClientNodeVersion()
            ).build();

            ClusterState state = ClusterState.builder(prevState)
                .metadata(Metadata.builder(prevState.metadata()).put(indexMetadata, true))
                .routingTable(RoutingTable.builder(prevState.routingTable()).addAsNew(indexMetadata).build())
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

        public Builder addUDFLanguage(UDFLanguage language) {
            udfService.registerLanguage(language);
            return this;
        }

        public Builder addUDF(UserDefinedFunctionMetadata udf) {
            udfService.updateImplementations(udf.schema(), Stream.of(udf));
            return this;
        }
    }

    public static Builder builder(ClusterService clusterService, AbstractModule... additionalModules) {
        return new Builder(clusterService, 1, Randomness.get(), List.of(), additionalModules);
    }

    public static Builder builder(ClusterService clusterService,
                                  int numNodes,
                                  Random random,
                                  List<AnalysisPlugin> analysisPlugins,
                                  AbstractModule... additionalModules) {
        return new Builder(clusterService, numNodes, random, analysisPlugins, additionalModules);
    }

    /**
     * This will build a cluster state containing the given table definition before returning a fully resolved
     * {@link DocTableInfo}.
     *
     * <p>Building a cluster state is a rather expensive operation thus this method should NOT be used when multiple
     * tables are needed (e.g. inside a loop) but instead building the cluster state once containing all tables
     * using {@link #builder(ClusterService, AbstractModule...)} and afterwards resolve all table infos manually.</p>
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

    private SQLExecutor(NodeContext nodeCtx,
                        Analyzer analyzer,
                        Planner planner,
                        RelationAnalyzer relAnalyzer,
                        CoordinatorSessionSettings sessionSettings,
                        Schemas schemas,
                        Random random,
                        FulltextAnalyzerResolver fulltextAnalyzerResolver,
                        UserDefinedFunctionService udfService) {
        this.analyzer = analyzer;
        this.planner = planner;
        this.relAnalyzer = relAnalyzer;
        this.sessionSettings = sessionSettings;
        this.coordinatorTxnCtx = new CoordinatorTxnCtx(sessionSettings);
        this.nodeCtx = nodeCtx;
        this.schemas = schemas;
        this.random = random;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.udfService = udfService;
    }

    public FulltextAnalyzerResolver fulltextAnalyzerResolver() {
        return fulltextAnalyzerResolver;
    }

    public UserDefinedFunctionService udfService() {
        return udfService;
    }

    public <T extends AnalyzedStatement> T analyze(String stmt, ParamTypeHints typeHints) {
        //noinspection unchecked
        return (T) analyzer.analyze(
            SqlParser.createStatement(stmt),
            coordinatorTxnCtx.sessionSettings(),
            typeHints);
    }

    public <T extends AnalyzedStatement> T analyze(String statement) {
        return analyze(statement, ParamTypeHints.EMPTY);
    }

    /**
     * Convert a expression to a symbol
     * If tables are used here they must also be registered in the SQLExecutor having used {@link Builder#addTable(String)}
     */
    public Symbol asSymbol(String expression) {
        MapBuilder<RelationName, AnalyzedRelation> sources = MapBuilder.newMapBuilder();
        for (SchemaInfo schemaInfo : schemas) {
            for (TableInfo tableInfo : schemaInfo.getTables()) {
                if (tableInfo instanceof DocTableInfo) {
                    RelationName relationName = tableInfo.ident();
                    sources.put(relationName, new DocTableRelation(schemas.getTableInfo(relationName)));
                }
            }
        }
        CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(sessionSettings);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            coordinatorTxnCtx,
            nodeCtx,
            ParamTypeHints.EMPTY,
            new FullQualifiedNameFieldProvider(
                sources.immutableMap(),
                ParentRelations.NO_PARENTS,
                sessionSettings.searchPath().currentSchema()
            ),
            new SubqueryAnalyzer(
                relAnalyzer,
                new StatementAnalysisContext(ParamTypeHints.EMPTY, Operation.READ, coordinatorTxnCtx)
            )
        );
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext(coordinatorTxnCtx.sessionSettings());
        return expressionAnalyzer.convert(
            SqlParser.createExpression(expression), expressionAnalysisContext);
    }

    public <T> T plan(String statement, UUID jobId, int fetchSize) {
        return plan(statement, jobId, fetchSize, Row.EMPTY);
    }

    public <T> T plan(String statement, UUID jobId, int fetchSize, Row params) {
        AnalyzedStatement analyzedStatement = analyze(statement, ParamTypeHints.EMPTY);
        return planInternal(analyzedStatement, jobId, fetchSize, params);
    }

    private <T> T planInternal(AnalyzedStatement analyzedStatement, UUID jobId, int fetchSize, Row params) {
        RoutingProvider routingProvider = new RoutingProvider(random.nextInt(), emptyList());
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            jobId,
            coordinatorTxnCtx,
            nodeCtx,
            fetchSize,
            null
        );
        Plan plan = planner.plan(analyzedStatement, plannerContext);
        if (plan instanceof LogicalPlan) {
            return (T) ((LogicalPlan) plan).build(
                mock(DependencyCarrier.class),
                plannerContext,
                Set.of(),
                new ProjectionBuilder(nodeCtx),
                TopN.NO_LIMIT,
                0,
                null,
                null,
                params,
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
        AnalyzedStatement stmt = analyze(statement, ParamTypeHints.EMPTY);
        return (T) planner.plan(stmt, getPlannerContext(planner.currentClusterState()));
    }

    public <T> T plan(String statement) {
        return plan(statement, UUID.randomUUID(), 0);
    }

    public Schemas schemas() {
        return schemas;
    }

    public CoordinatorSessionSettings getSessionSettings() {
        return sessionSettings;
    }

    public <T extends TableInfo> T resolveTableInfo(String tableName) {
        IndexParts indexParts = new IndexParts(tableName);
        QualifiedName qualifiedName = QualifiedName.of(indexParts.getSchema(), indexParts.getTable());
        return (T) schemas.resolveTableInfo(qualifiedName, Operation.READ, sessionSettings.sessionUser(), sessionSettings.searchPath());
    }
}
