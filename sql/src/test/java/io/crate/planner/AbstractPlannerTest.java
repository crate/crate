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

package io.crate.planner;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.Analyzer;
import io.crate.analyze.BaseAnalyzerTest;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.executor.transport.RepositoryService;
import io.crate.metadata.*;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.tablefunctions.TableFunctionModule;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESAllocationTestCase.createAllocationService;
import static org.elasticsearch.test.ESAllocationTestCase.newNode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractPlannerTest extends CrateUnitTest {

    protected static Routing shardRouting(String tableName) {
        return new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(tableName, Arrays.asList(1, 2)).map())
            .put("nodeTow", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(tableName, Arrays.asList(3, 4)).map())
            .map());
    }

    private static final Routing PARTED_ROUTING = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
        .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.parted.04232chj", Arrays.asList(1, 2)).map())
        .put("nodeTow", TreeMapBuilder.<String, List<Integer>>newMapBuilder().map())
        .map());

    private static final Routing CLUSTERED_PARTED_ROUTING = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
        .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.clustered_parted.04732cpp6ks3ed1o60o30c1g", Arrays.asList(1, 2)).map())
        .put("nodeTwo", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.clustered_parted.04732cpp6ksjcc9i60o30c1g", Arrays.asList(3)).map())
        .map());


    protected ClusterService clusterService;
    private ThreadPool threadPool;
    private Analyzer analyzer;
    private Planner planner;

    @Mock
    private SchemaInfo schemaInfo;

    @Before
    public void prepare() throws Exception {
        threadPool = TestingHelpers.newMockedThreadPool();
        Injector injector = new ModulesBuilder()
            .add(new AggregationImplModule())
            .add(new ScalarFunctionModule())
            .add(new TableFunctionModule())
            .add(new PredicateModule())
            .add(new OperatorModule())
            .add(new RepositorySettingsModule())
            .add(new SettingsModule(Settings.EMPTY))
            .add(new TestModule())
            .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);

        bindGeneratedColumnTable(injector.getInstance(Functions.class));
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    private void bindGeneratedColumnTable(Functions functions) {
        TableIdent generatedPartitionedTableIdent = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "parted_generated");
        TableInfo generatedPartitionedTableInfo = new TestingTableInfo.Builder(
            generatedPartitionedTableIdent, PARTED_ROUTING)
            .add("ts", DataTypes.TIMESTAMP, null)
            .addGeneratedColumn("day", DataTypes.TIMESTAMP, "date_trunc('day', ts)", true)
            .addPartitions(
                new PartitionName("parted_generated", Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
                new PartitionName("parted_generated", Arrays.asList(new BytesRef("1395961200000"))).asIndexName())
            .build(functions);
        when(schemaInfo.getTableInfo(generatedPartitionedTableIdent.name()))
            .thenReturn(generatedPartitionedTableInfo);
    }

    class TestModule extends MetaDataModule {

        @Override
        protected void configure() {
            bind(RepositoryService.class).toInstance(mock(RepositoryService.class));
            bind(TableStatsService.class).toInstance(mock(TableStatsService.class));
            bind(ThreadPool.class).toInstance(threadPool);
            bind(IndexNameExpressionResolver.class).toInstance(new IndexNameExpressionResolver(Settings.EMPTY));
            clusterService = new NoopClusterService();
            FulltextAnalyzerResolver fulltextAnalyzerResolver = mock(FulltextAnalyzerResolver.class);
            bind(FulltextAnalyzerResolver.class).toInstance(fulltextAnalyzerResolver);
            bind(ClusterService.class).toInstance(clusterService);
            bind(TransportPutIndexTemplateAction.class).toInstance(mock(TransportPutIndexTemplateAction.class));
            super.configure();
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            TableIdent userTableIdent = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "users");
            TableInfo userTableInfo = TestingTableInfo.builder(userTableIdent, shardRouting("users"))
                .add("name", DataTypes.STRING, null)
                .add("id", DataTypes.LONG, null)
                .add("date", DataTypes.TIMESTAMP, null)
                .add("text", DataTypes.STRING, null, ReferenceInfo.IndexType.ANALYZED)
                .add("no_index", DataTypes.STRING, null, ReferenceInfo.IndexType.NO)
                .addPrimaryKey("id")
                .clusteredBy("id")
                .build();
            TableIdent charactersTableIdent = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "characters");
            TableInfo charactersTableInfo = TestingTableInfo.builder(charactersTableIdent, shardRouting("characters"))
                .add("name", DataTypes.STRING, null)
                .add("id", DataTypes.STRING, null)
                .addPrimaryKey("id")
                .clusteredBy("id")
                .build();
            TableIdent partedTableIdent = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "parted");
            TableInfo partedTableInfo = TestingTableInfo.builder(partedTableIdent, PARTED_ROUTING)
                .add("name", DataTypes.STRING, null)
                .add("id", DataTypes.STRING, null)
                .add("date", DataTypes.TIMESTAMP, null, true)
                .addPartitions(
                    new PartitionName("parted", new ArrayList<BytesRef>() {{
                        add(null);
                    }}).asIndexName(), // TODO: invalid partition: null not valid as part of primary key
                    new PartitionName("parted", Arrays.asList(new BytesRef("0"))).asIndexName(),
                    new PartitionName("parted", Arrays.asList(new BytesRef("123"))).asIndexName()
                )
                .addPrimaryKey("id")
                .addPrimaryKey("date")
                .clusteredBy("id")
                .build();
            TableIdent emptyPartedTableIdent = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "empty_parted");
            TableInfo emptyPartedTableInfo = TestingTableInfo.builder(partedTableIdent, shardRouting("empty_parted"))
                .add("name", DataTypes.STRING, null)
                .add("id", DataTypes.STRING, null)
                .add("date", DataTypes.TIMESTAMP, null, true)
                .addPrimaryKey("id")
                .addPrimaryKey("date")
                .clusteredBy("id")
                .build();
            TableIdent multiplePartitionedTableIdent = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "multi_parted");
            TableInfo multiplePartitionedTableInfo = new TestingTableInfo.Builder(
                multiplePartitionedTableIdent, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
                .add("id", DataTypes.INTEGER, null)
                .add("date", DataTypes.TIMESTAMP, null, true)
                .add("num", DataTypes.LONG, null)
                .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
                .add("obj", DataTypes.STRING, Arrays.asList("name"), true)
                // add 3 partitions/simulate already done inserts
                .addPartitions(
                    new PartitionName("multi_parted", Arrays.asList(new BytesRef("1395874800000"), new BytesRef("0"))).asIndexName(),
                    new PartitionName("multi_parted", Arrays.asList(new BytesRef("1395961200000"), new BytesRef("-100"))).asIndexName(),
                    new PartitionName("multi_parted", Arrays.asList(null, new BytesRef("-100"))).asIndexName())
                .build();
            TableIdent clusteredByPartitionedIdent = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "clustered_parted");
            TableInfo clusteredByPartitionedTableInfo = new TestingTableInfo.Builder(
                multiplePartitionedTableIdent, CLUSTERED_PARTED_ROUTING)
                .add("id", DataTypes.INTEGER, null)
                .add("date", DataTypes.TIMESTAMP, null, true)
                .add("city", DataTypes.STRING, null)
                .clusteredBy("city")
                .addPartitions(
                    new PartitionName("clustered_parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
                    new PartitionName("clustered_parted", Arrays.asList(new BytesRef("1395961200000"))).asIndexName())
                .build();
            when(schemaInfo.getTableInfo(charactersTableIdent.name())).thenReturn(charactersTableInfo);
            when(schemaInfo.getTableInfo(userTableIdent.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(partedTableIdent.name())).thenReturn(partedTableInfo);
            when(schemaInfo.getTableInfo(emptyPartedTableIdent.name())).thenReturn(emptyPartedTableInfo);
            when(schemaInfo.getTableInfo(multiplePartitionedTableIdent.name())).thenReturn(multiplePartitionedTableInfo);
            when(schemaInfo.getTableInfo(clusteredByPartitionedIdent.name())).thenReturn(clusteredByPartitionedTableInfo);
            when(schemaInfo.getTableInfo(BaseAnalyzerTest.IGNORED_NESTED_TABLE_IDENT.name())).thenReturn(BaseAnalyzerTest.IGNORED_NESTED_TABLE_INFO);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
            schemaBinder.addBinding(SysSchemaInfo.NAME).toInstance(mockSysSchemaInfo());
            schemaBinder.addBinding(BlobSchemaInfo.NAME).toInstance(mockBlobSchemaInfo());
        }

        private SchemaInfo mockBlobSchemaInfo() {
            BlobSchemaInfo blobSchemaInfo = mock(BlobSchemaInfo.class);
            BlobTableInfo tableInfo = mock(BlobTableInfo.class);
            when(blobSchemaInfo.getTableInfo("screenshots")).thenReturn(tableInfo);
            return blobSchemaInfo;
        }

        private SchemaInfo mockSysSchemaInfo() {
            MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("parted").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
                .build();
            RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("parted")).build();
            ClusterState state = ClusterState
                .builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .build();

            state = ClusterState.builder(state).nodes(
                DiscoveryNodes.builder().put(newNode("nodeOne")).put(newNode("nodeTwo")).localNodeId("nodeOne")).build();

            AllocationService allocationService = createAllocationService();
            routingTable = allocationService.reroute(state, "test!").routingTable();
            state = ClusterState.builder(state).routingTable(routingTable).build();

            ClusterService clusterService = new NoopClusterService(state);
            return new SysSchemaInfo(clusterService);
        }
    }

    protected <T extends Plan> T plan(String statement) {
        //noinspection unchecked: for testing this is fine
        return (T) planner.plan(analyzer.analyze(SqlParser.createStatement(statement),
            new ParameterContext(new Object[0], new Object[0][], Schemas.DEFAULT_SCHEMA_NAME)), UUID.randomUUID());
    }

    protected Plan plan(String statement, Object[][] bulkArgs) {
        return planner.plan(analyzer.analyze(SqlParser.createStatement(statement),
            new ParameterContext(new Object[0], bulkArgs, Schemas.DEFAULT_SCHEMA_NAME)), UUID.randomUUID());
    }
}
