package io.crate.planner;

import com.google.common.collect.Iterables;
import io.crate.Constants;
import io.crate.analyze.Analyzer;
import io.crate.analyze.BaseAnalyzerTest;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.*;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.projectors.FetchProjector;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.ddl.DropTableNode;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsNode;
import io.crate.planner.node.dml.*;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlannerTest extends CrateUnitTest {

    private Analyzer analyzer;
    private Planner planner;
    Routing shardRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(1, 2)).map())
            .put("nodeTow", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(3, 4)).map())
            .map());

    Routing nodesRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().map())
            .put("nodeTow", TreeMapBuilder.<String, List<Integer>>newMapBuilder().map())
            .map());

    final Routing partedRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.parted.04232chj", Arrays.asList(1, 2)).map())
            .put("nodeTow", TreeMapBuilder.<String, List<Integer>>newMapBuilder().map())
            .map());

    final Routing clusteredPartedRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.clustered_parted.04732cpp6ks3ed1o60o30c1g",  Arrays.asList(1, 2)).map())
            .put("nodeTwo", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.clustered_parted.04732cpp6ksjcc9i60o30c1g",  Arrays.asList(3)).map())
            .map());

    private ClusterService clusterService;

    private final static String LOCAL_NODE_ID = "foo";
    private ThreadPool threadPool;


    @Before
    public void prepare() throws Exception {
        threadPool = TestingHelpers.newMockedThreadPool();
        Injector injector = new ModulesBuilder()
                .add(new TestModule())
                .add(new AggregationImplModule())
                .add(new ScalarFunctionModule())
                .add(new PredicateModule())
                .add(new OperatorModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }


    class TestModule extends MetaDataModule {

        @Override
        protected void configure() {
            bind(ThreadPool.class).toInstance(threadPool);
            clusterService = mock(ClusterService.class);
            DiscoveryNode localNode = mock(DiscoveryNode.class);
            when(localNode.id()).thenReturn(LOCAL_NODE_ID);
            when(clusterService.localNode()).thenReturn(localNode);
            ClusterState clusterState = mock(ClusterState.class);
            MetaData metaData = mock(MetaData.class);
            when(metaData.concreteAllOpenIndices()).thenReturn(new String[0]);
            when(metaData.getTemplates()).thenReturn(ImmutableOpenMap.<String, IndexTemplateMetaData>of());
            when(metaData.templates()).thenReturn(ImmutableOpenMap.<String, IndexTemplateMetaData>of());
            when(clusterState.metaData()).thenReturn(metaData);
            DiscoveryNodes nodes = mock(DiscoveryNodes.class);
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(clusterService.state()).thenReturn(clusterState);
            when(clusterState.nodes()).thenReturn(nodes);
            ImmutableOpenMap<String, DiscoveryNode> dataNodes =
                    ImmutableOpenMap.<String, DiscoveryNode>builder().fPut("foo", node).build();
            when(nodes.dataNodes()).thenReturn(dataNodes);
            when(nodes.localNodeId()).thenReturn(LOCAL_NODE_ID);
            FulltextAnalyzerResolver fulltextAnalyzerResolver = mock(FulltextAnalyzerResolver.class);
            bind(FulltextAnalyzerResolver.class).toInstance(fulltextAnalyzerResolver);
            bind(ClusterService.class).toInstance(clusterService);
            bind(TransportPutIndexTemplateAction.class).toInstance(mock(TransportPutIndexTemplateAction.class));
            super.configure();
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            TableIdent userTableIdent = new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "users");
            TableInfo userTableInfo = TestingTableInfo.builder(userTableIdent, RowGranularity.DOC, shardRouting)
                    .add("name", DataTypes.STRING, null)
                    .add("id", DataTypes.LONG, null)
                    .add("date", DataTypes.TIMESTAMP, null)
                    .add("text", DataTypes.STRING, null, ReferenceInfo.IndexType.ANALYZED)
                    .add("no_index", DataTypes.STRING, null, ReferenceInfo.IndexType.NO)
                    .addPrimaryKey("id")
                    .clusteredBy("id")
                    .build();
            when(userTableInfo.schemaInfo().name()).thenReturn(ReferenceInfos.DEFAULT_SCHEMA_NAME);
            TableIdent charactersTableIdent = new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "characters");
            TableInfo charactersTableInfo = TestingTableInfo.builder(charactersTableIdent, RowGranularity.DOC, shardRouting)
                    .add("name", DataTypes.STRING, null)
                    .add("id", DataTypes.STRING, null)
                    .addPrimaryKey("id")
                    .clusteredBy("id")
                    .build();
            when(charactersTableInfo.schemaInfo().name()).thenReturn(ReferenceInfos.DEFAULT_SCHEMA_NAME);
            TableIdent partedTableIdent = new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "parted");
            TableInfo partedTableInfo = TestingTableInfo.builder(partedTableIdent, RowGranularity.DOC, partedRouting)
                    .add("name", DataTypes.STRING, null)
                    .add("id", DataTypes.STRING, null)
                    .add("date", DataTypes.TIMESTAMP, null, true)
                    .addPartitions(
                            new PartitionName("parted", new ArrayList<BytesRef>(){{add(null);}}).stringValue(), // TODO: invalid partition: null not valid as part of primary key
                            new PartitionName("parted", Arrays.asList(new BytesRef("0"))).stringValue(),
                            new PartitionName("parted", Arrays.asList(new BytesRef("123"))).stringValue()
                    )
                    .addPrimaryKey("id")
                    .addPrimaryKey("date")
                    .clusteredBy("id")
                    .build();
            when(partedTableInfo.schemaInfo().name()).thenReturn(ReferenceInfos.DEFAULT_SCHEMA_NAME);
            TableIdent emptyPartedTableIdent = new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "empty_parted");
            TableInfo emptyPartedTableInfo = TestingTableInfo.builder(partedTableIdent, RowGranularity.DOC, shardRouting)
                    .add("name", DataTypes.STRING, null)
                    .add("id", DataTypes.STRING, null)
                    .add("date", DataTypes.TIMESTAMP, null, true)
                    .addPrimaryKey("id")
                    .addPrimaryKey("date")
                    .clusteredBy("id")
                    .build();
            TableIdent multiplePartitionedTableIdent= new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "multi_parted");
            TableInfo multiplePartitionedTableInfo = new TestingTableInfo.Builder(
                    multiplePartitionedTableIdent, RowGranularity.DOC, new Routing())
                    .add("id", DataTypes.INTEGER, null)
                    .add("date", DataTypes.TIMESTAMP, null, true)
                    .add("num", DataTypes.LONG, null)
                    .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
                    .add("obj", DataTypes.STRING, Arrays.asList("name"), true)
                            // add 3 partitions/simulate already done inserts
                    .addPartitions(
                            new PartitionName("multi_parted", Arrays.asList(new BytesRef("1395874800000"), new BytesRef("0"))).stringValue(),
                            new PartitionName("multi_parted", Arrays.asList(new BytesRef("1395961200000"), new BytesRef("-100"))).stringValue(),
                            new PartitionName("multi_parted", Arrays.asList(null, new BytesRef("-100"))).stringValue())
                    .build();
            TableIdent clusteredByParitionedIdent = new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "clustered_parted");
            TableInfo clusteredByPartitionedTableInfo = new TestingTableInfo.Builder(
                    multiplePartitionedTableIdent, RowGranularity.DOC, clusteredPartedRouting)
                    .add("id", DataTypes.INTEGER, null)
                    .add("date", DataTypes.TIMESTAMP, null, true)
                    .add("city", DataTypes.STRING, null)
                    .clusteredBy("city")
                    .addPartitions(
                            new PartitionName("clustered_parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                            new PartitionName("clustered_parted", Arrays.asList(new BytesRef("1395961200000"))).stringValue())
                    .build();
            when(emptyPartedTableInfo.schemaInfo().name()).thenReturn(ReferenceInfos.DEFAULT_SCHEMA_NAME);
            when(schemaInfo.getTableInfo(charactersTableIdent.name())).thenReturn(charactersTableInfo);
            when(schemaInfo.getTableInfo(userTableIdent.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(partedTableIdent.name())).thenReturn(partedTableInfo);
            when(schemaInfo.getTableInfo(emptyPartedTableIdent.name())).thenReturn(emptyPartedTableInfo);
            when(schemaInfo.getTableInfo(multiplePartitionedTableIdent.name())).thenReturn(multiplePartitionedTableInfo);
            when(schemaInfo.getTableInfo(clusteredByParitionedIdent.name())).thenReturn(clusteredByPartitionedTableInfo);
            when(schemaInfo.getTableInfo(BaseAnalyzerTest.IGNORED_NESTED_TABLE_IDENT.name())).thenReturn(BaseAnalyzerTest.IGNORED_NESTED_TABLE_INFO);
            schemaBinder.addBinding(ReferenceInfos.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
            schemaBinder.addBinding(SysSchemaInfo.NAME).toInstance(mockSysSchemaInfo());
            schemaBinder.addBinding(BlobSchemaInfo.NAME).toInstance(mockBlobSchemaInfo());
        }

        private SchemaInfo mockBlobSchemaInfo(){
            BlobSchemaInfo blobSchemaInfo = mock(BlobSchemaInfo.class);
            BlobTableInfo tableInfo = mock(BlobTableInfo.class);
            when(blobSchemaInfo.getTableInfo("screenshots")).thenReturn(tableInfo);
            when(tableInfo.schemaInfo()).thenReturn(blobSchemaInfo);
            return blobSchemaInfo;
        }

        private SchemaInfo mockSysSchemaInfo() {
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.name()).thenReturn(SysSchemaInfo.NAME);
            when(schemaInfo.systemSchema()).thenReturn(true);

            TableInfo sysClusterTableInfo = TestingTableInfo.builder(
                    SysClusterTableInfo.IDENT,
                    // granularity < DOC is already handled different
                    // here we want a table with handlerSideRouting and DOC granularity.
                    RowGranularity.DOC,
                    SysClusterTableInfo.ROUTING
            ).schemaInfo(schemaInfo).add("name", DataTypes.STRING, null).schemaInfo(schemaInfo).build();
            when(schemaInfo.getTableInfo(sysClusterTableInfo.ident().name())).thenReturn(sysClusterTableInfo);

            TableInfo sysNodesTableInfo = TestingTableInfo.builder(
                    SysNodesTableInfo.IDENT,
                    RowGranularity.NODE,
                    nodesRouting)
                    .schemaInfo(schemaInfo)
                    .add("name", DataTypes.STRING, null).schemaInfo(schemaInfo).build();

            when(schemaInfo.getTableInfo(sysNodesTableInfo.ident().name())).thenReturn(sysNodesTableInfo);

            TableInfo sysShardsTableInfo = TestingTableInfo.builder(
                    SysShardsTableInfo.IDENT,
                    RowGranularity.SHARD,
                    nodesRouting
            ).add("id", DataTypes.INTEGER, null)
             .add("table_name", DataTypes.STRING, null)
             .schemaInfo(schemaInfo).build();
            when(schemaInfo.getTableInfo(sysShardsTableInfo.ident().name())).thenReturn(sysShardsTableInfo);
            when(schemaInfo.systemSchema()).thenReturn(true);
            return schemaInfo;
        }
    }

    private Plan plan(String statement) {
        return planner.plan(analyzer.analyze(SqlParser.createStatement(statement),
                new ParameterContext(new Object[0], new Object[0][], ReferenceInfos.DEFAULT_SCHEMA_NAME)));
    }

    @Test
    public void testGroupByWithAggregationStringLiteralArguments() {
        CollectNode collectNode = ((DistributedGroupBy) plan("select count('foo'), name from users group by name")).collectNode();
        // TODO: optimize to not collect literal
        //assertThat(collectNode.toCollect().size(), is(1));
        GroupProjection groupProjection = (GroupProjection) collectNode.projections().get(0);
        Aggregation aggregation = groupProjection.values().get(0);
    }

    @Test
    public void testGroupByWithAggregationPlan() throws Exception {
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) plan(
                "select count(*), name from users group by name");

        // distributed collect
        CollectNode collectNode = distributedGroupBy.collectNode();
        assertThat(collectNode.hasDistributingDownstreams(), is(true));
        assertThat(collectNode.downstreamNodes().size(), is(2));
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectNode.executionNodes().size(), is(2));
        assertThat(collectNode.toCollect().size(), is(1));
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, collectNode.outputTypes().get(0));
        assertEquals(DataTypes.UNDEFINED, collectNode.outputTypes().get(1));

        MergeNode mergeNode = distributedGroupBy.reducerMergeNode();

        assertThat(mergeNode.numUpstreams(), is(2));
        assertThat(mergeNode.executionNodes().size(), is(2));
        assertEquals(mergeNode.inputTypes(), collectNode.outputTypes());
        assertThat(mergeNode.projections().size(), is(2)); // for the default limit there is always a TopNProjection
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));

        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) mergeNode.projections().get(0);
        InputColumn inputColumn = (InputColumn) groupProjection.values().get(0).inputs().get(0);
        assertThat(inputColumn.index(), is(1));

        assertThat(mergeNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.LONG, mergeNode.outputTypes().get(0));
        assertEquals(DataTypes.STRING, mergeNode.outputTypes().get(1));

        MergeNode localMerge = distributedGroupBy.localMergeNode();

        assertThat(localMerge.numUpstreams(), is(2));
        assertThat(localMerge.executionNodes().size(), is(1));
        assertThat(Iterables.getOnlyElement(localMerge.executionNodes()), is(LOCAL_NODE_ID));
        assertEquals(mergeNode.outputTypes(), localMerge.inputTypes());

        assertThat(localMerge.projections().get(0), instanceOf(TopNProjection.class));
        TopNProjection topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(topN.outputs().size(), is(2));

        assertEquals(DataTypes.LONG, localMerge.outputTypes().get(0));
        assertEquals(DataTypes.STRING, localMerge.outputTypes().get(1));

    }

    @Test
    public void testGetPlan() throws Exception {
        IterablePlan plan = (IterablePlan)  plan("select name from users where id = 1");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode) iterator.next();
        assertThat(node.tableInfo().ident().name(), is("users"));
        assertThat(node.docKeys().getOnlyKey(), isDocKey(1L));
        assertThat(node.outputs().size(), is(1));
    }

    @Test
    public void testGetWithVersion() throws Exception{
        expectedException.expect(VersionInvalidException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause of a SELECT statement");
        plan("select name from users where id = 1 and _version = 1");
    }

    @Test
    public void testGetPlanStringLiteral() throws Exception {
        IterablePlan plan = (IterablePlan) plan("select name from characters where id = 'one'");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode) iterator.next();
        assertThat(node.tableInfo().ident().name(), is("characters"));
        assertThat(node.docKeys().getOnlyKey(), isDocKey("one"));
        assertFalse(iterator.hasNext());
        assertThat(node.outputs().size(), is(1));
    }

    @Test
    public void testGetPlanPartitioned() throws Exception {
        IterablePlan plan = (IterablePlan) plan("select name, date from parted where id = 'one' and date = 0");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode node = iterator.next();
        assertThat(node, instanceOf(ESGetNode.class));
        ESGetNode getNode = (ESGetNode) node;
        assertThat(getNode.tableInfo().ident().name(), is("parted"));
        assertThat(getNode.docKeys().getOnlyKey(), isDocKey("one", 0L));

        //is(new PartitionName("parted", Arrays.asList(new BytesRef("0"))).stringValue()));
        assertEquals(DataTypes.STRING, getNode.outputTypes().get(0));
        assertEquals(DataTypes.TIMESTAMP, getNode.outputTypes().get(1));
    }

    @Test
    public void testMultiGetPlan() throws Exception {
        IterablePlan plan = (IterablePlan) plan("select name from users where id in (1, 2)");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode) iterator.next();
        assertThat(node.docKeys().size(), is(2));
        assertThat(node.docKeys(), containsInAnyOrder(isDocKey(1L), isDocKey(2L)));
    }

    @Test
    public void testDeletePlan() throws Exception {
        IterablePlan plan = (IterablePlan) plan("delete from users where id = 1");
        Iterator<PlanNode> iterator = plan.iterator();
        ESDeleteNode node = (ESDeleteNode) iterator.next();
        assertThat(node.tableInfo().ident().name(), is("users"));
        assertThat(node.docKeys().size(), is(1));
        assertThat(node.docKeys().get(0), isDocKey(1L));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMultiDeletePlan() throws Exception {
        IterablePlan plan = (IterablePlan) plan("delete from users where id in (1, 2)");
        Iterator<PlanNode> iterator = plan.iterator();
        assertThat(iterator.next(), instanceOf(ESDeleteByQueryNode.class));
    }

    @Test
    public void testGroupByWithAggregationAndLimit() throws Exception {
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) plan(
                "select count(*), name from users group by name limit 1 offset 1");

        // distributed merge
        MergeNode mergeNode = distributedGroupBy.reducerMergeNode();
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));

        // limit must include offset because the real limit can only be applied on the handler
        // after all rows have been gathered.
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(1);
        assertThat(topN.limit(), is(2));
        assertThat(topN.offset(), is(0));

        // local merge
        DQLPlanNode dqlPlanNode = distributedGroupBy.localMergeNode();
        assertThat(dqlPlanNode.projections().get(0), instanceOf(TopNProjection.class));
        topN = (TopNProjection) dqlPlanNode.projections().get(0);
        assertThat(topN.limit(), is(1));
        assertThat(topN.offset(), is(1));
        assertThat(topN.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(topN.outputs().get(1), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));
    }

    @Test
    public void testGlobalAggregationPlan() throws Exception {
        GlobalAggregate globalAggregate = (GlobalAggregate) plan("select count(name) from users");
        CollectNode collectNode = globalAggregate.collectNode();

        assertEquals(DataTypes.UNDEFINED, collectNode.outputTypes().get(0));
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(AggregationProjection.class));

        MergeNode mergeNode = globalAggregate.mergeNode();

        assertEquals(DataTypes.UNDEFINED, mergeNode.inputTypes().get(0));
        assertEquals(DataTypes.LONG, mergeNode.outputTypes().get(0));
    }

    @Test
    public void testGroupByOnNodeLevel() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select count(*), name from sys.nodes group by name");
        CollectNode collectNode = planNode.collectNode();
        assertFalse(collectNode.hasDistributingDownstreams());
        assertEquals(DataTypes.STRING, collectNode.outputTypes().get(0));
        assertEquals(DataTypes.UNDEFINED, collectNode.outputTypes().get(1));

        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.numUpstreams(), is(2));
        assertThat(mergeNode.projections().size(), is(2));

        assertEquals(DataTypes.LONG, mergeNode.outputTypes().get(0));
        assertEquals(DataTypes.STRING, mergeNode.outputTypes().get(1));

        GroupProjection groupProjection = (GroupProjection) mergeNode.projections().get(0);
        assertThat(groupProjection.keys().size(), is(1));
        assertThat(((InputColumn) groupProjection.outputs().get(0)).index(), is(0));
        assertThat(groupProjection.outputs().get(1), is(instanceOf(Aggregation.class)));
        assertThat(((Aggregation) groupProjection.outputs().get(1)).functionIdent().name(), is("count"));
        assertThat(((Aggregation) groupProjection.outputs().get(1)).fromStep(), is(Aggregation.Step.PARTIAL));
        assertThat(((Aggregation)groupProjection.outputs().get(1)).toStep(), is(Aggregation.Step.FINAL));

        TopNProjection projection = (TopNProjection) mergeNode.projections().get(1);
        assertThat(((InputColumn) projection.outputs().get(0)).index(), is(1));
        assertThat(((InputColumn) projection.outputs().get(1)).index(), is(0));

    }

    @Test
    public void testShardPlan() throws Exception {
        QueryAndFetch planNode = (QueryAndFetch) plan("select id from sys.shards order by id limit 10");
        CollectNode collectNode = planNode.collectNode();

        assertEquals(DataTypes.INTEGER, collectNode.outputTypes().get(0));
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.SHARD));

        MergeNode mergeNode = planNode.localMergeNode();

        assertThat(mergeNode.inputTypes().size(), is(1));
        assertEquals(DataTypes.INTEGER, mergeNode.inputTypes().get(0));
        assertThat(mergeNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.INTEGER, mergeNode.outputTypes().get(0));

        assertThat(mergeNode.numUpstreams(), is(2));
    }

    @Test
    public void testQueryThenFetchPlan() throws Exception {
        Plan plan = plan("select name from users where name = 'x' order by id limit 10");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        CollectNode collectNode = ((QueryThenFetch) plan).collectNode();
        assertTrue(collectNode.whereClause().hasQuery());
        assertFalse(collectNode.isPartitioned());

        DQLPlanNode resultNode = ((QueryThenFetch) plan).resultNode();
        assertThat(resultNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, resultNode.outputTypes().get(0));

        assertThat(resultNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode) resultNode;
        assertTrue(mergeNode.finalProjection().isPresent());

        Projection lastProjection = mergeNode.finalProjection().get();
        assertThat(lastProjection, instanceOf(FetchProjection.class));
        FetchProjection fetchProjection = (FetchProjection) lastProjection;
        assertThat(fetchProjection.outputs().size(), is(1));
        assertThat(fetchProjection.outputs().get(0), isReference("_doc['name']"));
    }

    @Test
    public void testQueryThenFetchPlanNoFetch() throws Exception {
        // testing that a fetch projection is not added if all output symbols are included
        // at the orderBy symbols
        Plan plan = plan("select name from users where name = 'x' order by name limit 10");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        CollectNode collectNode = ((QueryThenFetch) plan).collectNode();
        assertTrue(collectNode.whereClause().hasQuery());
        assertFalse(collectNode.isPartitioned());

        DQLPlanNode resultNode = ((QueryThenFetch) plan).resultNode();
        assertThat(resultNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, resultNode.outputTypes().get(0));

        assertThat(resultNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode) resultNode;
        assertTrue(mergeNode.finalProjection().isPresent());

        Projection lastProjection = mergeNode.finalProjection().get();
        assertThat(lastProjection, instanceOf(TopNProjection.class));
        TopNProjection topNProjection = (TopNProjection) lastProjection;
        assertThat(topNProjection.outputs().size(), is(1));
    }

    @Test
    public void testQueryThenFetchPlanDefaultLimit() throws Exception {
        QueryThenFetch plan = (QueryThenFetch)plan("select name from users");
        CollectNode collectNode = plan.collectNode();
        assertThat(collectNode.limit(), is(Constants.DEFAULT_SELECT_LIMIT));

        MergeNode mergeNode = plan.mergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.finalProjection().get(), instanceOf(FetchProjection.class));
        TopNProjection topN = (TopNProjection)mergeNode.projections().get(0);
        assertThat(topN.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(topN.offset(), is(0));
        assertNull(topN.orderBy());

        FetchProjection fetchProjection = (FetchProjection)mergeNode.projections().get(1);
        assertThat(fetchProjection.bulkSize(), is(FetchProjector.NO_BULK_REQUESTS));

        // with offset
        plan = (QueryThenFetch)plan("select name from users offset 20");
        collectNode = plan.collectNode();
        assertThat(collectNode.limit(), is(Constants.DEFAULT_SELECT_LIMIT + 20));

        mergeNode = plan.mergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.finalProjection().get(), instanceOf(FetchProjection.class));
        topN = (TopNProjection)mergeNode.projections().get(0);
        assertThat(topN.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(topN.offset(), is(20));
        assertNull(topN.orderBy());

        fetchProjection = (FetchProjection)mergeNode.projections().get(1);
        assertThat(fetchProjection.bulkSize(), is(FetchProjector.NO_BULK_REQUESTS));
    }

    @Test
    public void testQueryThenFetchPlanHighLimit() throws Exception {
        QueryThenFetch plan = (QueryThenFetch)plan("select name from users limit 100000");
        CollectNode collectNode = plan.collectNode();
        assertThat(collectNode.limit(), is(100_000));

        MergeNode mergeNode = plan.mergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.finalProjection().get(), instanceOf(FetchProjection.class));
        TopNProjection topN = (TopNProjection)mergeNode.projections().get(0);
        assertThat(topN.limit(), is(100_000));
        assertThat(topN.offset(), is(0));
        assertNull(topN.orderBy());

        FetchProjection fetchProjection = (FetchProjection)mergeNode.projections().get(1);
        assertThat(fetchProjection.bulkSize(), is(Constants.DEFAULT_SELECT_LIMIT));

        // with offset
        plan = (QueryThenFetch)plan("select name from users limit 100000 offset 20");
        collectNode = plan.collectNode();
        assertThat(collectNode.limit(), is(100_000 + 20));

        mergeNode = plan.mergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.finalProjection().get(), instanceOf(FetchProjection.class));
        topN = (TopNProjection)mergeNode.projections().get(0);
        assertThat(topN.limit(), is(100_000));
        assertThat(topN.offset(), is(20));
        assertNull(topN.orderBy());

        fetchProjection = (FetchProjection)mergeNode.projections().get(1);
        assertThat(fetchProjection.bulkSize(), is(Constants.DEFAULT_SELECT_LIMIT));
    }

    @Test
    public void testQueryThenFetchPlanPartitioned() throws Exception {
        Plan plan = plan("select id, name, date from parted where date > 0 and name = 'x' order by id limit 10");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        CollectNode collectNode = ((QueryThenFetch) plan).collectNode();

        List<String> indices = new ArrayList<>();
        Map<String, Map<String, List<Integer>>> locations = collectNode.routing().locations();
        for (Map.Entry<String, Map<String, List<Integer>>> entry : locations.entrySet()) {
            indices.addAll(entry.getValue().keySet());
        }
        assertThat(indices, Matchers.contains(
                new PartitionName("parted", Arrays.asList(new BytesRef("123"))).stringValue()));

        assertTrue(collectNode.whereClause().hasQuery());
        assertTrue(collectNode.isPartitioned());

        DQLPlanNode resultNode = ((QueryThenFetch) plan).resultNode();
        assertThat(resultNode.outputTypes().size(), is(3));
    }

    @Test
    public void testQueryThenFetchPlanFunction() throws Exception {
        Plan plan = plan("select format('Hi, my name is %s', name), name from users where name = 'x' order by id limit 10");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        CollectNode collectNode = ((QueryThenFetch) plan).collectNode();

        assertTrue(collectNode.whereClause().hasQuery());
        assertFalse(collectNode.isPartitioned());

        DQLPlanNode resultNode = ((QueryThenFetch) plan).resultNode();
        assertThat(resultNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, resultNode.outputTypes().get(0));
        assertEquals(DataTypes.STRING, resultNode.outputTypes().get(1));

        assertThat(resultNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode) resultNode;
        assertTrue(mergeNode.finalProjection().isPresent());

        Projection lastProjection = mergeNode.finalProjection().get();
        assertThat(lastProjection, instanceOf(FetchProjection.class));
        FetchProjection fetchProjection = (FetchProjection) lastProjection;
        assertThat(fetchProjection.outputs().size(), is(2));
        assertThat(fetchProjection.outputs().get(0), isFunction("format"));
        assertThat(fetchProjection.outputs().get(1), isReference("_doc['name']"));

    }

    @Test
    public void testInsertPlan() throws Exception {
        Upsert plan = (Upsert) plan("insert into users (id, name) values (42, 'Deep Thought')");

        assertThat(plan.nodes().size(), is(1));

        PlanNode next = ((IterablePlan) plan.nodes().get(0)).iterator().next();
        assertThat(next, instanceOf(SymbolBasedUpsertByIdNode.class));

        SymbolBasedUpsertByIdNode updateNode = (SymbolBasedUpsertByIdNode)next;

        assertThat(updateNode.insertColumns().length, is(2));
        Reference idRef = updateNode.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = updateNode.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(updateNode.items().size(), is(1));
        SymbolBasedUpsertByIdNode.Item item = updateNode.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("42"));
        assertThat(item.routing(), is("42"));

        assertThat(item.insertValues().length, is(2));
        assertThat((Long)item.insertValues()[0], is(42L));
        assertThat((BytesRef) item.insertValues()[1], is(new BytesRef("Deep Thought")));
    }

    @Test
    public void testInsertPlanMultipleValues() throws Exception {
        Upsert plan = (Upsert) plan("insert into users (id, name) values (42, 'Deep Thought'), (99, 'Marvin')");

        assertThat(plan.nodes().size(), is(1));

        PlanNode next = ((IterablePlan) plan.nodes().get(0)).iterator().next();
        assertThat(next, instanceOf(SymbolBasedUpsertByIdNode.class));

        SymbolBasedUpsertByIdNode updateNode = (SymbolBasedUpsertByIdNode)next;

        assertThat(updateNode.insertColumns().length, is(2));
        Reference idRef = updateNode.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = updateNode.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(updateNode.items().size(), is(2));

        SymbolBasedUpsertByIdNode.Item item1 = updateNode.items().get(0);
        assertThat(item1.index(), is("users"));
        assertThat(item1.id(), is("42"));
        assertThat(item1.routing(), is("42"));
        assertThat(item1.insertValues().length, is(2));
        assertThat((Long)item1.insertValues()[0], is(42L));
        assertThat((BytesRef)item1.insertValues()[1], is(new BytesRef("Deep Thought")));

        SymbolBasedUpsertByIdNode.Item item2 = updateNode.items().get(1);
        assertThat(item2.index(), is("users"));
        assertThat(item2.id(), is("99"));
        assertThat(item2.routing(), is("99"));
        assertThat(item2.insertValues().length, is(2));
        assertThat((Long)item2.insertValues()[0], is(99L));
        assertThat((BytesRef) item2.insertValues()[1], is(new BytesRef("Marvin")));
    }

    @Test
    public void testCountDistinctPlan() throws Exception {
        GlobalAggregate globalAggregate = (GlobalAggregate) plan("select count(distinct name) from users");

        CollectNode collectNode = globalAggregate.collectNode();
        Projection projection = collectNode.projections().get(0);
        assertThat(projection, instanceOf(AggregationProjection.class));
        AggregationProjection aggregationProjection = (AggregationProjection)projection;
        assertThat(aggregationProjection.aggregations().size(), is(1));

        Aggregation aggregation = aggregationProjection.aggregations().get(0);
        assertThat(aggregation.toStep(), is(Aggregation.Step.PARTIAL));
        Symbol aggregationInput = aggregation.inputs().get(0);
        assertThat(aggregationInput.symbolType(), is(SymbolType.INPUT_COLUMN));

        assertThat(collectNode.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference) collectNode.toCollect().get(0)).info().ident().columnIdent().name(), is("name"));

        MergeNode mergeNode = globalAggregate.mergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        Projection projection1 = mergeNode.projections().get(1);
        assertThat(projection1, instanceOf(TopNProjection.class));
        Symbol collection_count = projection1.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumn() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select count(*), id from users group by id limit 20");
        CollectNode collectNode = planNode.collectNode();
        assertFalse(collectNode.hasDistributingDownstreams());
        assertThat(collectNode.projections().size(), is(2));
        assertThat(collectNode.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(collectNode.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumnSorted() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select count(*), id from users group by id order by 1 desc nulls last limit 20");
        CollectNode collectNode = planNode.collectNode();
        assertFalse(collectNode.hasDistributingDownstreams());
        assertThat(collectNode.projections().size(), is(2));
        assertThat(collectNode.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection)collectNode.projections().get(1)).orderBy().size(), is(1));

        assertThat(collectNode.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));
        TopNProjection projection = (TopNProjection)mergeNode.projections().get(0);
        assertThat(projection.orderBy(), is(nullValue()));
        assertThat(mergeNode.sortedInputOutput(), is(true));
        assertThat(mergeNode.orderByIndices().length, is(1));
        assertThat(mergeNode.orderByIndices()[0], is(0));
        assertThat(mergeNode.reverseFlags()[0], is(true));
        assertThat(mergeNode.nullsFirst()[0], is(false));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumnSortedScalar() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select count(*) + 1, id from users group by id order by count(*) + 1 limit 20");
        CollectNode collectNode = planNode.collectNode();
        assertFalse(collectNode.hasDistributingDownstreams());
        assertThat(collectNode.projections().size(), is(2));
        assertThat(collectNode.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection)collectNode.projections().get(1)).orderBy().size(), is(1));

        assertThat(collectNode.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));
        TopNProjection projection = (TopNProjection)mergeNode.projections().get(0);
        assertThat(projection.orderBy(), is(nullValue()));
        assertThat(mergeNode.sortedInputOutput(), is(true));
        assertThat(mergeNode.orderByIndices().length, is(1));
        assertThat(mergeNode.orderByIndices()[0], is(0));
        assertThat(mergeNode.reverseFlags()[0], is(false));
        assertThat(mergeNode.nullsFirst()[0], is(nullValue()));
    }

    @Test
    public void testNoDistributedGroupByOnAllPrimaryKeys() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select count(*), id, date from empty_parted group by id, date limit 20");
        CollectNode collectNode = planNode.collectNode();
        assertFalse(collectNode.hasDistributingDownstreams());
        assertThat(collectNode.projections().size(), is(2));
        assertThat(collectNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectNode.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(collectNode.projections().get(1), instanceOf(TopNProjection.class));
        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(TopNProjection.class));
    }

    @Test
    public void testNonDistributedGroupByAggregationsWrappedInScalar() throws Exception {
        DistributedGroupBy planNode = (DistributedGroupBy) plan(
                "select (count(*) + 1), id from empty_parted group by id");
        CollectNode collectNode = planNode.collectNode();
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(GroupProjection.class));

        TopNProjection topNProjection = (TopNProjection) planNode.reducerMergeNode().projections().get(1);
        assertThat(topNProjection.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(topNProjection.offset(), is(0));

        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(TopNProjection.class));
    }

    @Test
    public void testGroupByWithOrderOnAggregate() throws Exception {
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) plan(
                "select count(*), name from users group by name order by count(*)");

        // sort is on handler because there is no limit/offset
        // handler
        MergeNode mergeNode = distributedGroupBy.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));

        TopNProjection topNProjection = (TopNProjection)mergeNode.projections().get(0);
        Symbol orderBy = topNProjection.orderBy().get(0);
        assertThat(orderBy, instanceOf(InputColumn.class));

        assertThat(orderBy.valueType(), Is.<DataType>is(DataTypes.LONG));
    }

    @Test
    public void testHandlerSideRouting() throws Exception {
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        QueryAndFetch plan = (QueryAndFetch) plan("select * from sys.cluster");
    }

    @Test
    public void testHandlerSideRoutingGroupBy() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select count(*) from sys.cluster group by name");
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        CollectNode collectNode = planNode.collectNode();
        assertThat(collectNode.toCollect().get(0), instanceOf(Reference.class));
        assertThat(collectNode.toCollect().size(), is(1));

        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));
    }

    @Test
    public void testCountDistinctWithGroupBy() throws Exception {
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) plan(
                "select count(distinct id), name from users group by name order by count(distinct id)");
        CollectNode collectNode = distributedGroupBy.collectNode();

        // collect
        assertThat(collectNode.toCollect().get(0), instanceOf(Reference.class));
        assertThat(collectNode.toCollect().size(), is(2));
        assertThat(((Reference)collectNode.toCollect().get(0)).info().ident().columnIdent().name(), is("id"));
        assertThat(((Reference)collectNode.toCollect().get(1)).info().ident().columnIdent().name(), is("name"));
        Projection projection = collectNode.projections().get(0);
        assertThat(projection, instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection)projection;
        Symbol groupKey = groupProjection.keys().get(0);
        assertThat(groupKey, instanceOf(InputColumn.class));
        assertThat(((InputColumn)groupKey).index(), is(1));
        assertThat(groupProjection.values().size(), is(1));

        Aggregation aggregation = groupProjection.values().get(0);
        assertThat(aggregation.toStep(), is(Aggregation.Step.PARTIAL));
        Symbol aggregationInput = aggregation.inputs().get(0);
        assertThat(aggregationInput.symbolType(), is(SymbolType.INPUT_COLUMN));



        // reducer
        MergeNode mergeNode = distributedGroupBy.reducerMergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        Projection groupProjection1 = mergeNode.projections().get(0);
        assertThat(groupProjection1, instanceOf(GroupProjection.class));
        groupProjection = (GroupProjection)groupProjection1;
        assertThat(groupProjection.keys().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn)groupProjection.keys().get(0)).index(), is(0));

        assertThat(groupProjection.values().get(0), instanceOf(Aggregation.class));
        Aggregation aggregationStep2 = groupProjection.values().get(0);
        assertThat(aggregationStep2.toStep(), is(Aggregation.Step.FINAL));

        TopNProjection topNProjection = (TopNProjection)mergeNode.projections().get(1);
        Symbol collection_count = topNProjection.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));


        // handler
        MergeNode localMergeNode = distributedGroupBy.localMergeNode();
        assertThat(localMergeNode.projections().size(), is(1));
        Projection localTopN = localMergeNode.projections().get(0);
        assertThat(localTopN, instanceOf(TopNProjection.class));
    }

    @Test
    public void testUpdateByQueryPlan() throws Exception {
        Upsert plan = (Upsert) plan("update users set name='Vogon lyric fan'");
        assertThat(plan.nodes().size(), is(1));

        CollectAndMerge planNode = (CollectAndMerge) plan.nodes().get(0);

        CollectNode collectNode = planNode.collectNode();
        assertThat(collectNode.routing(), is(shardRouting));
        assertFalse(collectNode.whereClause().noMatch());
        assertFalse(collectNode.whereClause().hasQuery());
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(UpdateProjection.class));
        assertThat(collectNode.toCollect().size(), is(1));
        assertThat(collectNode.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference)collectNode.toCollect().get(0)).info().ident().columnIdent().fqn(), is("_uid"));

        UpdateProjection updateProjection = (UpdateProjection)collectNode.projections().get(0);
        assertThat(updateProjection.uidSymbol(), instanceOf(InputColumn.class));

        assertThat(updateProjection.assignmentsColumns()[0], is("name"));
        Symbol symbol = updateProjection.assignments()[0];
        assertThat(symbol, isLiteral("Vogon lyric fan", DataTypes.STRING));

        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(AggregationProjection.class));

        assertThat(mergeNode.outputTypes().size(), is(1));
    }

    @Test
    public void testUpdateByIdPlan() throws Exception {
        Upsert planNode = (Upsert) plan("update users set name='Vogon lyric fan' where id=1");
        assertThat(planNode.nodes().size(), is(1));

        PlanNode next = ((IterablePlan) planNode.nodes().get(0)).iterator().next();
        assertThat(next, instanceOf(SymbolBasedUpsertByIdNode.class));

        SymbolBasedUpsertByIdNode updateNode = (SymbolBasedUpsertByIdNode) next;
        assertThat(updateNode.items().size(), is(1));

        assertThat(updateNode.updateColumns()[0], is("name"));

        SymbolBasedUpsertByIdNode.Item item = updateNode.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("1"));

        Symbol symbol = item.updateAssignments()[0];
        assertThat(symbol, isLiteral("Vogon lyric fan", DataTypes.STRING));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValues() throws Exception {
        Upsert planNode =  (Upsert) plan("update users set name='Vogon lyric fan' where id in (1,2,3)");
        assertThat(planNode.nodes().size(), is(1));

        PlanNode next = ((IterablePlan) planNode.nodes().get(0)).iterator().next();

        assertThat(next, instanceOf(SymbolBasedUpsertByIdNode.class));
        SymbolBasedUpsertByIdNode updateNode = (SymbolBasedUpsertByIdNode) next;

        List<String> ids = new ArrayList<>(3);
        for (SymbolBasedUpsertByIdNode.Item item : updateNode.items()) {
            ids.add(item.id());
            assertThat(item.updateAssignments().length, is(1));
            assertThat(item.updateAssignments()[0], isLiteral("Vogon lyric fan", DataTypes.STRING));
        }

        assertThat(ids, containsInAnyOrder("1", "2", "3"));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValuesPartitioned() throws Exception {
        Upsert planNode =  (Upsert) plan("update parted set name='Vogon lyric fan' where " +
                "(id=2 and date = 0) OR" +
                "(id=3 and date=123)");
        assertThat(planNode.nodes().size(), is(1));

        PlanNode next = ((IterablePlan) planNode.nodes().get(0)).iterator().next();

        assertThat(next, instanceOf(SymbolBasedUpsertByIdNode.class));
        SymbolBasedUpsertByIdNode updateNode = (SymbolBasedUpsertByIdNode) next;

        List<String> partitions = new ArrayList<>(2);
        List<String> ids = new ArrayList<>(2);
        for (SymbolBasedUpsertByIdNode.Item item : updateNode.items()) {
            partitions.add(item.index());
            ids.add(item.id());
            assertThat(item.updateAssignments().length, is(1));
            assertThat(item.updateAssignments()[0], isLiteral("Vogon lyric fan", DataTypes.STRING));
        }
        assertThat(ids, containsInAnyOrder("AgEyATA=", "AgEzAzEyMw==")); // multi primary key - values concatenated and base64'ed
        assertThat(partitions, containsInAnyOrder(".partitioned.parted.04130", ".partitioned.parted.04232chj"));
    }

    @Test
    public void testCopyFromPlan() throws Exception {
        CollectAndMerge plan = (CollectAndMerge) plan("copy users from '/path/to/file.extension'");
        assertThat(plan.collectNode(), instanceOf(FileUriCollectNode.class));

        FileUriCollectNode collectNode = (FileUriCollectNode)plan.collectNode();
        assertThat((BytesRef) ((Literal) collectNode.targetUri()).value(),
                is(new BytesRef("/path/to/file.extension")));
    }

    @Test
    public void testCopyFromNumReadersSetting() throws Exception {
        CollectAndMerge plan = (CollectAndMerge) plan("copy users from '/path/to/file.extension' with (num_readers=1)");
        assertThat(plan.collectNode(), instanceOf(FileUriCollectNode.class));
        FileUriCollectNode collectNode = (FileUriCollectNode) plan.collectNode();
        assertThat(collectNode.executionNodes().size(), is(1));
    }

    @Test
    public void testCopyFromPlanWithParameters() throws Exception {
        CollectAndMerge plan = (CollectAndMerge) plan("copy users from '/path/to/file.ext' with (bulk_size=30, compression='gzip', shared=true)");
        assertThat(plan.collectNode(), instanceOf(FileUriCollectNode.class));
        FileUriCollectNode collectNode = (FileUriCollectNode)plan.collectNode();
        SourceIndexWriterProjection indexWriterProjection = (SourceIndexWriterProjection) collectNode.projections().get(0);
        assertThat(indexWriterProjection.bulkActions(), is(30));
        assertThat(collectNode.compression(), is("gzip"));
        assertThat(collectNode.sharedStorage(), is(true));

        // verify defaults:
        plan = (CollectAndMerge) plan("copy users from '/path/to/file.ext'");
        collectNode = (FileUriCollectNode)plan.collectNode();
        assertNull(collectNode.compression());
        assertNull(collectNode.sharedStorage());
    }

    @Test
    public void testCopyToWithColumnsReferenceRewrite() throws Exception {
        CollectAndMerge plan = (CollectAndMerge) plan("copy users (name) to '/file.ext'");
        CollectNode node = plan.collectNode();
        Reference nameRef = (Reference)node.toCollect().get(0);

        assertThat(nameRef.info().ident().columnIdent().name(), is(DocSysColumns.DOC.name()));
        assertThat(nameRef.info().ident().columnIdent().path().get(0), is("name"));
    }

    @Test
    public void testCopyToWithNonExistentPartitionClause() throws Exception {
        CollectAndMerge plan = (CollectAndMerge) plan("copy parted partition (date=0) to '/foo.txt' ");
        assertFalse(plan.collectNode().routing().hasLocations());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCopyFromPlanWithInvalidParameters() throws Exception {
        plan("copy users from '/path/to/file.ext' with (bulk_size=-28)");
    }

    @Test
    public void testShardSelect() throws Exception {
        QueryAndFetch planNode = (QueryAndFetch) plan("select id from sys.shards");
        CollectNode collectNode = planNode.collectNode();
        assertTrue(collectNode.isRouted());
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testDropTable() throws Exception {
        IterablePlan plan = (IterablePlan) plan("drop table users");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(DropTableNode.class));

        DropTableNode node = (DropTableNode) planNode;
        assertThat(node.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropTableIfExistsWithUnknownSchema() throws Exception {
        Plan plan = plan("drop table if exists unknown_schema.unknwon_table");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testDropTableIfExists() throws Exception {
        IterablePlan plan = (IterablePlan) plan("drop table if exists users");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(DropTableNode.class));

        DropTableNode node = (DropTableNode) planNode;
        assertThat(node.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropTableIfExistsNonExistentTableCreatesNoop() throws Exception {
        Plan plan = plan("drop table if exists groups");
        assertThat(plan, instanceOf(NoopPlan.class));
    }


    @Test
    public void testDropPartitionedTable() throws Exception {
        IterablePlan plan = (IterablePlan) plan("drop table parted");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();

        assertThat(planNode, instanceOf(DropTableNode.class));
        DropTableNode node = (DropTableNode) planNode;
        assertThat(node.tableInfo().ident().name(), is("parted"));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDropBlobTableIfExistsCreatesIterablePlan() throws Exception {
        Plan plan = plan("drop blob table if exists screenshots");
        assertThat(plan, instanceOf(IterablePlan.class));
    }

    @Test
    public void testDropNonExistentBlobTableCreatesNoop() throws Exception {
        Plan plan = plan("drop blob table if exists unknown");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testGlobalCountPlan() throws Exception {
        CountPlan plan = (CountPlan) plan("select count(*) from users");
        assertThat(plan, instanceOf(PlannedAnalyzedRelation.class));

        assertThat(plan.countNode().whereClause(), equalTo(WhereClause.MATCH_ALL));

        assertThat(plan.mergeNode().projections().size(), is(1));
        assertThat(plan.mergeNode().projections().get(0), instanceOf(AggregationProjection.class));
    }

    @Test
    public void testSetPlan() throws Exception {
        IterablePlan plan = (IterablePlan) plan("set GLOBAL PERSISTENT stats.jobs_log_size=1024");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESClusterUpdateSettingsNode.class));

        ESClusterUpdateSettingsNode node = (ESClusterUpdateSettingsNode) planNode;
        // set transient settings too when setting persistent ones
        assertThat(node.transientSettings().toDelimitedString(','), is("stats.jobs_log_size=1024,"));
        assertThat(node.persistentSettings().toDelimitedString(','), is("stats.jobs_log_size=1024,"));

        plan = (IterablePlan)  plan("set GLOBAL TRANSIENT stats.enabled=false,stats.jobs_log_size=0");
        iterator = plan.iterator();
        planNode = iterator.next();
        assertThat(planNode, instanceOf(ESClusterUpdateSettingsNode.class));

        node = (ESClusterUpdateSettingsNode) planNode;
        assertThat(node.persistentSettings().getAsMap().size(), is(0));
        assertThat(node.transientSettings().toDelimitedString(','), is("stats.enabled=false,stats.jobs_log_size=0,"));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupBy() throws Exception {
        InsertFromSubQuery planNode = (InsertFromSubQuery) plan(
                "insert into users (id, name) (select name, count(*) from sys.nodes group by name)");
        NonDistributedGroupBy nonDistributedGroupBy = (NonDistributedGroupBy)planNode.innerPlan();
        MergeNode mergeNode = nonDistributedGroupBy.localMergeNode();
        assertThat(mergeNode.projections().size(), is(3));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(mergeNode.projections().get(2), instanceOf(ColumnIndexWriterProjection.class));
        assertThat(planNode.handlerMergeNode().isPresent(), is(false));
    }

    @Test (expected = UnsupportedFeatureException.class)
    public void testInsertFromSubQueryDistributedGroupByWithLimit() throws Exception {
        IterablePlan plan = (IterablePlan) plan("insert into users (id, name) (select name, count(*) from users group by name order by name limit 10)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(CollectNode.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode)planNode;
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        mergeNode = (MergeNode)planNode;
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(0), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection)mergeNode.projections().get(0)).limit(), is(10));

        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByWithoutLimit() throws Exception {
        InsertFromSubQuery planNode = (InsertFromSubQuery) plan(
                "insert into users (id, name) (select name, count(*) from users group by name)");
        DistributedGroupBy groupBy = (DistributedGroupBy)planNode.innerPlan();
        MergeNode mergeNode = groupBy.reducerMergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)mergeNode.projections().get(1);
        assertThat(projection.primaryKeys().size(), is(1));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.columnReferences().size(), is(2));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("id"));
        assertThat(projection.columnReferences().get(1).ident().columnIdent().fqn(), is("name"));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableIdent().fqn(), is("users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));

        MergeNode localMergeNode = planNode.handlerMergeNode().get();
        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByPartitioned() throws Exception {
        InsertFromSubQuery planNode = (InsertFromSubQuery) plan(
                "insert into parted (id, date) (select id, date from users group by id, date)");
        DistributedGroupBy groupBy = (DistributedGroupBy)planNode.innerPlan();
        MergeNode mergeNode = groupBy.reducerMergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)mergeNode.projections().get(1);
        assertThat(projection.primaryKeys().size(), is(2));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.primaryKeys().get(1).fqn(), is("date"));

        assertThat(projection.columnReferences().size(), is(1));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("id"));

        assertThat(projection.partitionedBySymbols().size(), is(1));
        assertThat(((InputColumn) projection.partitionedBySymbols().get(0)).index(), is(1));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableIdent().fqn(), is("parted"));

        MergeNode localMergeNode = planNode.handlerMergeNode().get();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

    }

    @Test
    public void testInsertFromSubQueryGlobalAggregate() throws Exception {
        InsertFromSubQuery planNode = (InsertFromSubQuery) plan(
                "insert into users (name, id) (select arbitrary(name), count(*) from users)");
        GlobalAggregate globalAggregate = (GlobalAggregate)planNode.innerPlan();
        MergeNode mergeNode = globalAggregate.mergeNode();
        assertThat(mergeNode.projections().size(), is(3));
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(mergeNode.projections().get(2), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)mergeNode.projections().get(2);

        assertThat(projection.columnReferences().size(), is(2));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("name"));
        assertThat(projection.columnReferences().get(1).ident().columnIdent().fqn(), is("id"));

        assertThat(projection.columnSymbols().size(), is(2));
        assertThat(((InputColumn)projection.columnSymbols().get(0)).index(), is(0));
        assertThat(((InputColumn)projection.columnSymbols().get(1)).index(), is(1));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableIdent().fqn(), is("users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));

        assertThat(planNode.handlerMergeNode().isPresent(), is(false));
    }

    @Test
    public void testInsertFromSubQueryESGet() throws Exception {
        // doesn't use ESGetNode but CollectNode.
        // Round-trip to handler can be skipped by writing from the shards directly
        InsertFromSubQuery planNode = (InsertFromSubQuery) plan(
                "insert into users (date, id, name) (select date, id, name from users where id=1)");
        QueryAndFetch queryAndFetch = (QueryAndFetch)planNode.innerPlan();
        CollectNode collectNode = queryAndFetch.collectNode();

        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)collectNode.projections().get(0);

        assertThat(projection.columnReferences().size(), is(3));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("date"));
        assertThat(projection.columnReferences().get(1).ident().columnIdent().fqn(), is("id"));
        assertThat(projection.columnReferences().get(2).ident().columnIdent().fqn(), is("name"));
        assertThat(((InputColumn) projection.ids().get(0)).index(), is(1));
        assertThat(((InputColumn)projection.clusteredBy()).index(), is(1));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));

        assertThat(planNode.handlerMergeNode().isPresent(), is(true));
    }

    @Test (expected = UnsupportedFeatureException.class)
    public void testInsertFromSubQueryWithLimit() throws Exception {
        Plan plan = plan("insert into users (date, id, name) (select date, id, name from users limit 10)");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        CollectNode collectNode = ((QueryThenFetch) plan).collectNode();
        assertTrue(collectNode.whereClause().hasQuery());
        assertFalse(collectNode.isPartitioned());

        DQLPlanNode resultNode = ((QueryThenFetch) plan).resultNode();
        assertThat(resultNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, resultNode.outputTypes().get(0));

        assertThat(resultNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode) resultNode;
        assertTrue(mergeNode.finalProjection().isPresent());

        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
    }

    @Test (expected = UnsupportedFeatureException.class)
    public void testInsertFromSubQueryWithOffset() throws Exception {
        plan("insert into users (date, id, name) (select date, id, name from users offset 10)");
    }

    @Test (expected = UnsupportedFeatureException.class)
    public void testInsertFromSubQueryWithOrderBy() throws Exception {
        plan("insert into users (date, id, name) (select date, id, name from users order by id)");
    }

    @Test
    public void testInsertFromSubQueryWithoutLimit() throws Exception {
        InsertFromSubQuery planNode = (InsertFromSubQuery) plan(
                "insert into users (date, id, name) (select date, id, name from users)");
        QueryAndFetch queryAndFetch = (QueryAndFetch)planNode.innerPlan();
        CollectNode collectNode = queryAndFetch.collectNode();
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(ColumnIndexWriterProjection.class));
        assertNull(queryAndFetch.localMergeNode());

        MergeNode localMergeNode = planNode.handlerMergeNode().get();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
    }

    @Test
    public void testGroupByHaving() throws Exception {
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) plan(
                "select avg(date), name from users group by name having min(date) > '1970-01-01'");
        CollectNode collectNode = distributedGroupBy.collectNode();
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(GroupProjection.class));

        MergeNode mergeNode = distributedGroupBy.reducerMergeNode();

        assertThat(mergeNode.projections().size(), is(3));

        // grouping
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection)mergeNode.projections().get(0);
        assertThat(groupProjection.values().size(), is(2));

        // filter the having clause
        assertThat(mergeNode.projections().get(1), instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection)mergeNode.projections().get(1);

        // apply the default limit
        assertThat(mergeNode.projections().get(2), instanceOf(TopNProjection.class));
        TopNProjection topN = (TopNProjection)mergeNode.projections().get(2);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.DOUBLE));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));
        assertThat(topN.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
    }

    @Test
    public void testInsertFromQueryWithPartitionedColumn() throws Exception {
        InsertFromSubQuery planNode = (InsertFromSubQuery) plan(
                "insert into users (id, date) (select id, date from parted)");
        QueryAndFetch queryAndFetch = (QueryAndFetch)planNode.innerPlan();
        CollectNode collectNode = queryAndFetch.collectNode();
        List<Symbol> toCollect = collectNode.toCollect();
        assertThat(toCollect.size(), is(2));
        assertThat(toCollect.get(0), isFunction("toLong"));
        assertThat(((Function) toCollect.get(0)).arguments().get(0), isReference("_doc['id']"));
        assertThat((Reference) toCollect.get(1), equalTo(new Reference(new ReferenceInfo(
                new ReferenceIdent(new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "parted"), "date"), RowGranularity.PARTITION, DataTypes.TIMESTAMP))));
    }

    @Test
    public void testGroupByHavingInsertInto() throws Exception {
        InsertFromSubQuery planNode = (InsertFromSubQuery) plan(
                "insert into users (id, name) (select name, count(*) from users group by name having count(*) > 3)");
        DistributedGroupBy groupByNode = (DistributedGroupBy)planNode.innerPlan();
        MergeNode mergeNode = groupByNode.reducerMergeNode();
        assertThat(mergeNode.projections().size(), is(3));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(FilterProjection.class));
        assertThat(mergeNode.projections().get(2), instanceOf(ColumnIndexWriterProjection.class));

        FilterProjection filterProjection = (FilterProjection)mergeNode.projections().get(1);
        assertThat(filterProjection.outputs().size(), is(2));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(filterProjection.outputs().get(1), instanceOf(InputColumn.class));

        InputColumn inputColumn = (InputColumn)filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));
        inputColumn = (InputColumn)filterProjection.outputs().get(1);
        assertThat(inputColumn.index(), is(1));
        MergeNode localMergeNode = planNode.handlerMergeNode().get();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

    }

    @Test
    public void testGroupByHavingNonDistributed() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select id from users group by id having id > 0");
        CollectNode collectNode = planNode.collectNode();
        assertThat(collectNode.projections().size(), is(2));
        assertThat(collectNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectNode.projections().get(1), instanceOf(FilterProjection.class));

        FilterProjection filterProjection = (FilterProjection)collectNode.projections().get(1);
        assertThat(filterProjection.requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(filterProjection.outputs().size(), is(1));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn)filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        MergeNode localMergeNode = planNode.localMergeNode();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(TopNProjection.class));
    }

    @Test
    public void testGlobalAggregationHaving() throws Exception {
        GlobalAggregate globalAggregate = (GlobalAggregate) plan(
                "select avg(date) from users having min(date) > '1970-01-01'");
        CollectNode collectNode = globalAggregate.collectNode();
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(AggregationProjection.class));

        MergeNode localMergeNode = globalAggregate.mergeNode();

        assertThat(localMergeNode.projections().size(), is(3));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.projections().get(1), instanceOf(FilterProjection.class));
        assertThat(localMergeNode.projections().get(2), instanceOf(TopNProjection.class));

        AggregationProjection aggregationProjection = (AggregationProjection)localMergeNode.projections().get(0);
        assertThat(aggregationProjection.aggregations().size(), is(2));

        FilterProjection filterProjection = (FilterProjection)localMergeNode.projections().get(1);
        assertThat(filterProjection.outputs().size(), is(2));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn)filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        TopNProjection topNProjection = (TopNProjection)localMergeNode.projections().get(2);
        assertThat(topNProjection.outputs().size(), is(1));
    }

    @Test
    public void testCountOnPartitionedTable() throws Exception {
        CountPlan plan = (CountPlan) plan("select count(*) from parted where date = 123");
        assertThat(plan, instanceOf(PlannedAnalyzedRelation.class));
        assertThat(plan.countNode().whereClause().partitions(), containsInAnyOrder(".partitioned.parted.04232chj"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectPartitionedTableOrderByPartitionedColumn() throws Exception {
        plan("select name from parted order by date");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectPartitionedTableOrderByPartitionedColumnInFunction() throws Exception {
        plan("select name from parted order by year(date)");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectOrderByPartitionedNestedColumn() throws Exception {
        plan("select id from multi_parted order by obj['name']");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectOrderByPartitionedNestedColumnInFunction() throws Exception {
        plan("select id from multi_parted order by format('abc %s', obj['name'])");
    }

    @Test(expected = UnsupportedFeatureException.class)
    public void testQueryRequiresScalar() throws Exception {
        // only scalar functions are allowed on system tables because we have no lucene queries
        plan("select * from sys.shards where match(table_name, 'characters')");
    }

    @Test
    public void testGroupByWithHavingAndLimit() throws Exception {
        DistributedGroupBy planNode = (DistributedGroupBy) plan(
                "select count(*), name from users group by name having count(*) > 1 limit 100");;

        MergeNode mergeNode = planNode.reducerMergeNode(); // reducer

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: count(*), name
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(2);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.LONG));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));


        MergeNode localMerge = planNode.localMergeNode();
        // topN projection
        //      outputs: count(*), name
        topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.LONG));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));
    }

    @Test
    public void testGroupByWithHavingAndNoLimit() throws Exception {
        DistributedGroupBy planNode = (DistributedGroupBy) plan(
                "select count(*), name from users group by name having count(*) > 1");

        MergeNode mergeNode = planNode.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        assertThat(mergeNode.outputTypes().get(0), equalTo((DataType) DataTypes.LONG));
        assertThat(mergeNode.outputTypes().get(1), equalTo((DataType) DataTypes.STRING));

        mergeNode = planNode.localMergeNode();

        assertThat(mergeNode.outputTypes().get(0), equalTo((DataType) DataTypes.LONG));
        assertThat(mergeNode.outputTypes().get(1), equalTo((DataType) DataTypes.STRING));
    }

    @Test
    public void testGroupByWithHavingAndNoSelectListReordering() throws Exception {
        DistributedGroupBy planNode = (DistributedGroupBy) plan(
                "select name, count(*) from users group by name having count(*) > 1");

        MergeNode mergeNode = planNode.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        MergeNode localMerge = planNode.localMergeNode();
        // topN projection
        //      outputs: name, count(*)
        TopNProjection topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));
    }

    @Test
    public void testGroupByHavingAndNoSelectListReOrderingWithLimit() throws Exception {
        DistributedGroupBy planNode = (DistributedGroupBy) plan(
                "select name, count(*) from users group by name having count(*) > 1 limit 100");

        MergeNode mergeNode = planNode.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)
        // topN projection
        //      outputs: name, count(*)

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        // outputs: name, count(*)
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(2);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));


        MergeNode localMerge = planNode.localMergeNode();

        // topN projection
        //      outputs: name, count(*)
        topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));
    }

    @Test
    public void testOrderByOnAnalyzed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'users.text': sorting on analyzed/fulltext columns is not possible");
        plan("select text from users u order by 1");
    }

    @Test
    public void testSortOnUnknownColumn() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'details['unknown_column']': invalid data type 'null'.");
        plan("select details from ignored_nested order by details['unknown_column']");
    }

    @Test
    public void testOrderByOnIndexOff() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'users.no_index': sorting on non-indexed columns is not possible");
        plan("select no_index from users u order by 1");
    }

    @Test
    public void testGroupByOnAnalyzed() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'users.text': grouping on analyzed/fulltext columns is not possible");
        plan("select text from users u group by 1");
    }

    @Test
    public void testGroupByOnIndexOff() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'users.no_index': grouping on non-indexed columns is not possible");
        plan("select no_index from users u group by 1");
    }

    @Test
    public void testSelectAnalyzedReferenceInFunctionGroupBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'users.text': grouping on analyzed/fulltext columns is not possible");
        plan("select substr(text, 0, 2) from users u group by 1");
    }

    @Test
    public void testSelectAnalyzedReferenceInFunctionAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select analyzed column 'users.text' within grouping or aggregations");
        plan("select min(substr(text, 0, 2)) from users");
    }

    @Test
    public void testSelectNonIndexedReferenceInFunctionGroupBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'users.no_index': grouping on non-indexed columns is not possible");
        plan("select substr(no_index, 0, 2) from users u group by 1");
    }

    @Test
    public void testSelectNonIndexedReferenceInFunctionAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select non-indexed column 'users.no_index' within grouping or aggregations");
        plan("select min(substr(no_index, 0, 2)) from users");
    }

    @Test
    public void testGlobalAggregateWithWhereOnPartitionColumn() throws Exception {
        GlobalAggregate globalAggregate = (GlobalAggregate) plan(
                "select min(name) from parted where date > 0");

        WhereClause whereClause = globalAggregate.collectNode().whereClause();
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.noMatch(), is(false));
    }

    private void assertNoop(Plan plan){
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testHasNoResultFromHaving() throws Exception {
        assertNoop(plan("select min(name) from users having 1 = 2"));
    }

    @Test
    public void testHasNoResultFromLimit() {
        assertNoop(plan("select count(*) from users limit 1 offset 1"));
        assertNoop(plan("select count(*) from users limit 5 offset 1"));
        assertNoop(plan("select count(*) from users limit 0"));
    }

    @Test
    public void testHasNoResultFromQuery() {
        assertNoop(plan("select name from users where false"));
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKey() throws Exception {
        Upsert plan = (Upsert) plan("insert into users (id, name) values (1, null) on duplicate key update name = values(name)");
        PlanNode planNode = ((IterablePlan) plan.nodes().get(0)).iterator().next();
        assertThat(planNode, instanceOf(SymbolBasedUpsertByIdNode.class));
        SymbolBasedUpsertByIdNode node = (SymbolBasedUpsertByIdNode) planNode;

        assertThat(node.updateColumns(), is(new String[]{ "name" }));

        assertThat(node.insertColumns().length, is(2));
        Reference idRef = node.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = node.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(node.items().size(), is(1));
        SymbolBasedUpsertByIdNode.Item item = node.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("1"));
        assertThat(item.routing(), is("1"));

        assertThat(item.insertValues().length, is(2));
        assertThat((Long)item.insertValues()[0], is(1L));
        assertNull(item.insertValues()[1]);

        assertThat(item.updateAssignments().length, is(1));
        assertThat(item.updateAssignments()[0], isLiteral(null, DataTypes.STRING));
    }

    @Test
    public void testGroupByOnClusteredByColumnPartitionedOnePartition() throws Exception {
        // only one partition hit
        Plan optimizedPlan = plan("select count(*), city from clustered_parted where date=1395874800000 group by city");
        assertThat(optimizedPlan, instanceOf(NonDistributedGroupBy.class));
        NonDistributedGroupBy optimizedGroupBy = (NonDistributedGroupBy) optimizedPlan;

        assertThat(optimizedGroupBy.collectNode().isPartitioned(), is(true));
        assertThat(optimizedGroupBy.collectNode().projections().size(), is(1));
        assertThat(optimizedGroupBy.collectNode().projections().get(0), instanceOf(GroupProjection.class));

        assertThat(optimizedGroupBy.localMergeNode().projections().size(), is(1));
        assertThat(optimizedGroupBy.localMergeNode().projections().get(0), instanceOf(TopNProjection.class));

        // > 1 partition hit
        Plan plan = plan("select count(*), city from clustered_parted where date=1395874800000 or date=1395961200000 group by city");
        assertThat(plan, instanceOf(DistributedGroupBy.class));
    }

    @Test
    public void testIndices() throws Exception {
        TableIdent custom = new TableIdent("custom", "table");
        String[] indices = Planner.indices(TestingTableInfo.builder(custom, RowGranularity.DOC, shardRouting).add("id", DataTypes.INTEGER, null).build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("custom.table"));

        indices = Planner.indices(TestingTableInfo.builder(new TableIdent(null, "table"), RowGranularity.DOC, shardRouting).add("id", DataTypes.INTEGER, null).build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("table"));

        indices = Planner.indices(TestingTableInfo.builder(custom, RowGranularity.DOC, shardRouting)
                .add("id", DataTypes.INTEGER, null)
                .add("date", DataTypes.TIMESTAMP, null, true)
                .addPartitions(new PartitionName(custom, Arrays.asList(new BytesRef("0"))).stringValue())
                .addPartitions(new PartitionName(custom, Arrays.asList(new BytesRef("12345"))).stringValue())
                .build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("custom..partitioned.table.04130", "custom..partitioned.table.04332chj6gqg"));
    }

    @Test
    public void testAllocatedJobSearchContextIds() throws Exception {
        Planner.Context plannerContext = new Planner.Context(clusterService);
        CollectNode collectNode = new CollectNode(
                UUID.randomUUID(),
                plannerContext.nextExecutionNodeId(), "collect", shardRouting);

        int shardNum = collectNode.routing().numShards();

        plannerContext.allocateJobSearchContextIds(collectNode.routing());

        java.lang.reflect.Field f = plannerContext.getClass().getDeclaredField("jobSearchContextIdBaseSeq");
        f.setAccessible(true);
        int jobSearchContextIdBaseSeq = (Integer)f.get(plannerContext);

        assertThat(jobSearchContextIdBaseSeq, is(shardNum));
        assertThat(collectNode.routing().jobSearchContextIdBase(), is(jobSearchContextIdBaseSeq-shardNum));

        int idx = 0;
        for (Map.Entry<String, Map<String, List<Integer>>> locations : collectNode.routing().locations().entrySet()) {
            String nodeId = locations.getKey();
            for (Map.Entry<String, List<Integer>> entry : locations.getValue().entrySet()) {
                for (Integer shardId : entry.getValue()) {
                    assertThat(plannerContext.shardId(idx), is(new ShardId(entry.getKey(), shardId)));
                    assertThat(plannerContext.nodeId(idx), is(nodeId));
                    idx++;
                }
            }
        }

        // jobSearchContextIdBase must only set once on a Routing instance
        int jobSearchContextIdBase = collectNode.routing().jobSearchContextIdBase();
        plannerContext.allocateJobSearchContextIds(collectNode.routing());
        assertThat(collectNode.routing().jobSearchContextIdBase(), is(jobSearchContextIdBase));
    }

    @Test
    public void testExecutionNodeIdSequence() throws Exception {
        Planner.Context plannerContext = new Planner.Context(clusterService);
        CollectNode collectNode1 = new CollectNode(
                UUID.randomUUID(),
                plannerContext.nextExecutionNodeId(), "collect1", shardRouting);
        CollectNode collectNode2 = new CollectNode(
                UUID.randomUUID(),
                plannerContext.nextExecutionNodeId(), "collect2", shardRouting);

        assertThat(collectNode1.executionNodeId(), is(0));
        assertThat(collectNode2.executionNodeId(), is(1));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testLimitThatIsBiggerThanPageSizeCausesQTFPUshPlan() throws Exception {
        QueryThenFetch plan = (QueryThenFetch) plan("select * from users limit 2147483647 ");
        assertThat(plan.collectNode().downstreamNodes().size(), is(1));
        assertThat(plan.collectNode().downstreamNodes().get(0), is(LOCAL_NODE_ID));
        assertThat(plan.collectNode().hasDistributingDownstreams(), is(true));
    }

    @Test
    public void testKillPlan() throws Exception {
        Plan killPlan = plan("kill all");
        assertThat(killPlan, instanceOf(KillPlan.class));
    }
}
