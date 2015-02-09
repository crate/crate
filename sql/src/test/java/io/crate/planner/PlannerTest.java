package io.crate.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.Analyzer;
import io.crate.analyze.BaseAnalyzerTest;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
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
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.ddl.DropTableNode;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsNode;
import io.crate.planner.node.ddl.GenericDDLNode;
import io.crate.planner.node.dml.*;
import io.crate.planner.node.dql.*;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
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
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlannerTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Analyzer analyzer;
    private Planner planner;
    static final Routing shardRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());

    static final Routing nodesRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of())
            .put("nodeTwo", ImmutableMap.<String, Set<Integer>>of())
            .build());

    static final Routing partedRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of(".partitioned.parted.04232chj", ImmutableSet.of(1, 2)))
            .put("nodeTwo", ImmutableMap.<String, Set<Integer>>of())
            .build());


    public static TestModule plannerTestModule() {
        return new TestModule();
    }

    public static class TestModule extends MetaDataModule {

        @Override
        protected void configure() {
            ClusterService clusterService = mock(ClusterService.class);
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
                    .add("address", DataTypes.OBJECT, null)
                    .add("address", DataTypes.STRING, ImmutableList.of("street"))
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
                            new PartitionName("parted", new ArrayList<BytesRef>(){{add(null);}}).stringValue(),
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
            when(emptyPartedTableInfo.schemaInfo().name()).thenReturn(ReferenceInfos.DEFAULT_SCHEMA_NAME);
            when(schemaInfo.getTableInfo(charactersTableIdent.name())).thenReturn(charactersTableInfo);
            when(schemaInfo.getTableInfo(userTableIdent.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(partedTableIdent.name())).thenReturn(partedTableInfo);
            when(schemaInfo.getTableInfo(emptyPartedTableIdent.name())).thenReturn(emptyPartedTableInfo);
            when(schemaInfo.getTableInfo(multiplePartitionedTableIdent.name())).thenReturn(multiplePartitionedTableInfo);
            when(schemaInfo.getTableInfo(BaseAnalyzerTest.IGNORED_NESTED_TABLE_IDENT.name())).thenReturn(BaseAnalyzerTest.IGNORED_NESTED_TABLE_INFO);
            schemaBinder.addBinding(ReferenceInfos.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
            schemaBinder.addBinding(SysSchemaInfo.NAME).toInstance(mockSysSchemaInfo());
            schemaBinder.addBinding(BlobSchemaInfo.NAME).toInstance(mockBlobSchemaInfo());
        }

        private SchemaInfo mockBlobSchemaInfo(){
            BlobSchemaInfo blobSchemaInfo = mock(BlobSchemaInfo.class);
            when(blobSchemaInfo.getTableInfo("screenshots")).thenReturn(mock(BlobTableInfo.class));
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

    @Before
    public void setUp() throws Exception {
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

    private Plan plan(String statement) {
        return planner.plan(analyzer.analyze(SqlParser.createStatement(statement)));
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
        assertThat(collectNode.downStreamNodes().size(), is(2));
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
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));

        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) mergeNode.projections().get(0);
        InputColumn inputColumn = (InputColumn) groupProjection.values().get(0).inputs().get(0);
        assertThat(inputColumn.index(), is(1));

        assertThat(mergeNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, mergeNode.outputTypes().get(0));
        assertEquals(DataTypes.LONG, mergeNode.outputTypes().get(1));

        MergeNode localMerge = distributedGroupBy.localMergeNode();

        assertThat(localMerge.numUpstreams(), is(2));
        assertTrue(localMerge.executionNodes().isEmpty());
        assertEquals(mergeNode.outputTypes(), localMerge.inputTypes());

        assertThat(localMerge.projections().get(0), instanceOf(TopNProjection.class));
        TopNProjection topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(topN.outputs().size(), is(2));

        // groupProjection changes output to  keys, aggregations
        // topN needs to swap the outputs back
        assertThat(topN.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(1));
        assertThat(topN.outputs().get(1), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(0));
    }

    @Test
    public void testGetPlan() throws Exception {
        IterablePlan plan = (IterablePlan)  plan("select name from users where id = 1");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode) iterator.next();
        assertThat(node.index(), is("users"));
        assertThat(node.ids().get(0), is("1"));
        assertFalse(iterator.hasNext());
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
        assertThat(node.index(), is("characters"));
        assertThat(node.ids().get(0), is("one"));
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
        assertThat(getNode.index(),
                is(new PartitionName("parted", Arrays.asList(new BytesRef("0"))).stringValue()));
        assertEquals(DataTypes.STRING, getNode.outputTypes().get(0));
        assertEquals(DataTypes.TIMESTAMP, getNode.outputTypes().get(1));
    }

    @Test
    public void testMultiGetPlan() throws Exception {
        IterablePlan plan = (IterablePlan) plan("select name from users where id in (1, 2)");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode) iterator.next();
        assertThat(node.index(), is("users"));
        assertThat(node.ids().size(), is(2));
        assertThat(node.ids().get(0), is("1"));
        assertThat(node.ids().get(1), is("2"));
    }

    @Test
    public void testDeletePlan() throws Exception {
        IterablePlan plan = (IterablePlan) plan("delete from users where id = 1");
        Iterator<PlanNode> iterator = plan.iterator();
        ESDeleteNode node = (ESDeleteNode) iterator.next();
        assertThat(node.index(), is("users"));
        assertThat(node.id(), is("1"));
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
        assertThat(topN.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(1));
        assertThat(topN.outputs().get(1), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(0));


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
    public void testGlobalAggregationVersion() throws Exception {
        expectedException.expect(VersionInvalidException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause of a SELECT statement");
        plan("select count(name) from users where _version=1");
    }

    @Test
    public void testGroupByOnNodeLevel() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select count(*), name from sys.nodes group by name");
        CollectNode collectNode = planNode.collectNode();
        assertFalse(collectNode.hasDownstreams());
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
    public void testESSearchPlan() throws Exception {
        IterablePlan plan = (IterablePlan) plan("select name from users where name = 'x' order by id limit 10");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryThenFetchNode.class));
        QueryThenFetchNode searchNode = (QueryThenFetchNode) planNode;

        assertThat(searchNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, searchNode.outputTypes().get(0));
        assertTrue(searchNode.whereClause().hasQuery());
        assertThat(searchNode.partitionBy().size(), is(0));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testESSEarchPlanWithVersion() throws Exception {
        expectedException.expect(VersionInvalidException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause of a SELECT statement");
        plan("select name from users where name = 'x' and _version = 1");
    }

    @Test
    public void testESSearchPlanPartitioned() throws Exception {
        IterablePlan plan = (IterablePlan) plan("select id, name, date from parted where date > 0 and name = 'x' order by id limit 10");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryThenFetchNode.class));
        QueryThenFetchNode searchNode = (QueryThenFetchNode) planNode;

        List<String> indices = new ArrayList<>();
        Map<String, Map<String, Set<Integer>>> locations = searchNode.routing().locations();
        for (Map.Entry<String, Map<String, Set<Integer>>> entry : locations.entrySet()) {
            indices.addAll(entry.getValue().keySet());
        }
        assertThat(indices, Matchers.contains(
                new PartitionName("parted", Arrays.asList(new BytesRef("123"))).stringValue()));
        assertThat(searchNode.outputTypes().size(), is(3));
        assertTrue(searchNode.whereClause().hasQuery());
        assertThat(searchNode.partitionBy().size(), is(1));
        assertThat(searchNode.partitionBy().get(0).ident().columnIdent().fqn(), is("date"));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testESSearchPlanFunction() throws Exception {
        IterablePlan plan = (IterablePlan) plan("select format('Hi, my name is %s', name), name from users where name = 'x' order by id limit 10");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryThenFetchNode.class));
        QueryThenFetchNode searchNode = (QueryThenFetchNode) planNode;

        assertThat(searchNode.outputs().size(), is(2));
        assertThat(searchNode.outputs().get(0), isFunction("format"));
        assertThat(searchNode.outputs().get(1), isReference("name"));

        assertThat(searchNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, searchNode.outputTypes().get(0));
        assertEquals(DataTypes.STRING, searchNode.outputTypes().get(1));
        assertTrue(searchNode.whereClause().hasQuery());
        assertThat(searchNode.partitionBy().size(), is(0));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testInsertPlan() throws Exception {
        Upsert plan = (Upsert) plan("insert into users (id, name) values (42, 'Deep Thought')");

        assertThat(plan.nodes().size(), is(1));

        List<DQLPlanNode> childNodes = plan.nodes().get(0);
        assertThat(childNodes.size(), is(1));
        assertThat(childNodes.get(0), instanceOf(UpsertByIdNode.class));

        UpsertByIdNode updateNode = (UpsertByIdNode)childNodes.get(0);

        assertThat(updateNode.insertColumns().length, is(2));
        Reference idRef = updateNode.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = updateNode.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(updateNode.items().size(), is(1));
        UpsertByIdNode.Item item = updateNode.items().get(0);
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

        List<DQLPlanNode> childNodes = plan.nodes().get(0);
        assertThat(childNodes.size(), is(1));
        assertThat(childNodes.get(0), instanceOf(UpsertByIdNode.class));

        UpsertByIdNode updateNode = (UpsertByIdNode)childNodes.get(0);

        assertThat(updateNode.insertColumns().length, is(2));
        Reference idRef = updateNode.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = updateNode.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(updateNode.items().size(), is(2));

        UpsertByIdNode.Item item1 = updateNode.items().get(0);
        assertThat(item1.index(), is("users"));
        assertThat(item1.id(), is("42"));
        assertThat(item1.routing(), is("42"));
        assertThat(item1.insertValues().length, is(2));
        assertThat((Long)item1.insertValues()[0], is(42L));
        assertThat((BytesRef)item1.insertValues()[1], is(new BytesRef("Deep Thought")));

        UpsertByIdNode.Item item2 = updateNode.items().get(1);
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
        assertThat(((Reference)collectNode.toCollect().get(0)).info().ident().columnIdent().name(), is("name"));

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
        assertNull(collectNode.downStreamNodes());
        assertThat(collectNode.projections().size(), is(2));
        assertThat(collectNode.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(collectNode.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));
    }

    @Test
    public void testNoDistributedGroupByOnAllPrimaryKeys() throws Exception {
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "select count(*), id, date from empty_parted group by id, date limit 20");
        CollectNode collectNode = planNode.collectNode();
        assertNull(collectNode.downStreamNodes());
        assertThat(collectNode.projections().size(), is(2));
        assertThat(collectNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectNode.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(collectNode.projections().get(1), instanceOf(TopNProjection.class));
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

        // points to the first values() entry of the previous GroupProjection
        assertThat(((InputColumn) orderBy).index(), is(1));
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
        assertThat(((Reference)collectNode.toCollect().get(1)).info().ident().columnIdent().name(), is("id"));
        assertThat(((Reference)collectNode.toCollect().get(0)).info().ident().columnIdent().name(), is("name"));
        Projection projection = collectNode.projections().get(0);
        assertThat(projection, instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection)projection;
        Symbol groupKey = groupProjection.keys().get(0);
        assertThat(groupKey, instanceOf(InputColumn.class));
        assertThat(((InputColumn)groupKey).index(), is(0));
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

        List<DQLPlanNode> childNodes = plan.nodes().get(0);

        PlanNode planNode= childNodes.get(0);

        assertThat(planNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.LONG, planNode.outputTypes().get(0));

        assertThat(childNodes.size(), is(2));
        assertThat(childNodes.get(0), instanceOf(CollectNode.class));
        assertThat(childNodes.get(1), instanceOf(MergeNode.class));

        CollectNode collectNode = (CollectNode)childNodes.get(0);
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

        MergeNode mergeNode = (MergeNode)childNodes.get(1);
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(AggregationProjection.class));
    }

    @Test
    public void testUpdateByIdPlan() throws Exception {
        Upsert planNode = (Upsert) plan("update users set name='Vogon lyric fan' where id=1");
        assertThat(planNode.nodes().size(), is(1));

        List<DQLPlanNode> childNodes = planNode.nodes().get(0);
        assertThat(childNodes.size(), is(1));
        assertThat(childNodes.get(0), instanceOf(UpsertByIdNode.class));

        UpsertByIdNode updateNode = (UpsertByIdNode)childNodes.get(0);
        assertThat(updateNode.items().size(), is(1));

        assertThat(updateNode.updateColumns()[0], is("name"));

        UpsertByIdNode.Item item = updateNode.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("1"));

        Symbol symbol = item.updateAssignments()[0];
        assertThat(symbol, isLiteral("Vogon lyric fan", DataTypes.STRING));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValues() throws Exception {
        Upsert planNode =  (Upsert) plan("update users set name='Vogon lyric fan' where id in (1,2,3)");;
        assertThat(planNode.nodes().size(), is(1));

        List<DQLPlanNode> childNodes = planNode.nodes().get(0);
        assertThat(childNodes.size(), is(1));

        assertThat(childNodes.get(0), instanceOf(UpsertByIdNode.class));
        UpsertByIdNode updateNode = (UpsertByIdNode)childNodes.get(0);

        List<String> ids = new ArrayList<>(3);
        for (UpsertByIdNode.Item item : updateNode.items()) {
            ids.add(item.id());
            assertThat(item.updateAssignments().length, is(1));
            assertThat(item.updateAssignments()[0], isLiteral("Vogon lyric fan", DataTypes.STRING));
        }

        assertThat(ids, containsInAnyOrder("1", "2", "3"));
    }

    @Test
    public void testCopyFromPlan() throws Exception {
        IterablePlan plan = (IterablePlan) plan("copy users from '/path/to/file.extension'");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(FileUriCollectNode.class));

        FileUriCollectNode collectNode = (FileUriCollectNode)planNode;
        assertThat((BytesRef) ((Literal) collectNode.targetUri()).value(),
                is(new BytesRef("/path/to/file.extension")));
    }

    @Test
    public void testCopyFromNumReadersSetting() throws Exception {
        IterablePlan plan = (IterablePlan) plan("copy users from '/path/to/file.extension' with (num_readers=1)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(FileUriCollectNode.class));
        FileUriCollectNode collectNode = (FileUriCollectNode)planNode;
        assertThat(collectNode.executionNodes().size(), is(1));
    }

    @Test
    public void testCopyFromPlanWithParameters() throws Exception {
        IterablePlan plan = (IterablePlan) plan("copy users from '/path/to/file.ext' with (bulk_size=30, compression='gzip', shared=true)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(FileUriCollectNode.class));
        FileUriCollectNode collectNode = (FileUriCollectNode)planNode;
        SourceIndexWriterProjection indexWriterProjection = (SourceIndexWriterProjection) collectNode.projections().get(0);
        assertThat(indexWriterProjection.bulkActions(), is(30));
        assertThat(collectNode.compression(), is("gzip"));
        assertThat(collectNode.sharedStorage(), is(true));

        // verify defaults:
        plan = (IterablePlan) plan("copy users from '/path/to/file.ext'");
        iterator = plan.iterator();
        collectNode = (FileUriCollectNode)iterator.next();
        assertNull(collectNode.compression());
        assertNull(collectNode.sharedStorage());
    }

    @Test
    public void testCopyToWithColumnsReferenceRewrite() throws Exception {
        IterablePlan plan = (IterablePlan) plan("copy users (name) to '/file.ext'");
        CollectNode node = (CollectNode)plan.iterator().next();
        Reference nameRef = (Reference)node.toCollect().get(0);

        assertThat(nameRef.info().ident().columnIdent().name(), is(DocSysColumns.DOC.name()));
        assertThat(nameRef.info().ident().columnIdent().path().get(0), is("name"));
    }

    @Test
    public void testCopyToWithNonExistentPartitionClause() throws Exception {
        IterablePlan plan = (IterablePlan) plan("copy parted partition (date=0) to '/foo.txt' ");
        CollectNode collectNode = (CollectNode) plan.iterator().next();
        assertFalse(collectNode.routing().hasLocations());
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
        IterablePlan plan = (IterablePlan) plan("select count(*) from users");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(PlannedAnalyzedRelation.class));
        assertThat(planNode, instanceOf(ESCountNode.class));

        ESCountNode node = (ESCountNode) planNode;
        assertThat(node.indices().length, is(1));
        assertThat(node.indices()[0], is("users"));
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
        NonDistributedGroupBy planNode = (NonDistributedGroupBy) plan(
                "insert into users (id, name) (select name, count(*) from sys.nodes where name='Ford' group by name)");

        MergeNode mergeNode = planNode.localMergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
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
        DistributedGroupBy planNode = (DistributedGroupBy) plan(
                "insert into users (id, name) (select name, count(*) from users group by name)");

        MergeNode mergeNode = planNode.reducerMergeNode();
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
        assertThat(projection.tableName(), is("users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));

        MergeNode localMergeNode = planNode.localMergeNode();
        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByPartitioned() throws Exception {
        DistributedGroupBy planNode = (DistributedGroupBy) plan(
                "insert into parted (id, date) (select id, date from users group by id, date)");

        MergeNode mergeNode = planNode.reducerMergeNode();
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
        assertThat(projection.tableName(), is("parted"));

        MergeNode localMergeNode = planNode.localMergeNode();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

    }

    @Test
    public void testInsertFromSubQueryGlobalAggregate() throws Exception {
        GlobalAggregate planNode = (GlobalAggregate) plan("insert into users (name, id) (select arbitrary(name), count(*) from users)");

        MergeNode mergeNode = planNode.mergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)mergeNode.projections().get(1);

        assertThat(projection.columnReferences().size(), is(2));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("name"));
        assertThat(projection.columnReferences().get(1).ident().columnIdent().fqn(), is("id"));

        assertThat(projection.columnSymbols().size(), is(2));
        assertThat(((InputColumn)projection.columnSymbols().get(0)).index(), is(0));
        assertThat(((InputColumn)projection.columnSymbols().get(1)).index(), is(1));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableName(), is("users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));
    }

    @Test
    public void testInsertFromSubQueryESGet() throws Exception {
        // doesn't use ESGetNode but CollectNode.
        // Round-trip to handler can be skipped by writing from the shards directly
        QueryAndFetch planNode = (QueryAndFetch) plan(
                "insert into users (date, id, name) (select date, id, name from users where id=1)");;
        CollectNode collectNode = planNode.collectNode();

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

        MergeNode mergeNode = planNode.localMergeNode();
    }

    @Test (expected = UnsupportedFeatureException.class)
    public void testInsertFromSubQueryWithLimit() throws Exception {
        IterablePlan plan = (IterablePlan) plan("insert into users (date, id, name) (select date, id, name from users limit 10)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryThenFetchNode.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode localMergeNode = (MergeNode)planNode;

        assertThat(localMergeNode.projections().size(), is(2));
        assertThat(localMergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
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
        QueryAndFetch planNode = (QueryAndFetch) plan(
                "insert into users (date, id, name) (select date, id, name from users)");
        CollectNode collectNode = planNode.collectNode();
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(ColumnIndexWriterProjection.class));

        MergeNode localMergeNode = planNode.localMergeNode();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
    }

    @Test
    public void testInsertFromSubQueryWithVersion() throws Exception {
        expectedException.expect(VersionInvalidException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause of a SELECT statement");
        plan("insert into users (date, id, name) (select date, id, name from users where _version = 1)");
    }

    @Test
    public void testGroupByHaving() throws Exception {
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) plan(
                "select avg(date), name from users group by name having min(date) > '1970-01-01'");
        CollectNode collectNode = distributedGroupBy.collectNode();
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(GroupProjection.class));

        MergeNode mergeNode = distributedGroupBy.reducerMergeNode();

        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(FilterProjection.class));

        GroupProjection groupProjection = (GroupProjection)mergeNode.projections().get(0);
        assertThat(groupProjection.values().size(), is(2));

        FilterProjection filterProjection = (FilterProjection)mergeNode.projections().get(1);
        assertThat(filterProjection.outputs().size(), is(2));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn)filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        assertThat(filterProjection.outputs().get(1), instanceOf(InputColumn.class));
        inputColumn = (InputColumn)filterProjection.outputs().get(1);
        assertThat(inputColumn.index(), is(1));
    }

    @Test
    public void testInsertFromQueryWithPartitionedColumn() throws Exception {
        QueryAndFetch planNode = (QueryAndFetch) plan(
                "insert into users (id, date) (select id, date from parted)");

        CollectNode collectNode = planNode.collectNode();
        List<Symbol> toCollect = collectNode.toCollect();
        assertThat(toCollect.size(), is(2));
        assertThat(toCollect.get(0), isFunction("toLong"));
        assertThat(((Function) toCollect.get(0)).arguments().get(0), isReference("_doc['id']"));
        assertThat((Reference) toCollect.get(1), equalTo(new Reference(new ReferenceInfo(
            new ReferenceIdent(new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "parted"), "date"), RowGranularity.PARTITION, DataTypes.TIMESTAMP))));
    }

    @Test
    public void testGroupByHavingInsertInto() throws Exception {
        DistributedGroupBy planNode = (DistributedGroupBy) plan(
                "insert into users (id, name) (select name, count(*) from users group by name having count(*) > 3)");
        MergeNode mergeNode = planNode.reducerMergeNode();
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
        MergeNode localMergeNode = planNode.localMergeNode();

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
        assertThat(filterProjection.outputs().size(), is(1));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn)filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        TopNProjection topNProjection = (TopNProjection)localMergeNode.projections().get(2);
        assertThat(topNProjection.outputs().size(), is(1));
    }

    @Test
    public void testCountOnPartitionedTable() throws Exception {
        IterablePlan plan = (IterablePlan) plan("select count(*) from parted");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(PlannedAnalyzedRelation.class));
        assertThat(planNode, instanceOf(ESCountNode.class));
        ESCountNode esCountNode = (ESCountNode) planNode;

        assertThat(esCountNode.indices(), arrayContainingInAnyOrder(
                ".partitioned.parted.0400", ".partitioned.parted.04130", ".partitioned.parted.04232chj"));
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

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)
        // topN projection
        //      outputs: count(*), name     -> swaps symbols to match original selectList

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        // outputs: count(*), name
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(2);
        // swapped!
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(1));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(0));


        MergeNode localMerge = planNode.localMergeNode();

        // topN projection
        //      outputs: count(*), name
        topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));
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

        // filter projection - can't reorder here
        //      outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        assertThat(mergeNode.outputTypes().get(0), equalTo((DataType) DataTypes.STRING));
        assertThat(mergeNode.outputTypes().get(1), equalTo((DataType) DataTypes.LONG));

        MergeNode localMerge = planNode.localMergeNode();

        // topN projection
        //      outputs: name, count(*)
        TopNProjection topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(1));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(0));
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
        assertThat(plan.nodes().size(), is(1));
        assertThat(plan.nodes().get(0).size(), is(1));
        assertThat(plan.nodes().get(0).get(0), instanceOf(UpsertByIdNode.class));
        UpsertByIdNode node = (UpsertByIdNode) plan.nodes().get(0).get(0);

        assertThat(node.updateColumns(), is(new String[]{ "name" }));

        assertThat(node.insertColumns().length, is(2));
        Reference idRef = node.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = node.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(node.items().size(), is(1));
        UpsertByIdNode.Item item = node.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("1"));
        assertThat(item.routing(), is("1"));

        assertThat(item.insertValues().length, is(2));
        assertThat((Long)item.insertValues()[0], is(1L));
        assertNull(item.insertValues()[1]);

        assertThat(item.updateAssignments().length, is(1));
        assertThat(item.updateAssignments()[0], isLiteral(null, DataTypes.STRING));
    }
}
