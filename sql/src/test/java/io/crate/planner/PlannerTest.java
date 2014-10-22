package io.crate.planner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.PartitionName;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.ddl.DropTableNode;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsNode;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.planner.node.dql.*;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

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

    private Injector injector;
    private Analyzer analyzer;
    private Planner planner;
    private TableInfo charactersTableInfo;
    private TableInfo userTableInfo;
    private ReferenceInfos referenceInfos;
    private ReferenceResolver resolver;
    private Functions functions;

    Routing shardRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());

    Routing nodesRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of())
            .put("nodeTwo", ImmutableMap.<String, Set<Integer>>of())
            .build());

    final Routing partedRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of(".partitioned.parted.04232chj", ImmutableSet.of(1, 2)))
            .put("nodeTwo", ImmutableMap.<String, Set<Integer>>of())
            .build());

    class TestModule extends MetaDataModule {

        @Override
        protected void configure() {
            ClusterService clusterService = mock(ClusterService.class);
            ClusterState clusterState = mock(ClusterState.class);
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
            super.configure();
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            TableIdent userTableIdent = new TableIdent(null, "users");
            userTableInfo = TestingTableInfo.builder(userTableIdent, RowGranularity.DOC, shardRouting)
                    .add("name", DataTypes.STRING, null)
                    .add("id", DataTypes.LONG, null)
                    .add("date", DataTypes.TIMESTAMP, null)
                    .addPrimaryKey("id")
                    .clusteredBy("id")
                    .build();
            TableIdent charactersTableIdent = new TableIdent(null, "characters");
            charactersTableInfo = TestingTableInfo.builder(charactersTableIdent, RowGranularity.DOC, shardRouting)
                    .add("name", DataTypes.STRING, null)
                    .add("id", DataTypes.STRING, null)
                    .addPrimaryKey("id")
                    .clusteredBy("id")
                    .build();
            TableIdent partedTableIdent = new TableIdent(null, "parted");
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
            TableIdent emptyPartedTableIdent = new TableIdent(null, "empty_parted");
            TableInfo emptyPartedTableInfo = TestingTableInfo.builder(partedTableIdent, RowGranularity.DOC, shardRouting)
                    .add("name", DataTypes.STRING, null)
                    .add("id", DataTypes.STRING, null)
                    .add("date", DataTypes.TIMESTAMP, null, true)
                    .addPrimaryKey("id")
                    .addPrimaryKey("date")
                    .clusteredBy("id")
                    .build();
            when(schemaInfo.getTableInfo(charactersTableIdent.name())).thenReturn(charactersTableInfo);
            when(schemaInfo.getTableInfo(userTableIdent.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(partedTableIdent.name())).thenReturn(partedTableInfo);
            when(schemaInfo.getTableInfo(emptyPartedTableIdent.name())).thenReturn(emptyPartedTableInfo);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
            schemaBinder.addBinding(SysSchemaInfo.NAME).toInstance(mockSysSchemaInfo());
        }

        private SchemaInfo mockSysSchemaInfo() {
            SchemaInfo schemaInfo = mock(SchemaInfo.class);

            TableInfo sysClusterTableInfo = TestingTableInfo.builder(
                    SysClusterTableInfo.IDENT,
                    // granularity < DOC is already handled different
                    // here we want a table with handlerSideRouting and DOC granularity.
                    RowGranularity.DOC,
                    SysClusterTableInfo.ROUTING
            ).add("name", DataTypes.STRING, null).build();
            when(schemaInfo.getTableInfo(sysClusterTableInfo.ident().name())).thenReturn(sysClusterTableInfo);

            TableInfo sysNodesTableInfo = TestingTableInfo.builder(
                    SysNodesTableInfo.IDENT,
                    RowGranularity.NODE,
                    nodesRouting)
                    .add("name", DataTypes.STRING, null).build();
            when(schemaInfo.getTableInfo(sysNodesTableInfo.ident().name())).thenReturn(sysNodesTableInfo);

            TableInfo sysShardsTableInfo = TestingTableInfo.builder(
                    SysShardsTableInfo.IDENT,
                    RowGranularity.SHARD,
                    nodesRouting
            ).add("id", DataTypes.INTEGER, null).build();
            when(schemaInfo.getTableInfo(sysShardsTableInfo.ident().name())).thenReturn(sysShardsTableInfo);
            when(schemaInfo.systemSchema()).thenReturn(true);
            return schemaInfo;
        }
    }

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder()
                .add(new TestModule())
                .add(new AggregationImplModule())
                .add(new ScalarFunctionModule())
                .add(new OperatorModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);

        referenceInfos = injector.getInstance(ReferenceInfos.class);
        resolver = injector.getInstance(ReferenceResolver.class);
        functions = injector.getInstance(Functions.class);
    }

    private Plan plan(String statement) {
        return planner.plan(analyzer.analyze(SqlParser.createStatement(statement)));
    }

    @Test
    public void testGroupByWithAggregationStringLiteralArguments() {
        Plan plan = plan("select count('foo'), name from users group by name");
        Iterator<PlanNode> iterator = plan.iterator();
        QueryAndFetchNode collectNode = (QueryAndFetchNode) iterator.next();
        // TODO: optimize to not queryAndFetch literal
        //assertThat(collectNode.toCollect().size(), is(1));
        GroupProjection groupProjection = (GroupProjection) collectNode.collectorProjections().get(0);
        Aggregation aggregation = groupProjection.values().get(0);
        //assertTrue(aggregation.inputs().get(0).symbolType().isValueSymbol());
    }

    @Test
    public void testGroupByWithAggregationPlan() throws Exception {
        Plan plan = plan("select count(*), name from users group by name");
        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));

        Iterator<PlanNode> iterator = plan.iterator();

        PlanNode planNode = iterator.next();
        // distributed queryAndFetch
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode qafNode = (QueryAndFetchNode) planNode;
        assertThat(qafNode.downStreamNodes().size(), is(2));
        assertThat(qafNode.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(qafNode.executionNodes().size(), is(2));
        assertThat(qafNode.toCollect().size(), is(1));
        assertThat(qafNode.collectorProjections().size(), is(1));
        assertThat(qafNode.collectorProjections().get(0), instanceOf(GroupProjection.class));
        assertThat(qafNode.projections().size(), is(0));

        assertThat(qafNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, qafNode.outputTypes().get(0));
        assertEquals(DataTypes.UNDEFINED, qafNode.outputTypes().get(1));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode) planNode;

        assertThat(mergeNode.numUpstreams(), is(2));
        assertThat(mergeNode.executionNodes().size(), is(2));
        assertEquals(mergeNode.inputTypes(), qafNode.outputTypes());
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));

        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) mergeNode.projections().get(0);
        InputColumn inputColumn = (InputColumn) groupProjection.values().get(0).inputs().get(0);
        assertThat(inputColumn.index(), is(1));

        assertThat(mergeNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, mergeNode.outputTypes().get(0));
        assertEquals(DataTypes.LONG, mergeNode.outputTypes().get(1));


        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));

        MergeNode localMerge = (MergeNode) planNode;

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

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testGetPlan() throws Exception {
        Plan plan = plan("select name from users where id = 1");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode) iterator.next();
        assertThat(node.index(), is("users"));
        assertThat(node.ids().get(0), is("1"));
        assertFalse(iterator.hasNext());
        assertThat(node.outputs().size(), is(1));
    }

    @Test
    public void testGetPlanStringLiteral() throws Exception {
        Plan plan = plan("select name from characters where id = 'one'");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode) iterator.next();
        assertThat(node.index(), is("characters"));
        assertThat(node.ids().get(0), is("one"));
        assertFalse(iterator.hasNext());
        assertThat(node.outputs().size(), is(1));
    }

    @Test
    public void testGetPlanPartitioned() throws Exception {
        Plan plan = plan("select name, date from parted where id = 'one' and date = 0");
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
        Plan plan = plan("select name from users where id in (1, 2)");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode) iterator.next();
        assertThat(node.index(), is("users"));
        assertThat(node.ids().size(), is(2));
        assertThat(node.ids().get(0), is("1"));
        assertThat(node.ids().get(1), is("2"));
    }

    @Test
    public void testDeletePlan() throws Exception {
        Plan plan = plan("delete from users where id = 1");
        Iterator<PlanNode> iterator = plan.iterator();
        ESDeleteNode node = (ESDeleteNode) iterator.next();
        assertThat(node.index(), is("users"));
        assertThat(node.id(), is("1"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMultiDeletePlan() throws Exception {
        Plan plan = plan("delete from users where id in (1, 2)");
        Iterator<PlanNode> iterator = plan.iterator();
        assertThat(iterator.next(), instanceOf(ESDeleteByQueryNode.class));
    }

    @Test
    public void testGroupByWithAggregationAndLimit() throws Exception {
        Plan plan = plan("select count(*), name from users group by name limit 1 offset 1");
        Iterator<PlanNode> iterator = plan.iterator();

        PlanNode planNode = iterator.next();
        planNode = iterator.next();

        // distributed merge
        MergeNode mergeNode = (MergeNode) planNode;
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
        DQLPlanNode dqlPlanNode = (DQLPlanNode)iterator.next();
        assertThat(dqlPlanNode.projections().get(0), instanceOf(TopNProjection.class));
        topN = (TopNProjection) dqlPlanNode.projections().get(0);
        assertThat(topN.limit(), is(1));
        assertThat(topN.offset(), is(1));
        assertThat(topN.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(topN.outputs().get(1), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testGlobalAggregationPlan() throws Exception {
        String statementString = "select count(name) from users";
        Statement statement = SqlParser.createStatement(statementString);

        Analysis analysis = analyzer.analyze(statement);
        Plan plan = planner.plan(analysis);
        Iterator<PlanNode> iterator = plan.iterator();

        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode qafNode = (QueryAndFetchNode) planNode;

        assertEquals(DataTypes.UNDEFINED, qafNode.intermediateOutputTypes().get(0));
        assertEquals(DataTypes.LONG, qafNode.outputTypes().get(0));
        assertThat(qafNode.maxRowGranularity(), is(RowGranularity.DOC));

        assertThat(qafNode.collectorProjections().size(), is(1));
        assertThat(qafNode.collectorProjections().get(0), instanceOf(AggregationProjection.class));

        assertThat(qafNode.projections().size(), is(1));
        assertThat(qafNode.projections().get(0), instanceOf(AggregationProjection.class));

        assertThat(iterator.hasNext(), is(false));

        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testGroupByOnNodeLevel() throws Exception {
        Plan plan = plan("select count(*), name from sys.nodes group by name");

        Iterator<PlanNode> iterator = plan.iterator();

        QueryAndFetchNode collectNode = (QueryAndFetchNode) iterator.next();
        assertFalse(collectNode.hasDownstreams());
        assertEquals(DataTypes.STRING, collectNode.outputTypes().get(0));
        assertEquals(DataTypes.UNDEFINED, collectNode.outputTypes().get(1));

        MergeNode mergeNode = (MergeNode) iterator.next();
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

        assertFalse(iterator.hasNext());

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testShardPlan() throws Exception {
        Plan plan = plan("select id from sys.shards order by id limit 10");
        // TODO: add where clause

        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode collectNode = (QueryAndFetchNode) planNode;

        assertEquals(DataTypes.INTEGER, collectNode.outputTypes().get(0));
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.SHARD));

        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testESSearchPlan() throws Exception {
        Plan plan = plan("select name from users where name = 'x' order by id limit 10");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryThenFetchNode.class));
        QueryThenFetchNode searchNode = (QueryThenFetchNode) planNode;

        assertThat(searchNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, searchNode.outputTypes().get(0));
        assertTrue(searchNode.whereClause().hasQuery());
        assertThat(searchNode.partitionBy().size(), is(0));

        assertFalse(iterator.hasNext());
        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testESSearchPlanPartitioned() throws Exception {
        Plan plan = plan("select id, name, date from parted where date > 0 and name = 'x' order by id limit 10");
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
        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testESSearchPlanFunction() throws Exception {
        Plan plan = plan("select format('Hi, my name is %s', name), name from users where name = 'x' order by id limit 10");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryThenFetchNode.class));
        QueryThenFetchNode searchNode = (QueryThenFetchNode) planNode;

        assertThat(searchNode.outputs().size(), is(1));
        assertThat(((Reference) searchNode.outputs().get(0)).info().ident().columnIdent().fqn(), is("name"));

        assertThat(searchNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, searchNode.outputTypes().get(0));
        assertEquals(DataTypes.STRING, searchNode.outputTypes().get(1));
        assertTrue(searchNode.whereClause().hasQuery());
        assertThat(searchNode.partitionBy().size(), is(0));

        assertThat(searchNode.projections().get(0), instanceOf(TopNProjection.class));
        assertThat(searchNode.projections().get(0).outputs().get(1), instanceOf(InputColumn.class));

        assertFalse(iterator.hasNext());
        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testESIndexPlan() throws Exception {
        Plan plan = plan("insert into users (id, name) values (42, 'Deep Thought')");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESIndexNode.class));

        ESIndexNode indexNode = (ESIndexNode) planNode;
        assertThat(indexNode.sourceMaps().size(), is(1));
        Map<String, Object> values = XContentHelper.convertToMap(indexNode.sourceMaps().get(0), false).v2();
        assertThat(values.size(), is(2));

        assertThat(values.keySet(), contains("id", "name"));

        assertThat((Integer) values.get("id"), is(42));
        assertThat((String) values.get("name"), is("Deep Thought"));

        assertThat(indexNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.LONG, indexNode.outputTypes().get(0));

        assertTrue(plan.expectsAffectedRows());
    }

    @Test
    public void testESIndexPlanMultipleValues() throws Exception {
        Plan plan = plan("insert into users (id, name) values (42, 'Deep Thought'), (99, 'Marvin')");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESIndexNode.class));

        ESIndexNode indexNode = (ESIndexNode) planNode;

        Map<String, Object> values0 = XContentHelper.convertToMap(indexNode.sourceMaps().get(0), false).v2();
        assertThat(indexNode.sourceMaps().size(), is(2));
        assertThat(values0.size(), is(2));

        Map<String, Object> values1 = XContentHelper.convertToMap(indexNode.sourceMaps().get(1), false).v2();
        assertThat(values1.size(), is(2));

        assertThat((Integer) values0.get("id"), is(42));
        assertThat((String) values0.get("name"), is("Deep Thought"));

        assertThat((Integer) values1.get("id"), is(99));
        assertThat((String) values1.get("name"), is("Marvin"));

        assertThat(indexNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.LONG, indexNode.outputTypes().get(0));

        assertTrue(plan.expectsAffectedRows());
    }

    @Test
    public void testCountDistinctPlan() throws Exception {
        Plan plan = plan("select count(distinct name) from users");
        Iterator<PlanNode> iterator = plan.iterator();
        QueryAndFetchNode qafNode = (QueryAndFetchNode)iterator.next();
        Projection projection = qafNode.collectorProjections().get(0);
        assertThat(projection, instanceOf(AggregationProjection.class));
        AggregationProjection aggregationProjection = (AggregationProjection)projection;
        assertThat(aggregationProjection.aggregations().size(), is(1));

        Aggregation aggregation = aggregationProjection.aggregations().get(0);
        assertThat(aggregation.toStep(), is(Aggregation.Step.PARTIAL));
        Symbol aggregationInput = aggregation.inputs().get(0);
        assertThat(aggregationInput.symbolType(), is(SymbolType.INPUT_COLUMN));

        assertThat(qafNode.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference)qafNode.toCollect().get(0)).info().ident().columnIdent().name(), is("name"));

        assertThat(qafNode.projections().size(), is(2));
        Projection projection1 = qafNode.projections().get(1);
        assertThat(projection1, instanceOf(TopNProjection.class));
        Symbol collection_count = projection1.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testNoDistributedGroupByOnClusteredColumn() throws Exception {
        Plan plan = plan("select count(*), id from users group by id limit 20");
        Iterator<PlanNode> iterator = plan.iterator();
        QueryAndFetchNode qafNode = (QueryAndFetchNode)iterator.next();
        assertNull(qafNode.downStreamNodes());
        assertThat(qafNode.collectorProjections().size(), is(2));
        assertThat(qafNode.collectorProjections().get(1), instanceOf(TopNProjection.class));
        assertThat(qafNode.collectorProjections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(qafNode.projections().size(), is(0));
        MergeNode mergeNode = (MergeNode)iterator.next();
        assertThat(mergeNode.projections().size(), is(1));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNoDistributedGroupByOnAllPrimaryKeys() throws Exception {
        Plan plan = plan("select count(*), id, date from empty_parted group by id, date limit 20");
        Iterator<PlanNode> iterator = plan.iterator();
        QueryAndFetchNode qafNode = (QueryAndFetchNode)iterator.next();
        assertNull(qafNode.downStreamNodes());
        assertThat(qafNode.collectorProjections().size(), is(2));
        assertThat(qafNode.collectorProjections().get(0), instanceOf(GroupProjection.class));
        assertThat(qafNode.collectorProjections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(qafNode.collectorProjections().get(1), instanceOf(TopNProjection.class));
        assertThat(qafNode.projections().size(), is(0));
        MergeNode mergeNode = (MergeNode)iterator.next();
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(TopNProjection.class));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGroupByWithOrderOnAggregate() throws Exception {
        Plan plan = plan("select count(*), name from users group by name order by count(*)");
        Iterator<PlanNode> iterator = plan.iterator();
        QueryAndFetchNode collectNode = (QueryAndFetchNode)iterator.next();

        // reducer
        iterator.next();

        // sort is on handler because there is no limit/offset
        // handler
        MergeNode mergeNode = (MergeNode)iterator.next();
        assertThat(mergeNode.projections().size(), is(1));

        TopNProjection topNProjection = (TopNProjection)mergeNode.projections().get(0);
        Symbol orderBy = topNProjection.orderBy().get(0);
        assertThat(orderBy, instanceOf(InputColumn.class));

        // points to the first values() entry of the previous GroupProjection
        assertThat(((InputColumn) orderBy).index(), is(1));
    }

    @Test
    public void testHandlerSideRouting() throws Exception {
        Plan plan = plan("select * from sys.cluster");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
    }

    @Test
    public void testHandlerSideRoutingGroupBy() throws Exception {
        Plan plan = plan("select count(*) from sys.cluster group by name");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));

        // no distributed merge, only 1 mergeNode
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testCountDistinctWithGroupBy() throws Exception {
        Plan plan = plan("select count(distinct id), name from users group by name order by count(distinct id)");
        Iterator<PlanNode> iterator = plan.iterator();

        // queryAndFetch
        QueryAndFetchNode qafNode = (QueryAndFetchNode)iterator.next();
        assertThat(qafNode.toCollect().get(0), instanceOf(Reference.class));
        assertThat(qafNode.toCollect().size(), is(2));
        assertThat(((Reference)qafNode.toCollect().get(1)).info().ident().columnIdent().name(), is("id"));
        assertThat(((Reference)qafNode.toCollect().get(0)).info().ident().columnIdent().name(), is("name"));
        Projection projection = qafNode.collectorProjections().get(0);
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
        MergeNode mergeNode = (MergeNode)iterator.next();
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
        MergeNode localMergeNode = (MergeNode)iterator.next();
        assertThat(localMergeNode.projections().size(), is(1));
        Projection localTopN = localMergeNode.projections().get(0);
        assertThat(localTopN, instanceOf(TopNProjection.class));
    }

    @Test
    public void testESUpdatePlan() throws Exception {
        Plan plan = plan("update users set name='Vogon lyric fan' where id=1");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESUpdateNode.class));

        ESUpdateNode updateNode = (ESUpdateNode)planNode;
        assertThat(updateNode.indices(), is(new String[]{"users"}));
        assertThat(updateNode.ids().size(), is(1));
        assertThat(updateNode.ids().get(0), is("1"));

        assertThat(updateNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.LONG, updateNode.outputTypes().get(0));

        Map.Entry<String, Object> entry = updateNode.updateDoc().entrySet().iterator().next();
        assertThat(entry.getKey(), is("name"));
        assertThat((String)entry.getValue(), is("Vogon lyric fan"));

        assertTrue(plan.expectsAffectedRows());
    }

    @Test
    public void testESUpdatePlanWithMultiplePrimaryKeyValues() throws Exception {
        Plan plan = plan("update users set name='Vogon lyric fan' where id in (1,2,3)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESUpdateNode.class));

        ESUpdateNode updateNode = (ESUpdateNode)planNode;
        assertThat(updateNode.ids().size(), is(3));
        assertThat(updateNode.ids(), containsInAnyOrder("1", "2", "3"));
    }

    @Test
    public void testCopyFromPlan() throws Exception {
        Plan plan = plan("copy users from '/path/to/file.extension'");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(FileUriCollectNode.class));

        FileUriCollectNode collectNode = (FileUriCollectNode)planNode;
        assertThat((BytesRef) ((Literal) collectNode.targetUri()).value(),
                is(new BytesRef("/path/to/file.extension")));
    }

    @Test
    public void testCopyFromNumReadersSetting() throws Exception {
        Plan plan = plan("copy users from '/path/to/file.extension' with (num_readers=1)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(FileUriCollectNode.class));
        FileUriCollectNode collectNode = (FileUriCollectNode)planNode;
        assertThat(collectNode.executionNodes().size(), is(1));
    }

    @Test
    public void testCopyFromPlanWithParameters() throws Exception {
        Plan plan = plan("copy users from '/path/to/file.ext' with (bulk_size=30, compression='gzip', shared=true)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(FileUriCollectNode.class));
        FileUriCollectNode collectNode = (FileUriCollectNode)planNode;
        SourceIndexWriterProjection indexWriterProjection = (SourceIndexWriterProjection) collectNode.collectorProjections().get(0);
        assertThat(indexWriterProjection.bulkActions(), is(30));
        assertThat(collectNode.compression(), is("gzip"));
        assertThat(collectNode.sharedStorage(), is(true));

        // verify defaults:
        plan = plan("copy users from '/path/to/file.ext'");
        iterator = plan.iterator();
        collectNode = (FileUriCollectNode)iterator.next();
        assertNull(collectNode.compression());
        assertNull(collectNode.sharedStorage());
    }

    @Test
    public void testCopyToWithColumnsReferenceRewrite() throws Exception {
        Plan plan = plan("copy users (name) to '/file.ext'");
        QueryAndFetchNode node = (QueryAndFetchNode)plan.iterator().next();
        Reference nameRef = (Reference)node.toCollect().get(0);

        assertThat(nameRef.info().ident().columnIdent().name(), is(DocSysColumns.DOC.name()));
        assertThat(nameRef.info().ident().columnIdent().path().get(0), is("name"));
    }

    @Test
    public void testCopyToWithNonExistentPartitionClause() throws Exception {
        Plan plan = plan("copy parted partition (date=0) to '/foo.txt' ");
        QueryAndFetchNode collectNode = (QueryAndFetchNode) plan.iterator().next();
        assertThat(collectNode.routing().hasLocations(), is(false));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCopyFromPlanWithInvalidParameters() throws Exception {
        plan("copy users from '/path/to/file.ext' with (bulk_size=-28)");
    }

    @Test
    public void testShardSelect() throws Exception {
        Plan plan = plan("select id from sys.shards");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode collectNode = (QueryAndFetchNode) planNode;
        assertTrue(collectNode.isRouted());
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testDropTable() throws Exception {
        Plan plan = plan("drop table users");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(DropTableNode.class));

        DropTableNode node = (DropTableNode) planNode;
        assertThat(node.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropPartitionedTable() throws Exception {
        Plan plan = plan("drop table parted");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();

        assertThat(planNode, instanceOf(DropTableNode.class));
        DropTableNode node = (DropTableNode) planNode;
        assertThat(node.tableInfo().ident().name(), is("parted"));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGlobalCountPlan() throws Exception {
        Plan plan = plan("select count(*) from users");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESCountNode.class));

        ESCountNode node = (ESCountNode)planNode;
        assertThat(node.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testSetPlan() throws Exception {
        Plan plan = plan("set GLOBAL PERSISTENT stats.jobs_log_size=1024");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESClusterUpdateSettingsNode.class));

        ESClusterUpdateSettingsNode node = (ESClusterUpdateSettingsNode) planNode;
        // set transient settings too when setting persistent ones
        assertThat(node.transientSettings().toDelimitedString(','), is("stats.jobs_log_size=1024,"));
        assertThat(node.persistentSettings().toDelimitedString(','), is("stats.jobs_log_size=1024,"));

        plan = plan("set GLOBAL TRANSIENT stats.enabled=false,stats.jobs_log_size=0");
        iterator = plan.iterator();
        planNode = iterator.next();
        assertThat(planNode, instanceOf(ESClusterUpdateSettingsNode.class));

        node = (ESClusterUpdateSettingsNode) planNode;
        assertThat(node.persistentSettings().getAsMap().size(), is(0));
        assertThat(node.transientSettings().toDelimitedString(','), is("stats.enabled=false,stats.jobs_log_size=0,"));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupBy() throws Exception {
        Plan plan = plan("insert into users (id, name) (select name, count(*) from sys.nodes where name='Ford' group by name)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode)planNode;

        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test (expected = UnsupportedFeatureException.class)
    public void testInsertFromSubQueryDistributedGroupByWithLimit() throws Exception {
        Plan plan = plan("insert into users (id, name) (select name, count(*) from users group by name order by name limit 10)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));

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
        Plan plan = plan("insert into users (id, name) (select name, count(*) from users group by name)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode)planNode;
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)mergeNode.projections().get(1);
        assertThat(projection.primaryKeys().size(), is(1));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.columnIdents().size(), is(2));
        assertThat(projection.columnIdents().get(0).fqn(), is("id"));
        assertThat(projection.columnIdents().get(1).fqn(), is("name"));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableName(), is("users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode localMergeNode = (MergeNode)planNode;

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByPartitioned() throws Exception {
        Plan plan = plan("insert into parted (id, date) (select id, date from users group by id, date)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode)planNode;
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)mergeNode.projections().get(1);
        assertThat(projection.primaryKeys().size(), is(2));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.primaryKeys().get(1).fqn(), is("date"));

        assertThat(projection.columnIdents().size(), is(1));
        assertThat(projection.columnIdents().get(0).fqn(), is("id"));

        assertThat(projection.partitionedBySymbols().size(), is(1));
        assertThat(((InputColumn)projection.partitionedBySymbols().get(0)).index(), is(1));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableName(), is("parted"));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode localMergeNode = (MergeNode)planNode;

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testInsertFromSubQueryGlobalAggregate() throws Exception {
        Plan plan = plan("insert into users (name, id) (select arbitrary(name), count(*) from users)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode qafNode = (QueryAndFetchNode)planNode;

        assertThat(qafNode.projections().size(), is(2));
        assertThat(qafNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)qafNode.projections().get(1);

        assertThat(projection.columnIdents().size(), is(2));
        assertThat(projection.columnIdents().get(0).fqn(), is("name"));
        assertThat(projection.columnIdents().get(1).fqn(), is("id"));

        assertThat(projection.columnSymbols().size(), is(2));
        assertThat(((InputColumn)projection.columnSymbols().get(0)).index(), is(0));
        assertThat(((InputColumn)projection.columnSymbols().get(1)).index(), is(1));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableName(), is("users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testInsertFromSubQueryESGet() throws Exception {
        Plan plan = plan("insert into users (date, id, name) (select date, id, name from users where id=1)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESGetNode.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode localMergeNode = (MergeNode)planNode;

        assertThat(localMergeNode.projections().size(), is(2));
        assertThat(localMergeNode.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection)localMergeNode.projections().get(1);

        assertThat(projection.columnIdents().size(), is(3));
        assertThat(projection.columnIdents().get(0).fqn(), is("date"));
        assertThat(projection.columnIdents().get(1).fqn(), is("id"));
        assertThat(projection.columnIdents().get(2).fqn(), is("name"));

        assertThat(((InputColumn)projection.ids().get(0)).index(), is(1));
        assertThat(((InputColumn)projection.clusteredBy()).index(), is(1));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));
    }

    @Test (expected = UnsupportedFeatureException.class)
    public void testInsertFromSubQueryWithLimit() throws Exception {
        Plan plan = plan("insert into users (date, id, name) (select date, id, name from users limit 10)");
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
        Plan plan = plan("insert into users (date, id, name) (select date, id, name from users)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode qafNode = (QueryAndFetchNode)planNode;
        assertThat(qafNode.collectorProjections().size(), is(1));
        assertThat(qafNode.collectorProjections().get(0), instanceOf(ColumnIndexWriterProjection.class));

        assertThat(qafNode.projections().size(), is(1));
        assertThat(qafNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGroupByHaving() throws Exception {
        Plan plan = plan("select avg(date), name from users group by name having min(date) > '1970-01-01'");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode qafNode = (QueryAndFetchNode)planNode;
        assertThat(qafNode.collectorProjections().size(), is(1));
        assertThat(qafNode.collectorProjections().get(0), instanceOf(GroupProjection.class));
        assertThat(qafNode.projections().size(), is(0));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode localMergeNode = (MergeNode)planNode;

        assertThat(localMergeNode.projections().size(), is(2));
        assertThat(localMergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(localMergeNode.projections().get(1), instanceOf(FilterProjection.class));

        GroupProjection groupProjection = (GroupProjection)localMergeNode.projections().get(0);
        assertThat(groupProjection.values().size(), is(2));

        FilterProjection filterProjection = (FilterProjection)localMergeNode.projections().get(1);
        assertThat(filterProjection.outputs().size(), is(2));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn)filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        assertThat(filterProjection.outputs().get(1), instanceOf(InputColumn.class));
        inputColumn = (InputColumn)filterProjection.outputs().get(1);
        assertThat(inputColumn.index(), is(1));
    }

    @Test
    public void testGroupByHavingInsertInto() throws Exception {
        Plan plan = plan("insert into users (id, name) (select name, count(*) from users group by name having count(*) > 3)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode)planNode;
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

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode localMergeNode = (MergeNode)planNode;

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testGroupByHavingNonDistributed() throws Exception {
        Plan plan = plan("select id from users group by id having id > 0");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode qafNode = (QueryAndFetchNode)planNode;
        assertThat(qafNode.collectorProjections().size(), is(1));
        assertThat(qafNode.collectorProjections().get(0), instanceOf(GroupProjection.class));
        assertThat(qafNode.projections().size(), is(0));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode localMergeNode = (MergeNode)planNode;

        assertThat(localMergeNode.projections().size(), is(2));
        assertThat(localMergeNode.projections().get(0), instanceOf(FilterProjection.class));
        assertThat(localMergeNode.projections().get(1), instanceOf(TopNProjection.class));

        FilterProjection filterProjection = (FilterProjection)localMergeNode.projections().get(0);
        assertThat(filterProjection.requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(filterProjection.outputs().size(), is(1));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn)filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));
    }

    @Test
    public void testGlobalAggregationHaving() throws Exception {
        Plan plan = plan("select avg(date) from users having min(date) > '1970-01-01'");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(QueryAndFetchNode.class));
        QueryAndFetchNode qafNode = (QueryAndFetchNode)planNode;
        assertThat(qafNode.collectorProjections().size(), is(1));
        assertThat(qafNode.collectorProjections().get(0), instanceOf(AggregationProjection.class));

        assertThat(qafNode.projections().size(), is(3));
        assertThat(qafNode.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(qafNode.projections().get(1), instanceOf(FilterProjection.class));
        assertThat(qafNode.projections().get(2), instanceOf(TopNProjection.class));

        AggregationProjection aggregationProjection = (AggregationProjection)qafNode.projections().get(0);
        assertThat(aggregationProjection.aggregations().size(), is(2));

        FilterProjection filterProjection = (FilterProjection)qafNode.projections().get(1);
        assertThat(filterProjection.outputs().size(), is(1));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn)filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        TopNProjection topNProjection = (TopNProjection)qafNode.projections().get(2);
        assertThat(topNProjection.outputs().size(), is(1));

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testCountOnPartitionedTable() throws Exception {
        Plan plan = plan("select count(*) from parted");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESCountNode.class));
    }
}
