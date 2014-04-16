package io.crate.planner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.DataType;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.WhereClause;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.ddl.ESDeleteIndexNode;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.planner.node.dql.*;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlannerTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Injector injector;
    private Analyzer analyzer;
    private Planner planner;
    Routing shardRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());

    Routing nodesRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of())
            .put("nodeTwo", ImmutableMap.<String, Set<Integer>>of())
            .build());


    class TestClusterTableInfo extends SysClusterTableInfo {

        // granularity < DOC is already handled different
        // here we want a table with handlerSideRouting and DOC granularity.

        @Override
        public RowGranularity rowGranularity() {
            return RowGranularity.DOC;
        }
    }

    class TestShardsTableInfo extends SysShardsTableInfo {


        public TestShardsTableInfo() {
            super(null);
        }

        @Override
        public Routing getRouting(WhereClause whereClause) {
            return shardRouting;
        }
    }

    class TestNodesTableInfo extends SysNodesTableInfo {

        public TestNodesTableInfo() {
            super(null);
        }

        @Override
        public Routing getRouting(WhereClause whereClause) {
            return nodesRouting;
        }
    }

    class TestSysModule extends MetaDataSysModule {

        @Override
        protected void bindTableInfos() {
            tableInfoBinder.addBinding(TestNodesTableInfo.IDENT.name()).toInstance(
                    new TestNodesTableInfo());
            tableInfoBinder.addBinding(TestShardsTableInfo.IDENT.name()).toInstance(
                    new TestShardsTableInfo());
            tableInfoBinder.addBinding(TestClusterTableInfo.IDENT.name()).toInstance(
                    new TestClusterTableInfo());
        }
    }

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
            TableInfo userTableInfo = TestingTableInfo.builder(userTableIdent, RowGranularity.DOC, shardRouting)
                    .add("name", DataType.STRING, null)
                    .add("id", DataType.LONG, null)
                    .add(DocSysColumns.RAW.name(), DataType.STRING, null)
                    .addPrimaryKey("id")
                    .clusteredBy("id")
                    .build();
            TableIdent charactersTableIdent = new TableIdent(null, "characters");
            TableInfo charactersTableInfo = TestingTableInfo.builder(charactersTableIdent, RowGranularity.DOC, shardRouting)
                    .add("name", DataType.STRING, null)
                    .add("id", DataType.STRING, null)
                    .addPrimaryKey("id")
                    .clusteredBy("id")
                    .build();
            when(schemaInfo.getTableInfo(charactersTableIdent.name())).thenReturn(charactersTableInfo);
            when(schemaInfo.getTableInfo(userTableIdent.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder()
                .add(new TestModule())
                .add(new TestSysModule())
                .add(new AggregationImplModule())
                .add(new ScalarFunctionModule())
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
        Plan plan = plan("select count('foo'), name from users group by name");
        Iterator<PlanNode> iterator = plan.iterator();
        CollectNode collectNode = (CollectNode) iterator.next();
        // TODO: optimize to not collect literal
        //assertThat(collectNode.toCollect().size(), is(1));
        GroupProjection groupProjection = (GroupProjection) collectNode.projections().get(0);
        Aggregation aggregation = groupProjection.values().get(0);
        //assertTrue(aggregation.inputs().get(0).symbolType().isLiteral());
    }

    @Test
    public void testGroupByWithAggregationPlan() throws Exception {
        Plan plan = plan("select count(*), name from users group by name");
        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));

        Iterator<PlanNode> iterator = plan.iterator();

        PlanNode planNode = iterator.next();
        // distributed collect
        assertThat(planNode, instanceOf(CollectNode.class));
        CollectNode collectNode = (CollectNode) planNode;
        assertThat(collectNode.downStreamNodes().size(), is(2));
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectNode.executionNodes().size(), is(2));
        assertThat(collectNode.toCollect().size(), is(1));
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectNode.outputTypes().size(), is(2));
        assertThat(collectNode.outputTypes().get(0), is(DataType.STRING));
        assertThat(collectNode.outputTypes().get(1), is(DataType.NULL));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode) planNode;

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
        assertThat(mergeNode.outputTypes().get(0), is(DataType.STRING));
        assertThat(mergeNode.outputTypes().get(1), is(DataType.LONG));


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
        assertThat(planNode, instanceOf(CollectNode.class));
        CollectNode collectNode = (CollectNode) planNode;

        assertThat(collectNode.outputTypes().get(0), is(DataType.NULL));
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectNode.projections().size(), is(1));
        assertThat(collectNode.projections().get(0), instanceOf(AggregationProjection.class));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode) planNode;

        assertThat(mergeNode.inputTypes().get(0), is(DataType.NULL));
        assertThat(mergeNode.outputTypes().get(0), is(DataType.LONG));

        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testGroupByOnNodeLevel() throws Exception {
        Plan plan = plan("select count(*), name from sys.nodes group by name");

        Iterator<PlanNode> iterator = plan.iterator();

        CollectNode collectNode = (CollectNode) iterator.next();
        assertFalse(collectNode.hasDownstreams());
        assertThat(collectNode.outputTypes().get(0), is(DataType.STRING));
        assertThat(collectNode.outputTypes().get(1), is(DataType.NULL));

        MergeNode mergeNode = (MergeNode) iterator.next();
        assertThat(mergeNode.numUpstreams(), is(2));
        assertThat(mergeNode.projections().size(), is(2));

        assertThat(mergeNode.outputTypes().get(0), is(DataType.LONG));
        assertThat(mergeNode.outputTypes().get(1), is(DataType.STRING));

        GroupProjection groupProjection = (GroupProjection) mergeNode.projections().get(0);
        assertThat(groupProjection.keys().size(), is(1));
        assertThat(((InputColumn) groupProjection.outputs().get(0)).index(), is(0));
        assertThat(groupProjection.outputs().get(1), is(instanceOf(Aggregation.class)));
        assertThat(((Aggregation)groupProjection.outputs().get(1)).functionIdent().name(), is("count"));
        assertThat(((Aggregation)groupProjection.outputs().get(1)).fromStep(), is(Aggregation.Step.PARTIAL));
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
        assertThat(planNode, instanceOf(CollectNode.class));
        CollectNode collectNode = (CollectNode) planNode;

        assertThat(collectNode.outputTypes().get(0), is(DataType.INTEGER));
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.SHARD));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode) planNode;

        assertThat(mergeNode.inputTypes().size(), is(1));
        assertThat(mergeNode.inputTypes().get(0), is(DataType.INTEGER));
        assertThat(mergeNode.outputTypes().size(), is(1));
        assertThat(mergeNode.outputTypes().get(0), is(DataType.INTEGER));

        assertThat(mergeNode.numUpstreams(), is(2));

        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testESSearchPlan() throws Exception {
        Plan plan = plan("select name from users where name = 'x' order by id limit 10");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESSearchNode.class));
        ESSearchNode searchNode = (ESSearchNode) planNode;

        assertThat(searchNode.outputTypes().size(), is(1));
        assertThat(searchNode.outputTypes().get(0), is(DataType.STRING));
        assertTrue(searchNode.whereClause().hasQuery());

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
        assertThat(indexNode.sourceMaps().get(0).size(), is(2));

        assertThat(indexNode.sourceMaps().get(0).keySet(), contains("id", "name"));

        assertThat((Long)indexNode.sourceMaps().get(0).get("id"), is(42L));
        assertThat((String)indexNode.sourceMaps().get(0).get("name"), is("Deep Thought"));

        assertThat(indexNode.outputTypes().size(), is(1));
        assertThat(indexNode.outputTypes().get(0), is(DataType.LONG));

        assertTrue(plan.expectsAffectedRows());
    }

    @Test
    public void testESIndexPlanMultipleValues() throws Exception {
        Plan plan = plan("insert into users (id, name) values (42, 'Deep Thought'), (99, 'Marvin')");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESIndexNode.class));

        ESIndexNode indexNode = (ESIndexNode) planNode;

        assertThat(indexNode.sourceMaps().size(), is(2));
        assertThat(indexNode.sourceMaps().get(0).size(), is(2));
        assertThat(indexNode.sourceMaps().get(1).size(), is(2));

        assertThat((Long)indexNode.sourceMaps().get(0).get("id"), is(42L));
        assertThat((String)indexNode.sourceMaps().get(0).get("name"), is("Deep Thought"));

        assertThat((Long)indexNode.sourceMaps().get(1).get("id"), is(99L));
        assertThat((String)indexNode.sourceMaps().get(1).get("name"), is("Marvin"));

        assertThat(indexNode.outputTypes().size(), is(1));
        assertThat(indexNode.outputTypes().get(0), is(DataType.LONG));

        assertTrue(plan.expectsAffectedRows());
    }

    @Test
    public void testCountDistinctPlan() throws Exception {
        Plan plan = plan("select count(distinct name) from users");
        Iterator<PlanNode> iterator = plan.iterator();
        CollectNode collectNode = (CollectNode)iterator.next();
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

        MergeNode mergeNode = (MergeNode)iterator.next();
        assertThat(mergeNode.projections().size(), is(2));
        Projection projection1 = mergeNode.projections().get(1);
        assertThat(projection1, instanceOf(TopNProjection.class));
        Symbol collection_count = projection1.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));
    }

    @Test
    public void testGroupByWithOrderOnAggregate() throws Exception {
        Plan plan = plan("select count(*), name from users group by name order by count(*)");
        Iterator<PlanNode> iterator = plan.iterator();
        CollectNode collectNode = (CollectNode)iterator.next();

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
        assertThat(planNode, instanceOf(CollectNode.class));
    }

    @Test
    public void testHandlerSideRoutingGroupBy() throws Exception {
        Plan plan = plan("select count(*) from sys.cluster group by name");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        assertThat(planNode, instanceOf(CollectNode.class));
        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));

        // no distributed merge, only 1 mergeNode
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testCountDistinctWithGroupBy() throws Exception {
        Plan plan = plan("select count(distinct id), name from users group by name order by count(distinct id)");
        Iterator<PlanNode> iterator = plan.iterator();

        // collect
        CollectNode collectNode = (CollectNode)iterator.next();
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
        assertThat(updateNode.index(), is("users"));
        assertThat(updateNode.ids().size(), is(1));
        assertThat(updateNode.ids().get(0), is("1"));

        assertThat(updateNode.outputTypes().size(), is(1));
        assertThat(updateNode.outputTypes().get(0), is(DataType.LONG));

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
        assertThat(((Literal)collectNode.targetUri()).valueAsString(), is("/path/to/file.extension"));
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
        Plan plan = plan("copy users from '/path/to/file.ext' with (concurrency=8, bulk_size=30, compression='gzip', shared=true)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(FileUriCollectNode.class));
        FileUriCollectNode collectNode = (FileUriCollectNode)planNode;
        IndexWriterProjection indexWriterProjection = (IndexWriterProjection) collectNode.projections().get(0);
        assertThat(indexWriterProjection.concurrency(), is(8));
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
        CollectNode node = (CollectNode)plan.iterator().next();
        Reference nameRef = (Reference)node.toCollect().get(0);

        assertThat(nameRef.info().ident().columnIdent().name(), is(DocSysColumns.DOC.name()));
        assertThat(nameRef.info().ident().columnIdent().path().get(0), is("name"));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCopyFromPlanWithInvalidParameters() throws Exception {
        plan("copy users from '/path/to/file.ext' with (concurrency=-28)");
    }

    @Test
    public void testShardSelect() throws Exception {
        Plan plan = plan("select table_name from sys.shards");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(CollectNode.class));
        CollectNode collectNode = (CollectNode) planNode;
        assertTrue(collectNode.isRouted());
        assertThat(collectNode.maxRowGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testDropTable() throws Exception {
        Plan plan = plan("drop table users");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESDeleteIndexNode.class));

        ESDeleteIndexNode node = (ESDeleteIndexNode) planNode;
        assertThat(node.index(), is("users"));
    }

    @Test
    public void testGlobalCountPlan() throws Exception {
        Plan plan = plan("select count(*) from users");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESCountNode.class));

        ESCountNode node = (ESCountNode)planNode;
        assertThat(node.indexName(), is("users"));
    }
}
