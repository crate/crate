package io.crate.planner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.operator.OperatorModule;
import io.crate.planner.node.*;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.LongLiteral;
import io.crate.planner.symbol.StringLiteral;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.cratedb.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertTrue;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlannerTest {

    private Injector injector;
    private Analyzer analyzer;
    private Planner planner = new Planner();
    Routing shardRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());

    Routing nodesRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of())
            .put("nodeTwo", ImmutableMap.<String, Set<Integer>>of())
            .build());

    class TestShardsTableInfo extends SysShardsTableInfo {


        public TestShardsTableInfo() {
            super(null);
        }

        @Override
        public Routing getRouting(Function whereClause) {
            return shardRouting;
        }
    }

    class TestNodesTableInfo extends SysNodesTableInfo {

        public TestNodesTableInfo() {
            super(null);
        }

        @Override
        public Routing getRouting(Function whereClause) {
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
        }
    }

    class TestModule extends MetaDataModule {

        @Override
        protected void configure() {
            ClusterService clusterService = mock(ClusterService.class);
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
                    .addPrimaryKey("id")
                    .build();
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
                .add(new OperatorModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
    }

    private Plan plan(String statement) {
        return planner.plan(analyzer.analyze(SqlParser.createStatement(statement)));
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
        ESGetNode node = (ESGetNode)iterator.next();
        assertThat(node.index(), is("users"));
        assertThat(node.ids().get(0), is("1"));
        assertFalse(iterator.hasNext());
        assertThat(node.outputs().size(), is(1));
    }

    @Test
    public void testMultiGetPlan() throws Exception {
        Plan plan = plan("select name from users where id in (1, 2)");
        Iterator<PlanNode> iterator = plan.iterator();
        ESGetNode node = (ESGetNode)iterator.next();
        assertThat(node.index(), is("users"));
        assertThat(node.ids().size(), is(2));
        assertThat(node.ids().get(0), is("1"));
        assertThat(node.ids().get(1), is("2"));
    }

    @Test
    public void testDeletePlan() throws Exception {
        Plan plan = plan("delete from users where id = 1");
        Iterator<PlanNode> iterator = plan.iterator();
        ESDeleteNode node = (ESDeleteNode)iterator.next();
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
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(topN.outputs().get(1), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));


        // local merge
        planNode = iterator.next();
        assertThat(planNode.projections().get(0), instanceOf(TopNProjection.class));
        topN = (TopNProjection) planNode.projections().get(0);
        assertThat(topN.limit(), is(1));
        assertThat(topN.offset(), is(1));
        assertThat(topN.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(1));
        assertThat(topN.outputs().get(1), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(0));

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testGlobalAggregationPlan() throws Exception {
        Statement statement = SqlParser.createStatement("select count(name) from users");

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
        assertThat(collectNode.outputTypes().get(1), is(DataType.LONG));

        MergeNode mergeNode = (MergeNode) iterator.next();
        assertThat(mergeNode.numUpstreams(), is(2));
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
        assertTrue(searchNode.whereClause().isPresent());

        assertFalse(plan.expectsAffectedRows());
    }

    @Test
    public void testESIndexPlan() throws Exception {
        Plan plan = plan("insert into users (id, name) values (42, 'Deep Thought')");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(ESIndexNode.class));

        ESIndexNode indexNode = (ESIndexNode) planNode;
        assertThat(indexNode.columns().size(), is(2));
        assertThat(indexNode.columns().get(0).valueType(), is(DataType.LONG));
        assertThat(indexNode.columns().get(0).info().ident().columnIdent().name(), is("id"));

        assertThat(indexNode.columns().get(1).valueType(), is(DataType.STRING));
        assertThat(indexNode.columns().get(1).info().ident().columnIdent().name(), is("name"));

        assertThat(indexNode.valuesLists().size(), is(1));
        assertThat(((LongLiteral)indexNode.valuesLists().get(0).get(0)).value(), is(42l));
        assertThat(((StringLiteral)indexNode.valuesLists().get(0).get(1)).value().utf8ToString(), is("Deep Thought"));

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

        assertThat(indexNode.valuesLists().size(), is(2));
        assertThat(((LongLiteral)indexNode.valuesLists().get(0).get(0)).value(), is(42l));
        assertThat(((StringLiteral)indexNode.valuesLists().get(0).get(1)).value().utf8ToString(), is("Deep Thought"));

        assertThat(((LongLiteral)indexNode.valuesLists().get(1).get(0)).value(), is(99l));
        assertThat(((StringLiteral)indexNode.valuesLists().get(1).get(1)).value().utf8ToString(), is("Marvin"));

        assertThat(indexNode.outputTypes().size(), is(1));
        assertThat(indexNode.outputTypes().get(0), is(DataType.LONG));

        assertTrue(plan.expectsAffectedRows());
    }
}
