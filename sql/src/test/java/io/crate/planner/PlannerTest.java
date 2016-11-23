package io.crate.planner;

import com.carrotsearch.hppc.IntSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.*;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import io.crate.planner.node.ddl.ESDeletePartition;
import io.crate.planner.node.ddl.GenericDDLPlan;
import io.crate.planner.node.dml.Delete;
import io.crate.planner.node.dml.ESDelete;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.projection.*;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.LongLiteral;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.*;

import static io.crate.testing.SymbolMatchers.*;
import static io.crate.testing.TestingHelpers.isDocKey;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

@SuppressWarnings("ConstantConditions")
public class PlannerTest extends AbstractPlannerTest {

    @Test
    public void testGetPlan() throws Exception {
        ESGet esGet = plan("select name from users where id = 1");
        assertThat(esGet.tableInfo().ident().name(), is("users"));
        assertThat(esGet.docKeys().getOnlyKey(), isDocKey(1L));
        assertThat(esGet.outputs().size(), is(1));
    }

    @Test
    public void testGetWithVersion() throws Exception {
        expectedException.expect(VersionInvalidException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause of a SELECT statement");
        plan("select name from users where id = 1 and _version = 1");
    }

    @Test
    public void testGetPlanStringLiteral() throws Exception {
        ESGet esGet = plan("select name from characters where id = 'one'");
        assertThat(esGet.tableInfo().ident().name(), is("characters"));
        assertThat(esGet.docKeys().getOnlyKey(), isDocKey("one"));
        assertThat(esGet.outputs().size(), is(1));
    }

    @Test
    public void testGetPlanPartitioned() throws Exception {
        ESGet esGet = plan("select name, date from parted where id = 'one' and date = 0");
        assertThat(esGet.tableInfo().ident().name(), is("parted"));
        assertThat(esGet.docKeys().getOnlyKey(), isDocKey("one", 0L));

        //is(new PartitionName("parted", Arrays.asList(new BytesRef("0"))).asIndexName()));
        assertEquals(DataTypes.STRING, esGet.outputTypes().get(0));
        assertEquals(DataTypes.TIMESTAMP, esGet.outputTypes().get(1));
    }

    @Test
    public void testMultiGetPlan() throws Exception {
        ESGet esGet = plan("select name from users where id in (1, 2)");
        assertThat(esGet.docKeys().size(), is(2));
        assertThat(esGet.docKeys(), containsInAnyOrder(isDocKey(1L), isDocKey(2L)));
    }

    @Test
    public void testDeletePlan() throws Exception {
        ESDelete plan = plan("delete from users where id = 1");
        assertThat(plan.tableInfo().ident().name(), is("users"));
        assertThat(plan.docKeys().size(), is(1));
        assertThat(plan.docKeys().get(0), isDocKey(1L));
    }

    @Test
    public void testBulkDeletePartitionedTable() throws Exception {
        ESDeletePartition plan = (ESDeletePartition) plan("delete from parted where date = ?", new Object[][]{
            new Object[]{"0"},
            new Object[]{"123"},
        });
        assertThat(plan.indices(), is(new String[]{".partitioned.parted.04130", ".partitioned.parted.04232chj"}));
    }

    @Test
    public void testMultiDeletePlan() throws Exception {
        Delete plan = plan("delete from users where id in (1, 2)");
        assertThat(plan.nodes().size(), is(1));

        Merge merge = (Merge) plan.nodes().get(0);
        Collect collect = (Collect) merge.subPlan();
        assertThat(collect.collectPhase().projections().size(), is(1));
        assertThat(collect.collectPhase().projections().get(0), instanceOf(DeleteProjection.class));
    }


    @Test
    public void testGlobalAggregationPlan() throws Exception {
        Merge globalAggregate = plan("select count(name) from users");
        Collect collect = (Collect) globalAggregate.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertEquals(CountAggregation.LongStateType.INSTANCE, collectPhase.outputTypes().get(0));
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));

        MergePhase mergeNode = globalAggregate.mergePhase();

        assertEquals(CountAggregation.LongStateType.INSTANCE, Iterables.get(mergeNode.inputTypes(), 0));
        assertEquals(DataTypes.LONG, mergeNode.outputTypes().get(0));
    }

    @Test
    public void testShardSelectWithOrderBy() throws Exception {
        Merge merge = plan("select id from sys.shards order by id limit 10");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertEquals(DataTypes.INTEGER, collectPhase.outputTypes().get(0));
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.SHARD));

        assertThat(collectPhase.orderBy(), notNullValue());

        List<Projection> projections = collectPhase.projections();
        assertThat(projections.size(), is(1));
        assertThat(projections.get(0), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection) projections.get(0)).isOrdered(), is(false));

        MergePhase mergeNode = merge.mergePhase();

        assertThat(mergeNode.inputTypes().size(), is(1));
        assertEquals(DataTypes.INTEGER, Iterables.get(mergeNode.inputTypes(), 0));
        assertThat(mergeNode.outputTypes().size(), is(1));
        assertEquals(DataTypes.INTEGER, mergeNode.outputTypes().get(0));

        assertThat(mergeNode.numUpstreams(), is(2));
    }

    @Test
    public void testCollectAndMergePlan() throws Exception {
        QueryThenFetch qtf = plan("select name from users where name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertTrue(collectPhase.whereClause().hasQuery());

        TopNProjection topNProjection = (TopNProjection) collectPhase.projections().get(0);
        assertThat(topNProjection.limit(), is(10));
        assertThat(topNProjection.isOrdered(), is(false));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));

        assertTrue(mergePhase.finalProjection().isPresent());

        Projection lastProjection = mergePhase.finalProjection().get();
        assertThat(lastProjection, instanceOf(FetchProjection.class));
        FetchProjection fetchProjection = (FetchProjection) lastProjection;
        assertThat(fetchProjection.outputs(), isSQL("FETCH(INPUT(0), doc.users._doc['name'])"));
    }

    @Test
    public void testCollectAndMergePlanNoFetch() throws Exception {
        // testing that a fetch projection is not added if all output symbols are included
        // at the orderBy symbols
        Merge merge = plan("select name from users where name = 'x' order by name limit 10");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertTrue(collectPhase.whereClause().hasQuery());

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));

        assertTrue(mergePhase.finalProjection().isPresent());

        Projection lastProjection = mergePhase.finalProjection().get();
        assertThat(lastProjection, instanceOf(TopNProjection.class));
        TopNProjection topNProjection = (TopNProjection) lastProjection;
        assertThat(topNProjection.outputs().size(), is(1));
    }

    @Test
    public void testCollectAndMergePlanHighLimit() throws Exception {
        QueryThenFetch qtf = plan("select name from users limit 100000");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(collectPhase.nodePageSizeHint(), is(100_000));

        MergePhase mergeNode = merge.mergePhase();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.finalProjection().get(), instanceOf(FetchProjection.class));
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(0);
        assertThat(topN.limit(), is(100_000));
        assertThat(topN.offset(), is(0));
        assertNull(topN.orderBy());

        FetchProjection fetchProjection = (FetchProjection) mergeNode.projections().get(1);

        // with offset
        qtf = plan("select name from users limit 100000 offset 20");
        merge = ((Merge) qtf.subPlan());

        collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(collectPhase.nodePageSizeHint(), is(100_000 + 20));

        mergeNode = merge.mergePhase();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.finalProjection().get(), instanceOf(FetchProjection.class));
        topN = (TopNProjection) mergeNode.projections().get(0);
        assertThat(topN.limit(), is(100_000));
        assertThat(topN.offset(), is(20));
        assertNull(topN.orderBy());

        fetchProjection = (FetchProjection) mergeNode.projections().get(1);
    }


    @Test
    public void testCollectAndMergePlanPartitioned() throws Exception {
        QueryThenFetch qtf = plan("select id, name, date from parted where date > 0 and name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());

        List<String> indices = new ArrayList<>();
        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        for (Map.Entry<String, Map<String, List<Integer>>> entry : locations.entrySet()) {
            indices.addAll(entry.getValue().keySet());
        }
        assertThat(indices, Matchers.contains(
            new PartitionName("parted", Arrays.asList(new BytesRef("123"))).asIndexName()));

        assertTrue(collectPhase.whereClause().hasQuery());

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(3));
    }

    @Test
    public void testCollectAndMergePlanFunction() throws Exception {
        QueryThenFetch qtf = plan("select format('Hi, my name is %s', name), name from users where name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());

        assertTrue(collectPhase.whereClause().hasQuery());

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(1));

        assertTrue(mergePhase.finalProjection().isPresent());

        Projection lastProjection = mergePhase.finalProjection().get();
        assertThat(lastProjection, instanceOf(FetchProjection.class));
        FetchProjection fetchProjection = (FetchProjection) lastProjection;
        assertThat(fetchProjection.outputs().size(), is(2));
        assertThat(fetchProjection.outputs().get(0), isFunction("format"));
        assertThat(fetchProjection.outputs().get(1), isFetchRef(0, "_doc['name']"));
    }

    @Test
    public void testInsertPlan() throws Exception {
        UpsertById upsertById = plan("insert into users (id, name) values (42, 'Deep Thought')");

        assertThat(upsertById.insertColumns().length, is(2));
        Reference idRef = upsertById.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = upsertById.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(upsertById.items().size(), is(1));
        UpsertById.Item item = upsertById.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("42"));
        assertThat(item.routing(), is("42"));

        assertThat(item.insertValues().length, is(2));
        assertThat((Long) item.insertValues()[0], is(42L));
        assertThat((BytesRef) item.insertValues()[1], is(new BytesRef("Deep Thought")));
    }

    @Test
    public void testInsertPlanMultipleValues() throws Exception {
        UpsertById upsertById = plan("insert into users (id, name) values (42, 'Deep Thought'), (99, 'Marvin')");

        assertThat(upsertById.insertColumns().length, is(2));
        Reference idRef = upsertById.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = upsertById.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(upsertById.items().size(), is(2));

        UpsertById.Item item1 = upsertById.items().get(0);
        assertThat(item1.index(), is("users"));
        assertThat(item1.id(), is("42"));
        assertThat(item1.routing(), is("42"));
        assertThat(item1.insertValues().length, is(2));
        assertThat((Long) item1.insertValues()[0], is(42L));
        assertThat((BytesRef) item1.insertValues()[1], is(new BytesRef("Deep Thought")));

        UpsertById.Item item2 = upsertById.items().get(1);
        assertThat(item2.index(), is("users"));
        assertThat(item2.id(), is("99"));
        assertThat(item2.routing(), is("99"));
        assertThat(item2.insertValues().length, is(2));
        assertThat((Long) item2.insertValues()[0], is(99L));
        assertThat((BytesRef) item2.insertValues()[1], is(new BytesRef("Marvin")));
    }

    @Test
    public void testCountDistinctPlan() throws Exception {
        Merge globalAggregate = plan("select count(distinct name) from users");
        Collect collect = (Collect) globalAggregate.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        Projection projection = collectPhase.projections().get(0);
        assertThat(projection, instanceOf(AggregationProjection.class));
        AggregationProjection aggregationProjection = (AggregationProjection) projection;
        assertThat(aggregationProjection.aggregations().size(), is(1));

        Aggregation aggregation = aggregationProjection.aggregations().get(0);
        assertThat(aggregation.toStep(), is(Aggregation.Step.PARTIAL));
        Symbol aggregationInput = aggregation.inputs().get(0);
        assertThat(aggregationInput.symbolType(), is(SymbolType.INPUT_COLUMN));

        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference) collectPhase.toCollect().get(0)).ident().columnIdent().name(), is("name"));

        MergePhase mergeNode = globalAggregate.mergePhase();
        assertThat(mergeNode.projections().size(), is(2));
        Projection projection1 = mergeNode.projections().get(1);
        assertThat(projection1, instanceOf(TopNProjection.class));
        Symbol collection_count = projection1.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));
    }


    @Test
    public void testNoDistributedGroupByOnAllPrimaryKeys() throws Exception {
        Merge merge = plan(
            "select count(*), id, date from empty_parted group by id, date limit 20");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(collectPhase.projections().get(1), instanceOf(TopNProjection.class));
        MergePhase mergeNode = merge.mergePhase();
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(TopNProjection.class));
    }

    @Test
    public void testNonDistributedGroupByAggregationsWrappedInScalar() throws Exception {
        Merge planNode = plan(
            "select (count(*) + 1), id from empty_parted group by id");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) planNode.subPlan();

        RoutedCollectPhase collectPhase = distributedGroupBy.collectPhase();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));

        TopNProjection topNProjection = (TopNProjection) distributedGroupBy.reducerMergeNode().projections().get(1);
        assertThat(topNProjection.limit(), is(TopN.NO_LIMIT));
        assertThat(topNProjection.offset(), is(0));

        MergePhase mergeNode = planNode.mergePhase();
        assertThat(mergeNode.projections().size(), is(0));
    }

    @Test
    public void testHandlerSideRouting() throws Exception {
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        Merge plan = plan("select * from sys.cluster");
        assertThat(plan.subPlan(), instanceOf(Collect.class));
    }

    @Test
    public void testUpdateByQueryPlan() throws Exception {
        Upsert plan = plan("update users set name='Vogon lyric fan'");
        assertThat(plan.nodes().size(), is(1));

        Merge merge = (Merge) plan.nodes().get(0);
        Collect collect = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.routing(), is(shardRouting("users")));
        assertFalse(collectPhase.whereClause().noMatch());
        assertFalse(collectPhase.whereClause().hasQuery());
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(UpdateProjection.class));
        assertThat(collectPhase.toCollect().size(), is(1));
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference) collectPhase.toCollect().get(0)).ident().columnIdent().fqn(), is("_uid"));

        UpdateProjection updateProjection = (UpdateProjection) collectPhase.projections().get(0);
        assertThat(updateProjection.uidSymbol(), instanceOf(InputColumn.class));

        assertThat(updateProjection.assignmentsColumns()[0], is("name"));
        Symbol symbol = updateProjection.assignments()[0];
        assertThat(symbol, isLiteral("Vogon lyric fan", DataTypes.STRING));

        MergePhase mergeNode = merge.mergePhase();
        assertThat(mergeNode.projections().size(), is(1));
        assertThat(mergeNode.projections().get(0), instanceOf(MergeCountProjection.class));

        assertThat(mergeNode.outputTypes().size(), is(1));
    }

    @Test
    public void testUpdateByIdPlan() throws Exception {
        UpsertById upsertById = plan("update users set name='Vogon lyric fan' where id=1");
        assertThat(upsertById.items().size(), is(1));

        assertThat(upsertById.updateColumns()[0], is("name"));

        UpsertById.Item item = upsertById.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("1"));

        Symbol symbol = item.updateAssignments()[0];
        assertThat(symbol, isLiteral("Vogon lyric fan", DataTypes.STRING));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValues() throws Exception {
        UpsertById plan = plan("update users set name='Vogon lyric fan' where id in (1,2,3)");

        List<String> ids = new ArrayList<>(3);
        for (UpsertById.Item item : plan.items()) {
            ids.add(item.id());
            assertThat(item.updateAssignments().length, is(1));
            assertThat(item.updateAssignments()[0], isLiteral("Vogon lyric fan", DataTypes.STRING));
        }

        assertThat(ids, containsInAnyOrder("1", "2", "3"));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValuesPartitioned() throws Exception {
        UpsertById planNode = plan("update parted set name='Vogon lyric fan' where " +
                                   "(id=2 and date = 0) OR" +
                                   "(id=3 and date=123)");

        List<String> partitions = new ArrayList<>(2);
        List<String> ids = new ArrayList<>(2);
        for (UpsertById.Item item : planNode.items()) {
            partitions.add(item.index());
            ids.add(item.id());
            assertThat(item.updateAssignments().length, is(1));
            assertThat(item.updateAssignments()[0], isLiteral("Vogon lyric fan", DataTypes.STRING));
        }
        assertThat(ids, containsInAnyOrder("AgEyATA=", "AgEzAzEyMw==")); // multi primary key - values concatenated and base64'ed
        assertThat(partitions, containsInAnyOrder(".partitioned.parted.04130", ".partitioned.parted.04232chj"));
    }

    @Test
    public void testCopyToWithColumnsReferenceRewrite() throws Exception {
        Merge plan = plan("copy users (name) to directory '/tmp'");
        Collect innerPlan = (Collect) plan.subPlan();
        RoutedCollectPhase node = ((RoutedCollectPhase) innerPlan.collectPhase());
        Reference nameRef = (Reference) node.toCollect().get(0);

        assertThat(nameRef.ident().columnIdent().name(), is(DocSysColumns.DOC.name()));
        assertThat(nameRef.ident().columnIdent().path().get(0), is("name"));
    }

    @Test
    public void testCopyToWithPartitionedGeneratedColumn() throws Exception {
        // test that generated partition column is NOT exported
        Merge plan = plan("copy parted_generated to directory '/tmp'");
        Collect innerPlan = (Collect) plan.subPlan();
        RoutedCollectPhase node = ((RoutedCollectPhase) innerPlan.collectPhase());
        WriterProjection projection = (WriterProjection) node.projections().get(0);
        assertThat(projection.overwrites().size(), is(0));
    }

    @Test
    public void testShardSelect() throws Exception {
        Merge merge = plan("select id from sys.shards");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertTrue(collectPhase.isRouted());
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testDropTable() throws Exception {
        DropTablePlan plan = plan("drop table users");
        assertThat(plan.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropTableIfExistsWithUnknownSchema() throws Exception {
        Plan plan = plan("drop table if exists unknown_schema.unknwon_table");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testDropTableIfExists() throws Exception {
        DropTablePlan plan = plan("drop table if exists users");
        assertThat(plan.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropTableIfExistsNonExistentTableCreatesNoop() throws Exception {
        Plan plan = plan("drop table if exists groups");
        assertThat(plan, instanceOf(NoopPlan.class));
    }


    @Test
    public void testDropPartitionedTable() throws Exception {
        DropTablePlan plan = plan("drop table parted");
        assertThat(plan.tableInfo().ident().name(), is("parted"));
    }

    @Test
    public void testDropBlobTableIfExistsCreatesIterablePlan() throws Exception {
        Plan plan = plan("drop blob table if exists screenshots");
        assertThat(plan, instanceOf(GenericDDLPlan.class));
    }

    @Test
    public void testDropNonExistentBlobTableCreatesNoop() throws Exception {
        Plan plan = plan("drop blob table if exists unknown");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testGlobalCountPlan() throws Exception {
        CountPlan plan = plan("select count(*) from users");

        assertThat(plan.countNode().whereClause(), equalTo(WhereClause.MATCH_ALL));

        assertThat(plan.mergeNode().projections().size(), is(1));
        assertThat(plan.mergeNode().projections().get(0), instanceOf(MergeCountProjection.class));
    }

    @Test
    public void testSetPlan() throws Exception {
        ESClusterUpdateSettingsPlan plan = plan("set GLOBAL PERSISTENT stats.jobs_log_size=1024");

        // set transient settings too when setting persistent ones
        assertThat(plan.transientSettings().get("stats.jobs_log_size").get(0), Is.<Expression>is(new LongLiteral("1024")));
        assertThat(plan.persistentSettings().get("stats.jobs_log_size").get(0), Is.<Expression>is(new LongLiteral("1024")));

        plan = plan("set GLOBAL TRANSIENT stats.enabled=false,stats.jobs_log_size=0");

        assertThat(plan.persistentSettings().size(), is(0));
        assertThat(plan.transientSettings().size(), is(2));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupBy() throws Exception {
        Merge nonDistributedGroupBy = plan(
            "insert into users (id, name) (select count(*), name from sys.nodes group by name)");
        MergePhase mergeNode = nonDistributedGroupBy.mergePhase();
        assertThat(mergeNode.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupByWithCast() throws Exception {
        Merge nonDistributedGroupBy = plan(
            "insert into users (id, name) (select name, count(*) from sys.nodes group by name)");
        MergePhase mergeNode = nonDistributedGroupBy.mergePhase();
        assertThat(mergeNode.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));

        TopNProjection topN = (TopNProjection) mergeNode.projections().get(1);
        assertThat(topN.offset(), is(TopN.NO_OFFSET));
        assertThat(topN.limit(), is(TopN.NO_LIMIT));
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByWithLimit() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Using limit, offset or order by is not supported on insert using a sub-query");

        plan("insert into users (id, name) (select name, count(*) from users group by name order by name limit 10)");
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByWithoutLimit() throws Exception {
        Merge planNode = plan(
            "insert into users (id, name) (select name, count(*) from users group by name)");
        DistributedGroupBy groupBy = (DistributedGroupBy) planNode.subPlan();
        MergePhase mergeNode = groupBy.reducerMergeNode();
        assertThat(mergeNode.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));

        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergeNode.projections().get(2);
        assertThat(projection.primaryKeys().size(), is(1));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.columnReferences().size(), is(2));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("id"));
        assertThat(projection.columnReferences().get(1).ident().columnIdent().fqn(), is("name"));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableIdent().fqn(), is("doc.users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));

        MergePhase localMergeNode = planNode.mergePhase();
        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(MergeCountProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByPartitioned() throws Exception {
        Merge planNode = plan(
            "insert into parted (id, date) (select id, date from users group by id, date)");
        DistributedGroupBy groupBy = (DistributedGroupBy) planNode.subPlan();
        MergePhase mergeNode = groupBy.reducerMergeNode();
        assertThat(mergeNode.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergeNode.projections().get(2);
        assertThat(projection.primaryKeys().size(), is(2));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.primaryKeys().get(1).fqn(), is("date"));

        assertThat(projection.columnReferences().size(), is(1));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("id"));

        assertThat(projection.partitionedBySymbols().size(), is(1));
        assertThat(((InputColumn) projection.partitionedBySymbols().get(0)).index(), is(1));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableIdent().fqn(), is("doc.parted"));

        MergePhase localMergeNode = planNode.mergePhase();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(MergeCountProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

    }

    @Test
    public void testInsertFromSubQueryGlobalAggregate() throws Exception {
        Merge globalAggregate = plan(
            "insert into users (name, id) (select arbitrary(name), count(*) from users)");
        MergePhase mergeNode = globalAggregate.mergePhase();
        assertThat(mergeNode.projections().size(), is(3));
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(1);
        assertThat(topN.limit(), is(TopN.NO_LIMIT));
        assertThat(topN.offset(), is(TopN.NO_OFFSET));

        assertThat(mergeNode.projections().get(2), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergeNode.projections().get(2);

        assertThat(projection.columnReferences().size(), is(2));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("name"));
        assertThat(projection.columnReferences().get(1).ident().columnIdent().fqn(), is("id"));

        assertThat(projection.columnSymbols().size(), is(2));
        assertThat(((InputColumn) projection.columnSymbols().get(0)).index(), is(0));
        assertThat(((InputColumn) projection.columnSymbols().get(1)).index(), is(1));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableIdent().fqn(), is("doc.users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));
    }

    @Test
    public void testInsertFromSubQueryESGet() throws Exception {
        // doesn't use ESGetNode but CollectNode.
        // Round-trip to handler can be skipped by writing from the shards directly
        Merge merge = plan(
            "insert into users (date, id, name) (select date, id, name from users where id=1)");
        Collect queryAndFetch = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) queryAndFetch.collectPhase());

        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) collectPhase.projections().get(0);

        assertThat(projection.columnReferences().size(), is(3));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("date"));
        assertThat(projection.columnReferences().get(1).ident().columnIdent().fqn(), is("id"));
        assertThat(projection.columnReferences().get(2).ident().columnIdent().fqn(), is("name"));
        assertThat(((InputColumn) projection.ids().get(0)).index(), is(1));
        assertThat(((InputColumn) projection.clusteredBy()).index(), is(1));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));
    }

    @Test
    public void testInsertFromSubQueryJoin() throws Exception {
        NestedLoop nestedLoop = plan(
            "insert into users (id, name) (select u1.id, u2.name from users u1 CROSS JOIN users u2)");
        assertThat(nestedLoop.nestedLoopPhase().projections(), contains(
            instanceOf(TopNProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)
        ));
        assertThat(nestedLoop.nestedLoopPhase().projections().get(0), instanceOf(TopNProjection.class));
        TopNProjection topN = (TopNProjection) nestedLoop.nestedLoopPhase().projections().get(0);
        assertThat(topN.limit(), is(TopN.NO_LIMIT));
        assertThat(topN.offset(), is(TopN.NO_OFFSET));

        assertThat(nestedLoop.nestedLoopPhase().projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) nestedLoop.nestedLoopPhase().projections().get(1);

        assertThat(projection.columnReferences().size(), is(2));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("id"));
        assertThat(projection.columnReferences().get(1).ident().columnIdent().fqn(), is("name"));
        assertThat(((InputColumn) projection.ids().get(0)).index(), is(0));
        assertThat(((InputColumn) projection.clusteredBy()).index(), is(0));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));
    }

    @Test
    public void testInsertFromSubQueryWithLimit() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Using limit, offset or order by is not supported on insert using a sub-query");

        plan("insert into users (date, id, name) (select date, id, name from users limit 10)");
    }

    @Test
    public void testInsertFromSubQueryWithOffset() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Using limit, offset or order by is not supported on insert using a sub-query");

        plan("insert into users (date, id, name) (select date, id, name from users offset 10)");
    }

    @Test
    public void testInsertFromSubQueryWithOrderBy() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Using limit, offset or order by is not supported on insert using a sub-query");

        plan("insert into users (date, id, name) (select date, id, name from users order by id)");
    }

    @Test
    public void testInsertFromSubQueryWithoutLimit() throws Exception {
        Merge planNode = plan(
            "insert into users (date, id, name) (select date, id, name from users)");
        Collect collect = (Collect) planNode.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(ColumnIndexWriterProjection.class));

        MergePhase localMergeNode = planNode.mergePhase();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(MergeCountProjection.class));
    }

    @Test
    public void testInsertFromSubQueryReduceOnCollectorGroupBy() throws Exception {
        Merge merge = plan(
            "insert into users (id, name) (select id, arbitrary(name) from users group by id)");
        Collect collect = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)
        ));
        ColumnIndexWriterProjection columnIndexWriterProjection =
            (ColumnIndexWriterProjection) collectPhase.projections().get(2);
        assertThat(columnIndexWriterProjection.columnReferences(), contains(isReference("id"), isReference("name")));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections(), contains(instanceOf(MergeCountProjection.class)));

    }

    @Test
    public void testInsertFromSubQueryReduceOnCollectorGroupByWithCast() throws Exception {
        Merge merge = plan(
            "insert into users (id, name) (select id, count(*) from users group by id)");
        Collect nonDistributedGroupBy = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) nonDistributedGroupBy.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
        TopNProjection collectTopN = (TopNProjection) collectPhase.projections().get(1);
        assertThat(collectTopN.limit(), is(TopN.NO_LIMIT));
        assertThat(collectTopN.offset(), is(TopN.NO_OFFSET));
        assertThat(collectTopN.outputs(), contains(isInputColumn(0), isFunction("to_string")));

        ColumnIndexWriterProjection columnIndexWriterProjection = (ColumnIndexWriterProjection) collectPhase.projections().get(2);
        assertThat(columnIndexWriterProjection.columnReferences(), contains(isReference("id"), isReference("name")));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections(), contains(instanceOf(MergeCountProjection.class)));

    }

    @Test
    public void testGroupByHaving() throws Exception {
        Merge distributedGroupByMerge = plan(
            "select avg(date), name from users group by name having min(date) > '1970-01-01'");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) distributedGroupByMerge.subPlan();
        RoutedCollectPhase collectPhase = distributedGroupBy.collectPhase();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));

        MergePhase mergeNode = distributedGroupBy.reducerMergeNode();

        assertThat(mergeNode.projections().size(), is(3));

        // grouping
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) mergeNode.projections().get(0);
        assertThat(groupProjection.values().size(), is(2));

        // filter the having clause
        assertThat(mergeNode.projections().get(1), instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) mergeNode.projections().get(1);

        // apply the default limit
        assertThat(mergeNode.projections().get(2), instanceOf(TopNProjection.class));
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(2);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.DOUBLE));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));
        assertThat(topN.limit(), is(TopN.NO_LIMIT));
    }

    @Test
    public void testInsertFromQueryWithPartitionedColumn() throws Exception {
        Merge planNode = plan(
            "insert into users (id, date) (select id, date from parted)");
        Collect queryAndFetch = (Collect) planNode.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) queryAndFetch.collectPhase());
        List<Symbol> toCollect = collectPhase.toCollect();
        assertThat(toCollect.size(), is(2));
        assertThat(toCollect.get(0), isFunction("to_long"));
        assertThat(((Function) toCollect.get(0)).arguments().get(0), isReference("_doc['id']"));
        assertThat((Reference) toCollect.get(1), equalTo(new Reference(
            new ReferenceIdent(new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "parted"), "date"), RowGranularity.PARTITION, DataTypes.TIMESTAMP)));
    }

    @Test
    public void testGroupByHavingInsertInto() throws Exception {
        Merge planNode = plan(
            "insert into users (id, name) (select name, count(*) from users group by name having count(*) > 3)");
        DistributedGroupBy groupByNode = (DistributedGroupBy) planNode.subPlan();
        MergePhase mergeNode = groupByNode.reducerMergeNode();
        assertThat(mergeNode.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));

        FilterProjection filterProjection = (FilterProjection) mergeNode.projections().get(1);
        assertThat(filterProjection.outputs().size(), is(2));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(filterProjection.outputs().get(1), instanceOf(InputColumn.class));

        InputColumn inputColumn = (InputColumn) filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));
        inputColumn = (InputColumn) filterProjection.outputs().get(1);
        assertThat(inputColumn.index(), is(1));
        MergePhase localMergeNode = planNode.mergePhase();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(MergeCountProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

    }

    @Test
    public void testGlobalAggregationHaving() throws Exception {
        Merge globalAggregate = plan(
            "select avg(date) from users having min(date) > '1970-01-01'");
        Collect collect = (Collect) globalAggregate.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(AggregationProjection.class));

        MergePhase localMergeNode = globalAggregate.mergePhase();

        assertThat(localMergeNode.projections(), contains(
            instanceOf(AggregationProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(TopNProjection.class)));

        AggregationProjection aggregationProjection = (AggregationProjection) localMergeNode.projections().get(0);
        assertThat(aggregationProjection.aggregations().size(), is(2));

        FilterProjection filterProjection = (FilterProjection) localMergeNode.projections().get(1);
        assertThat(filterProjection.outputs().size(), is(2));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn) filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        TopNProjection topNProjection = (TopNProjection) localMergeNode.projections().get(2);
        assertThat(topNProjection.outputs().size(), is(1));
    }

    @Test
    public void testCountOnPartitionedTable() throws Exception {
        CountPlan plan = plan("select count(*) from parted where date = 123");
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
    public void testOrderByOnAnalyzed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'text': sorting on analyzed/fulltext columns is not possible");
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
        expectedException.expectMessage("Cannot ORDER BY 'no_index': sorting on non-indexed columns is not possible");
        plan("select no_index from users u order by 1");
    }

    @Test
    public void testSelectAnalyzedReferenceInFunctionAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select analyzed column 'text' within grouping or aggregations");
        plan("select min(substr(text, 0, 2)) from users");
    }

    @Test
    public void testSelectNonIndexedReferenceInFunctionAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select non-indexed column 'no_index' within grouping or aggregations");
        plan("select min(substr(no_index, 0, 2)) from users");
    }

    @Test
    public void testGlobalAggregateWithWhereOnPartitionColumn() throws Exception {
        Merge globalAggregate = plan(
            "select min(name) from parted where date > 0");
        Collect collect = (Collect) globalAggregate.subPlan();

        WhereClause whereClause = ((RoutedCollectPhase) collect.collectPhase()).whereClause();
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.noMatch(), is(false));
    }

    private void assertNoop(Plan plan) {
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testHasNoResultFromHaving() throws Exception {
        plan("select min(name) from users having 1 = 2");
        // TODO:
    }

    @Test
    public void testHasNoResultFromLimit() {
        // TODO:
        plan("select count(*) from users limit 1 offset 1");
        plan("select count(*) from users limit 5 offset 1");
        plan("select count(*) from users limit 0");
        plan("select * from users order by name limit 0");
        plan("select * from users order by name limit 0 offset 0");
    }

    @Test
    public void testHasNoResultFromQuery() {
        // TODO:
        plan("select name from users where false");
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKey() throws Exception {
        UpsertById node = plan("insert into users (id, name) values (1, null) on duplicate key update name = values(name)");

        assertThat(node.updateColumns(), is(new String[]{"name"}));

        assertThat(node.insertColumns().length, is(2));
        Reference idRef = node.insertColumns()[0];
        assertThat(idRef.ident().columnIdent().fqn(), is("id"));
        Reference nameRef = node.insertColumns()[1];
        assertThat(nameRef.ident().columnIdent().fqn(), is("name"));

        assertThat(node.items().size(), is(1));
        UpsertById.Item item = node.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("1"));
        assertThat(item.routing(), is("1"));

        assertThat(item.insertValues().length, is(2));
        assertThat((Long) item.insertValues()[0], is(1L));
        assertNull(item.insertValues()[1]);

        assertThat(item.updateAssignments().length, is(1));
        assertThat(item.updateAssignments()[0], isLiteral(null, DataTypes.STRING));
    }

    @Test
    public void testGroupByOnClusteredByColumnPartitionedOnePartition() throws Exception {
        // only one partition hit
        Merge optimizedPlan = plan("select count(*), city from clustered_parted where date=1395874800000 group by city");
        Collect collect = (Collect) optimizedPlan.subPlan();

        assertThat(collect.collectPhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(TopNProjection.class)));
        assertThat(collect.collectPhase().projections().get(0), instanceOf(GroupProjection.class));

        assertThat(optimizedPlan.mergePhase().projections().size(), is(0));

        // > 1 partition hit
        Plan plan = plan("select count(*), city from clustered_parted where date=1395874800000 or date=1395961200000 group by city");
        assertThat(plan, instanceOf(Merge.class));
        assertThat(((Merge) plan).subPlan(), instanceOf(DistributedGroupBy.class));
    }

    @Test
    public void testIndices() throws Exception {
        TableIdent custom = new TableIdent("custom", "table");
        String[] indices = Planner.indices(TestingTableInfo.builder(custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("custom.table"));

        indices = Planner.indices(TestingTableInfo.builder(new TableIdent(null, "table"), shardRouting("t1")).add("id", DataTypes.INTEGER, null).build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("table"));

        indices = Planner.indices(TestingTableInfo.builder(custom, shardRouting("t1"))
            .add("id", DataTypes.INTEGER, null)
            .add("date", DataTypes.TIMESTAMP, null, true)
            .addPartitions(new PartitionName(custom, Arrays.asList(new BytesRef("0"))).asIndexName())
            .addPartitions(new PartitionName(custom, Arrays.asList(new BytesRef("12345"))).asIndexName())
            .build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("custom..partitioned.table.04130", "custom..partitioned.table.04332chj6gqg"));
    }

    @Test
    public void testBuildReaderAllocations() throws Exception {
        TableIdent custom = new TableIdent("custom", "t1");
        TableInfo tableInfo = TestingTableInfo.builder(
            custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build();
        Planner.Context plannerContext = new Planner.Context(planner,
            clusterService, UUID.randomUUID(), null, normalizer, new TransactionContext(SessionContext.SYSTEM_SESSION), 0, 0);
        plannerContext.allocateRouting(tableInfo, WhereClause.MATCH_ALL, null);

        Planner.Context.ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();

        assertThat(readerAllocations.indices().size(), is(1));
        assertThat(readerAllocations.indices().get(0), is("t1"));
        assertThat(readerAllocations.nodeReaders().size(), is(2));

        IntSet n1 = readerAllocations.nodeReaders().get("nodeOne");
        assertThat(n1.size(), is(2));
        assertTrue(n1.contains(1));
        assertTrue(n1.contains(2));

        IntSet n2 = readerAllocations.nodeReaders().get("nodeTwo");
        assertThat(n2.size(), is(2));
        assertTrue(n2.contains(3));
        assertTrue(n2.contains(4));

        assertThat(readerAllocations.bases().get("t1"), is(0));

        // allocations must stay same on multiple calls
        Planner.Context.ReaderAllocations readerAllocations2 = plannerContext.buildReaderAllocations();
        assertThat(readerAllocations, is(readerAllocations2));
    }

    @Test
    public void testAllocateRouting() throws Exception {
        TableIdent custom = new TableIdent("custom", "t1");
        TableInfo tableInfo1 =
            TestingTableInfo.builder(custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build();
        TableInfo tableInfo2 =
            TestingTableInfo.builder(custom, shardRoutingForReplicas("t1")).add("id", DataTypes.INTEGER, null).build();
        Planner.Context plannerContext =
            new Planner.Context(planner, clusterService, UUID.randomUUID(), null, normalizer, new TransactionContext(SessionContext.SYSTEM_SESSION), 0, 0);

        WhereClause whereClause = new WhereClause(
            new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME,
                                  Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.INTEGER)),
                DataTypes.BOOLEAN),
                         Arrays.asList(tableInfo1.getReference(new ColumnIdent("id")), Literal.of(2))
            ));

        plannerContext.allocateRouting(tableInfo1, WhereClause.MATCH_ALL, null);
        plannerContext.allocateRouting(tableInfo2, whereClause, null);

        // 2 routing allocations with different where clause must result in 2 allocated routings
        java.lang.reflect.Field tableRoutings = Planner.Context.class.getDeclaredField("tableRoutings");
        tableRoutings.setAccessible(true);
        Multimap<TableIdent, Planner.TableRouting> routing =
            (Multimap<TableIdent, Planner.TableRouting>) tableRoutings.get(plannerContext);
        assertThat(routing.size(), is(2));

        // The routings must be the same after merging the locations
        Iterator<Planner.TableRouting> iterator = routing.values().iterator();
        Routing routing1 = iterator.next().routing;
        Routing routing2 = iterator.next().routing;
        assertThat(routing1, is(routing2));
    }

    @Test
    public void testExecutionPhaseIdSequence() throws Exception {
        Planner.Context plannerContext = new Planner.Context(planner,
            clusterService, UUID.randomUUID(), null, normalizer, new TransactionContext(SessionContext.SYSTEM_SESSION.SYSTEM_SESSION), 0, 0);

        assertThat(plannerContext.nextExecutionPhaseId(), is(0));
        assertThat(plannerContext.nextExecutionPhaseId(), is(1));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testLimitThatIsBiggerThanPageSizeCausesQTFPUshPlan() throws Exception {
        QueryThenFetch qtf = plan("select * from users limit 2147483647 ");
        Merge merge = (Merge) qtf.subPlan();
        assertThat(merge.mergePhase().nodeIds().size(), is(1));

        qtf = plan("select * from users limit 2");
        merge = (Merge) qtf.subPlan();
        assertThat(merge.mergePhase().nodeIds().size(), is(0));
    }

    @Test
    public void testKillPlanAll() throws Exception {
        KillPlan killPlan = plan("kill all");
        assertThat(killPlan, instanceOf(KillPlan.class));
        assertThat(killPlan.jobId(), notNullValue());
        assertThat(killPlan.jobToKill().isPresent(), is(false));
    }

    @Test
    public void testKillPlanJobs() throws Exception {
        KillPlan killJobsPlan = plan("kill '6a3d6fb6-1401-4333-933d-b38c9322fca7'");
        assertThat(killJobsPlan.jobId(), notNullValue());
        assertThat(killJobsPlan.jobToKill().get().toString(), is("6a3d6fb6-1401-4333-933d-b38c9322fca7"));
    }

    @Test
    public void testShardQueueSizeCalculation() throws Exception {
        Merge merge = plan("select name from users order by name limit 100");
        Collect collect = (Collect) merge.subPlan();
        int shardQueueSize = ((RoutedCollectPhase) collect.collectPhase()).shardQueueSize(
            collect.collectPhase().nodeIds().iterator().next());
        assertThat(shardQueueSize, is(75));
    }

    @Test
    public void testQAFPagingIsEnabledOnHighLimit() throws Exception {
        Merge plan = plan("select name from users order by name limit 1000000");
        assertThat(plan.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled

        Collect collect = (Collect) plan.subPlan();
        assertThat(((RoutedCollectPhase) collect.collectPhase()).nodePageSizeHint(), is(750000));
    }

    @Test
    public void testQAFPagingIsEnabledOnHighOffset() throws Exception {
        Merge merge = plan("select name from users order by name limit 10 offset 1000000");
        Collect collect = (Collect) merge.subPlan();
        assertThat(merge.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled
        assertThat(((RoutedCollectPhase) collect.collectPhase()).nodePageSizeHint(), is(750007));
    }

    @Test
    public void testQTFPagingIsEnabledOnHighLimit() throws Exception {
        QueryThenFetch qtf = plan("select name, date from users order by name limit 1000000");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(merge.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled
        assertThat(collectPhase.nodePageSizeHint(), is(750000));
    }

    @Test
    public void testSelectFromUnnestResultsInTableFunctionPlan() throws Exception {
        Collect collect = plan("select * from unnest([1, 2], ['Arthur', 'Trillian'])");
        assertNotNull(collect);
        assertThat(collect.collectPhase().toCollect(), contains(isReference("col1"), isReference("col2")));
    }

    @Test
    public void testSoftLimitIsApplied() throws Exception {
        QueryThenFetch qtf = plan("select * from users", 0, 10);
        Merge merge = (Merge) qtf.subPlan();
        assertThat(merge.mergePhase().projections(),
            contains(instanceOf(TopNProjection.class), instanceOf(FetchProjection.class)));
        TopNProjection topNProjection = (TopNProjection) merge.mergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(10));

        qtf = plan("select * from users limit 5", 0, 10);
        merge = (Merge) qtf.subPlan();
        assertThat(merge.mergePhase().projections(), contains(instanceOf(TopNProjection.class), instanceOf(FetchProjection.class)));
        topNProjection = (TopNProjection) merge.mergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(5));
    }

    @Test
    public void testNestedGroupByAggregation() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot create plan for: ");
        plan("select count(*) from (" +
             "  select max(load['1']) as maxLoad, hostname " +
             "  from sys.nodes " +
             "  group by hostname having max(load['1']) > 50) as nodes " +
             "group by hostname");
    }

    @Test
    public void testReferenceToNestedAggregatedField() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot create plan for: ");
        plan("select ii, xx from ( " +
             "  select i + i as ii, xx from (" +
             "    select i, sum(x) as xx from t1 group by i) as t) as tt " +
             "where (ii * 2) > 4 and (xx * 2) > 120");
    }

    @Test
    public void test3TableJoinQuerySplitting() throws Exception {
        QueryThenFetch qtf = plan("select" +
                                  "  u1.id as u1, " +
                                  "  u2.id as u2, " +
                                  "  u3.id as u3 " +
                                  "from " +
                                  "  users u1," +
                                  "  users u2," +
                                  "  users u3 " +
                                  "where " +
                                  "  u1.name = 'Arthur'" +
                                  "  and u2.id = u1.id" +
                                  "  and u2.name = u1.name");
        NestedLoop outerNl = (NestedLoop) qtf.subPlan();
        NestedLoop innerNl = (NestedLoop) outerNl.left();

        assertThat(((FilterProjection) innerNl.nestedLoopPhase().projections().get(0)).query(),
            isSQL("((INPUT(2) = INPUT(0)) AND (INPUT(3) = INPUT(1)))"));
    }

    @Test
    public void testOuterJoinToInnerJoinRewrite() throws Exception {
        QueryThenFetch qtf = plan("select u1.text, u2.text " +
                                  "from users u1 left join users u2 on u1.id = u2.id " +
                                  "where u2.name = 'Arthur'" +
                                  "and u2.id > 1 ");
        NestedLoop nl = (NestedLoop) qtf.subPlan();
        assertThat(nl.nestedLoopPhase().joinType(), is(JoinType.INNER));
        Collect rightCM = (Collect) nl.right();
        assertThat(((RoutedCollectPhase) rightCM.collectPhase()).whereClause().query(),
            isSQL("((doc.users.name = 'Arthur') AND (doc.users.id > 1))"));

        // doesn't contain "name" because whereClause is pushed down,
        // but still contains "id" because it is in the joinCondition
        assertThat(rightCM.collectPhase().toCollect(), contains(isReference("_fetchid"), isReference("id")));
    }

    @Test
    public void testNoSoftLimitOnUnlimitedChildRelation() throws Exception {
        int softLimit = 10_000;
        Planner.Context plannerContext = new Planner.Context(planner,
            clusterService, UUID.randomUUID(), null, normalizer, new TransactionContext(SessionContext.SYSTEM_SESSION), softLimit, 0);
        Limits limits = plannerContext.getLimits(new QuerySpec());
        assertThat(limits.finalLimit(), is(TopN.NO_LIMIT));
    }
}
