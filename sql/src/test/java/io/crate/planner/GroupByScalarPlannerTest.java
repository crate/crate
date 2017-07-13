package io.crate.planner;


import com.google.common.collect.Iterables;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.metadata.RowGranularity;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.scalar.arithmetic.ArithmeticFunctions;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GroupByScalarPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .build();
    }

    @Test
    public void testGroupByWithScalarPlan() throws Exception {
        Merge merge = e.plan("select id + 1 from users group by id");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertEquals(DataTypes.LONG, collectPhase.outputTypes().get(0));
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(collectPhase.projections().get(1), instanceOf(EvalProjection.class));
        assertThat(collectPhase.projections().get(1).outputs().get(0), instanceOf(Function.class));
        assertThat(collectPhase.toCollect(), contains(isFunction("add"), isReference("id", DataTypes.LONG)));

        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.keys().get(0).valueType(), is(DataTypes.LONG));


        MergePhase mergePhase = merge.mergePhase();

        assertEquals(DataTypes.LONG, Iterables.get(mergePhase.inputTypes(), 0));
        assertEquals(DataTypes.LONG, mergePhase.outputTypes().get(0));
    }

    @Test
    public void testGroupByWithMultipleScalarPlan() throws Exception {
        Merge merge = e.plan("select abs(id + 1) from users group by id");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertEquals(DataTypes.LONG, collectPhase.outputTypes().get(0));
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(collectPhase.projections().get(1), instanceOf(EvalProjection.class));
        assertThat(collectPhase.projections().get(1).outputs().get(0), instanceOf(Function.class));
        assertThat(collectPhase.toCollect(), contains(isFunction("abs"), isReference("id", DataTypes.LONG)));

        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.keys().get(0).valueType(), is(DataTypes.LONG));

        MergePhase mergePhase = merge.mergePhase();

        assertEquals(DataTypes.LONG, Iterables.get(mergePhase.inputTypes(), 0));
        assertEquals(DataTypes.LONG, mergePhase.outputTypes().get(0));
    }

    @Test
    public void testGroupByScalarWithMultipleColumnArgumentsPlan() throws Exception {
        Merge merge = e.plan("select abs(id + other_id) from users group by id, other_id");
        Merge subplan = (Merge) merge.subPlan();
        Collect collect = (Collect) subplan.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(collectPhase.toCollect(), contains(isFunction("abs"), isReference("id", DataTypes.LONG), isReference("other_id", DataTypes.LONG)));

        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.keys().size(), is(2));
        assertThat(groupProjection.keys().get(0).valueType(), is(DataTypes.LONG));
        assertThat(groupProjection.keys().get(1).valueType(), is(DataTypes.LONG));

        MergePhase mergePhase = subplan.mergePhase();
        assertThat(mergePhase.projections().size(), is(2));
        assertThat(mergePhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergePhase.projections().get(1), instanceOf(EvalProjection.class));
    }
}
