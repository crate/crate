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

package io.crate.planner;

import static io.crate.testing.Asserts.isReference;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.OrderedLimitAndOffsetProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.IndexType;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.operators.InsertFromValues;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class InsertPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .setNumNodes(2)
            .build()
            .addPartitionedTable(
                "create table parted_pks (" +
                "   id int," +
                "   name string," +
                "   date timestamp with time zone," +
                "   obj object," +
                "   primary key (id, date)" +
                ") partitioned by (date) clustered by (id) ",
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395961200000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList(null)).asIndexName()
            )
            .addTable(
                "create table users (" +
                "   id long primary key," +
                "   name string," +
                "   date timestamp with time zone" +
                ") clustered into 4 shards")
            .addTable("create table source (id int primary key, name string)")
            .addPartitionedTable("CREATE TABLE double_parted(x int, y int) PARTITIONED BY (x, y)");
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupBy() {
        Collect nonDistributedGroupBy = e.plan(
            "insert into users (id, name) (select count(*), name from sys.nodes group by name)");
        assertThat("nodeIds size must 1 one if there is no mergePhase", nonDistributedGroupBy.nodeIds().size(), is(1));
        assertThat(nonDistributedGroupBy.collectPhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupByWithCast() {
        Collect nonDistributedGroupBy = e.plan(
            "insert into users (id, name) (select name, count(*) from sys.nodes group by name)");
        assertThat("nodeIds size must 1 one if there is no mergePhase", nonDistributedGroupBy.nodeIds().size(), is(1));
        assertThat(nonDistributedGroupBy.collectPhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByWithLimit() {
        Merge localMerge = e.plan("insert into users (id, name) " +
                             "(select name, count(*) from users group by name order by name limit 10)");

        Merge distMerge = (Merge) localMerge.subPlan();
        Collect collect = (Collect) distMerge.subPlan();
        assertThat(
            collect.collectPhase().projections(),
            contains(instanceOf(GroupProjection.class))
        );
        assertThat(
            distMerge.mergePhase().projections(),
            contains(
                instanceOf(GroupProjection.class),
                instanceOf(OrderedLimitAndOffsetProjection.class),
                instanceOf(EvalProjection.class)
            )
        );
        assertThat(
            localMerge.mergePhase().projections(),
            contains(
                instanceOf(LimitAndOffsetProjection.class),
                instanceOf(ColumnIndexWriterProjection.class)
            )
        );
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByWithoutLimit() {
        Merge planNode = e.plan(
            "insert into users (id, name) (select name, count(*) from users group by name)");
        Merge groupBy = (Merge) planNode.subPlan();
        MergePhase mergePhase = groupBy.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));

        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergePhase.projections().get(2);
        assertThat(projection.primaryKeys().size(), is(1));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.allTargetColumns().size(), is(2));
        assertThat(projection.allTargetColumns().get(0).column().fqn(), is("id"));
        assertThat(projection.allTargetColumns().get(1).column().fqn(), is("name"));

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
    public void testInsertFromSubQueryDistributedGroupByPartitioned() {
        Merge planNode = e.plan(
            "insert into parted_pks (id, date) (select id, date from users group by id, date)");
        Merge groupBy = (Merge) planNode.subPlan();
        MergePhase mergePhase = groupBy.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergePhase.projections().get(2);
        assertThat(projection.primaryKeys().size(), is(2));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.primaryKeys().get(1).fqn(), is("date"));

        assertThat(projection.allTargetColumns().size(), is(2));
        assertThat(projection.allTargetColumns().get(0).column().fqn(), is("id"));

        assertThat(projection.partitionedBySymbols().size(), is(1));
        assertThat(((InputColumn) projection.partitionedBySymbols().get(0)).index(), is(1));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableIdent().fqn(), is("doc.parted_pks"));

        MergePhase localMergeNode = planNode.mergePhase();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(MergeCountProjection.class));
        assertThat(localMergeNode.finalProjection().get().outputs().size(), is(1));

    }

    @Test
    public void testInsertFromSubQueryGlobalAggregate() {
        Merge globalAggregate = e.plan(
            "insert into users (name, id) (select arbitrary(name), count(*) from users)");
        MergePhase mergePhase = globalAggregate.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(AggregationProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)
        ));
        assertThat(mergePhase.projections().get(1), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergePhase.projections().get(1);

        assertThat(projection.allTargetColumns().size(), is(2));
        assertThat(projection.allTargetColumns().get(0).column().fqn(), is("name"));
        assertThat(projection.allTargetColumns().get(1).column().fqn(), is("id"));

        assertNotNull(projection.clusteredByIdent());
        assertThat(projection.clusteredByIdent().fqn(), is("id"));
        assertThat(projection.tableIdent().fqn(), is("doc.users"));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));
    }

    @Test
    public void testInsertFromSubQueryESGet() {
        Merge merge = e.plan(
            "insert into users (date, id, name) (select date, id, name from users where id=1)");
        Collect queryAndFetch = (Collect) merge.subPlan();
        PKLookupPhase collectPhase = ((PKLookupPhase) queryAndFetch.collectPhase());

        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) collectPhase.projections().get(0);

        assertThat(projection.allTargetColumns().size(), is(3));
        assertThat(projection.allTargetColumns().get(0).column().fqn(), is("date"));
        assertThat(projection.allTargetColumns().get(1).column().fqn(), is("id"));
        assertThat(projection.allTargetColumns().get(2).column().fqn(), is("name"));
        assertThat(((InputColumn) projection.ids().get(0)).index(), is(1));
        assertThat(((InputColumn) projection.clusteredBy()).index(), is(1));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));
    }

    @Test
    public void testInsertFromSubQueryJoin() {
        Join join = e.plan(
            "insert into users (id, name) (select u1.id, u2.name from users u1 CROSS JOIN users u2)");
        assertThat(join.joinPhase().projections(), contains(
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)
        ));

        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) join.joinPhase().projections().get(1);

        assertThat(projection.allTargetColumns().size(), is(2));
        assertThat(projection.allTargetColumns().get(0).column().fqn(), is("id"));
        assertThat(projection.allTargetColumns().get(1).column().fqn(), is("name"));
        assertThat(((InputColumn) projection.ids().get(0)).index(), is(0));
        assertThat(((InputColumn) projection.clusteredBy()).index(), is(0));
        assertThat(projection.partitionedBySymbols().isEmpty(), is(true));
    }

    @Test
    public void testInsertFromSubQueryWithLimit() {
        QueryThenFetch qtf = e.plan("insert into users (date, id, name) (select date, id, name from users limit 10)");
        Merge merge = (Merge) qtf.subPlan();
        Collect collect = (Collect) merge.subPlan();
        assertThat(collect.collectPhase().projections(), contains(instanceOf(LimitAndOffsetProjection.class)));
        assertThat(
            merge.mergePhase().projections(),
            contains(
                instanceOf(LimitAndOffsetProjection.class),
                instanceOf(FetchProjection.class),
                instanceOf(ColumnIndexWriterProjection.class)
            )
        );
    }

    @Test
    public void testInsertFromSubQueryWithOffsetDoesTableWriteOnCollect() {
        QueryThenFetch qtf = e.plan("insert into users (id, name) (select id, name from users offset 10)");
        Merge merge = (Merge) qtf.subPlan();
        // We can ignore the offset since SQL semantics don't promise a deterministic order without explicit order by clause
        Collect collect = (Collect) merge.subPlan();
        assertThat(collect.collectPhase().projections(), Matchers.empty());
        assertThat(merge.mergePhase().projections(), contains(
            instanceOf(FetchProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)
        ));
    }

    @Test
    public void testInsertFromSubQueryWithOrderBy() {
        Merge merge = e.plan("insert into users (date, id, name) (select date, id, name from users order by id)");
        Collect collect = (Collect) merge.subPlan();
        assertThat(collect.collectPhase().projections(), contains(instanceOf(ColumnIndexWriterProjection.class)));
        assertThat(merge.mergePhase().projections(), contains(instanceOf(MergeCountProjection.class)));
    }

    @Test
    public void testInsertFromSubQueryWithoutLimit() {
        Merge planNode = e.plan(
            "insert into users (id, name) (select id, name from users)");
        Collect collect = (Collect) planNode.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(ColumnIndexWriterProjection.class));

        MergePhase localMergeNode = planNode.mergePhase();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(MergeCountProjection.class));
    }

    @Test
    public void testInsertFromSubQueryReduceOnCollectorGroupBy() {
        Merge merge = e.plan(
            "insert into users (id, name) (select id, arbitrary(name) from users group by id)");
        Collect collect = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)
        ));
        ColumnIndexWriterProjection columnIndexWriterProjection =
            (ColumnIndexWriterProjection) collectPhase.projections().get(1);
        Asserts.assertThat(columnIndexWriterProjection.allTargetColumns()).satisfiesExactly(
            isReference("id"), isReference("name"));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections(), contains(instanceOf(MergeCountProjection.class)));
    }

    @Test
    public void testInsertFromSubQueryReduceOnCollectorGroupByWithCast() {
        Merge merge = e.plan(
            "insert into users (id, name) (select id, count(*) from users group by id)");
        Collect nonDistributedGroupBy = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) nonDistributedGroupBy.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
        EvalProjection projection = (EvalProjection) collectPhase.projections().get(1);
        Asserts.assertThat(projection.outputs())
            .satisfiesExactly(
                s -> Asserts.assertThat(s).isInputColumn(0),
                s -> Asserts.assertThat(s).isFunction(
                    ImplicitCastFunction.NAME,
                    List.of(DataTypes.LONG, DataTypes.STRING)));

        ColumnIndexWriterProjection columnIndexWriterProjection = (ColumnIndexWriterProjection) collectPhase.projections().get(2);
        Asserts.assertThat(columnIndexWriterProjection.allTargetColumns()).satisfiesExactly(
            isReference("id"), isReference("name"));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections(), contains(instanceOf(MergeCountProjection.class)));
    }

    @Test
    public void testInsertFromQueryWithPartitionedColumn() {
        Merge planNode = e.plan(
            "insert into users (id, date) (select id, date from parted_pks)");
        Collect queryAndFetch = (Collect) planNode.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) queryAndFetch.collectPhase());
        List<Symbol> toCollect = collectPhase.toCollect();
        assertThat(toCollect.size(), is(2));
        Asserts.assertThat(toCollect.get(0)).isReference().hasName("_doc['id']");
        assertThat(toCollect.get(1), equalTo(new SimpleReference(
            new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, "parted_pks"), "date"),
            RowGranularity.PARTITION,
            DataTypes.TIMESTAMPZ,
            ColumnPolicy.DYNAMIC,
            IndexType.PLAIN,
            false,
            true,
            3,
            3,
            false,
            null)));
    }

    @Test
    public void testGroupByHavingInsertInto() {
        Merge planNode = e.plan(
            "insert into users (id, name) (select name, count(*) from users group by name having count(*) > 3)");
        Merge groupByNode = (Merge) planNode.subPlan();
        MergePhase mergePhase = groupByNode.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));

        FilterProjection filterProjection = (FilterProjection) mergePhase.projections().get(1);
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
    public void testProjectionWithCastsIsAddedIfSourceTypeDoNotMatchTargetTypes() {
        Merge plan = e.plan("insert into users (id, name) (select id, name from source)");
        List<Projection> projections = ((Collect) plan.subPlan()).collectPhase().projections();
        assertThat(projections,
            contains(
                instanceOf(EvalProjection.class),
                instanceOf(ColumnIndexWriterProjection.class))
        );
        Asserts.assertThat(projections.get(0).outputs())
            .satisfiesExactly(
                s -> Asserts.assertThat(s).isFunction(
                    ImplicitCastFunction.NAME,
                    List.of(DataTypes.INTEGER, DataTypes.STRING)),
                s -> Asserts.assertThat(s).isInputColumn(1));
    }

    @Test
    public void test_insert_from_sub_query_with_sys_tables_has_no_doc_lookup() {
        Collect collect = e.plan("insert into users (id, name) (select oid, typname from pg_catalog.pg_type)");
        Asserts.assertThat(collect.collectPhase().toCollect()).satisfiesExactly(
            isReference("oid"),
            isReference("typname"));
    }

    @Test
    public void test_insert_from_query_rewritten_to_insert_from_values() {
        Plan plan = e.logicalPlan("insert into users (id, name) values (42, 'Deep Thought')");
        assertThat(plan, instanceOf(InsertFromValues.class));
    }

    @Test
    public void test_insert_select_distinct() throws Exception {
        Merge merge = e.plan("insert into users (id) (select distinct id from users)");
        Collect collect = (Collect) merge.subPlan();
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(
            projections,
            contains(
                instanceOf(GroupProjection.class),
                instanceOf(ColumnIndexWriterProjection.class)
            )
        );
        assertThat(projections.get(0).requiredGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void test_insert_from_group_by_uses_doc_values() throws Exception {
        Merge merge = e.plan("insert into users (id) (select id from users group by 1)");
        Collect collect = (Collect) merge.subPlan();
        Asserts.assertThat(collect.collectPhase().toCollect()).satisfiesExactly(isReference("id"));
    }

    @Test
    public void test_insert_into_partitioned_table_with_less_columns_than_the_partition_by_ones() {
        Plan plan = e.logicalPlan("insert into double_parted (x) VALUES (1)");
        assertThat(plan, instanceOf(InsertFromValues.class));
    }
}
