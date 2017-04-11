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

import io.crate.analyze.TableDefinitions;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.projection.*;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.SymbolMatchers.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;

public class InsertPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addDocTable(TableDefinitions.PARTED_PKS_TI)
            .build();
    }

    @Test
    public void testInsertPlan() throws Exception {
        UpsertById upsertById = e.plan("insert into users (id, name) values (42, 'Deep Thought')");

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
        assertThat(item.insertValues()[0], is(42L));
        assertThat(item.insertValues()[1], is(new BytesRef("Deep Thought")));
    }

    @Test
    public void testInsertPlanMultipleValues() throws Exception {
        UpsertById upsertById = e.plan("insert into users (id, name) values (42, 'Deep Thought'), (99, 'Marvin')");

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
        assertThat(item1.insertValues()[0], is(42L));
        assertThat(item1.insertValues()[1], is(new BytesRef("Deep Thought")));

        UpsertById.Item item2 = upsertById.items().get(1);
        assertThat(item2.index(), is("users"));
        assertThat(item2.id(), is("99"));
        assertThat(item2.routing(), is("99"));
        assertThat(item2.insertValues().length, is(2));
        assertThat(item2.insertValues()[0], is(99L));
        assertThat(item2.insertValues()[1], is(new BytesRef("Marvin")));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupBy() throws Exception {
        Merge nonDistributedGroupBy = e.plan(
            "insert into users (id, name) (select count(*), name from sys.nodes group by name)");
        MergePhase mergePhase = nonDistributedGroupBy.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupByWithCast() throws Exception {
        Merge nonDistributedGroupBy = e.plan(
            "insert into users (id, name) (select name, count(*) from sys.nodes group by name)");
        MergePhase mergePhase = nonDistributedGroupBy.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByWithLimit() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Using limit, offset or order by is not supported on insert using a sub-query");

        e.plan("insert into users (id, name) (select name, count(*) from users group by name order by name limit 10)");
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupByWithoutLimit() throws Exception {
        Merge planNode = e.plan(
            "insert into users (id, name) (select name, count(*) from users group by name)");
        DistributedGroupBy groupBy = (DistributedGroupBy) planNode.subPlan();
        MergePhase mergePhase = groupBy.reducerMergeNode();
        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));

        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergePhase.projections().get(2);
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
        Merge planNode = e.plan(
            "insert into parted_pks (id, date) (select id, date from users group by id, date)");
        DistributedGroupBy groupBy = (DistributedGroupBy) planNode.subPlan();
        MergePhase mergePhase = groupBy.reducerMergeNode();
        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergePhase.projections().get(2);
        assertThat(projection.primaryKeys().size(), is(2));
        assertThat(projection.primaryKeys().get(0).fqn(), is("id"));
        assertThat(projection.primaryKeys().get(1).fqn(), is("date"));

        assertThat(projection.columnReferences().size(), is(1));
        assertThat(projection.columnReferences().get(0).ident().columnIdent().fqn(), is("id"));

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
    public void testInsertFromSubQueryGlobalAggregate() throws Exception {
        Merge globalAggregate = e.plan(
            "insert into users (name, id) (select arbitrary(name), count(*) from users)");
        MergePhase mergePhase = globalAggregate.mergePhase();
        assertThat(mergePhase.projections().size(), is(3));
        assertThat(mergePhase.projections().get(1), instanceOf(EvalProjection.class));

        assertThat(mergePhase.projections().get(2), instanceOf(ColumnIndexWriterProjection.class));
        ColumnIndexWriterProjection projection = (ColumnIndexWriterProjection) mergePhase.projections().get(2);

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
        Merge merge = e.plan(
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
        NestedLoop nestedLoop = e.plan(
            "insert into users (id, name) (select u1.id, u2.name from users u1 CROSS JOIN users u2)");
        assertThat(nestedLoop.nestedLoopPhase().projections(), contains(
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)
        ));

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

        e.plan("insert into users (date, id, name) (select date, id, name from users limit 10)");
    }

    @Test
    public void testInsertFromSubQueryWithOffset() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Using limit, offset or order by is not supported on insert using a sub-query");

        e.plan("insert into users (id, name) (select id, name from users offset 10)");
    }

    @Test
    public void testInsertFromSubQueryWithOrderBy() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Using limit, offset or order by is not supported on insert using a sub-query");

        e.plan("insert into users (date, id, name) (select date, id, name from users order by id)");
    }

    @Test
    public void testInsertFromSubQueryWithoutLimit() throws Exception {
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
    public void testInsertFromSubQueryReduceOnCollectorGroupBy() throws Exception {
        Merge merge = e.plan(
            "insert into users (id, name) (select id, arbitrary(name) from users group by id)");
        Collect collect = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
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
        Merge merge = e.plan(
            "insert into users (id, name) (select id, count(*) from users group by id)");
        Collect nonDistributedGroupBy = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) nonDistributedGroupBy.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(ColumnIndexWriterProjection.class)));
        EvalProjection collectTopN = (EvalProjection) collectPhase.projections().get(1);
        assertThat(collectTopN.outputs(), contains(isInputColumn(0), isFunction("to_string")));

        ColumnIndexWriterProjection columnIndexWriterProjection = (ColumnIndexWriterProjection) collectPhase.projections().get(2);
        assertThat(columnIndexWriterProjection.columnReferences(), contains(isReference("id"), isReference("name")));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections(), contains(instanceOf(MergeCountProjection.class)));

    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKey() throws Exception {
        UpsertById node = e.plan("insert into users (id, name) values (1, null) on duplicate key update name = values(name)");

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
        assertThat(item.insertValues()[0], is(1L));
        assertNull(item.insertValues()[1]);

        assertThat(item.updateAssignments().length, is(1));
        assertThat(item.updateAssignments()[0], isLiteral(null, DataTypes.STRING));
    }

    @Test
    public void testInsertFromQueryWithPartitionedColumn() throws Exception {
        Merge planNode = e.plan(
            "insert into users (id, date) (select id, date from parted_pks)");
        Collect queryAndFetch = (Collect) planNode.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) queryAndFetch.collectPhase());
        List<Symbol> toCollect = collectPhase.toCollect();
        assertThat(toCollect.size(), is(2));
        assertThat(toCollect.get(0), isFunction("to_long"));
        assertThat(((Function) toCollect.get(0)).arguments().get(0), isReference("_doc['id']"));
        assertThat(toCollect.get(1), equalTo(new Reference(
            new ReferenceIdent(TableDefinitions.PARTED_PKS_IDENT, "date"), RowGranularity.PARTITION, DataTypes.TIMESTAMP)));
    }

    @Test
    public void testGroupByHavingInsertInto() throws Exception {
        Merge planNode = e.plan(
            "insert into users (id, name) (select name, count(*) from users group by name having count(*) > 3)");
        DistributedGroupBy groupByNode = (DistributedGroupBy) planNode.subPlan();
        MergePhase mergePhase = groupByNode.reducerMergeNode();
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
}
