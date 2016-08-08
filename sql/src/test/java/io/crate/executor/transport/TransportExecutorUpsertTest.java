/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.UpdateProjection;
import io.crate.testing.CollectingRowReceiver;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import java.util.*;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class TransportExecutorUpsertTest extends BaseTransportExecutorTest {

    @Test
    public void testInsertWithUpsertByIdTask() throws Exception {
        execute("create table characters (id int primary key, name string)");
        ensureYellow();

        /* insert into characters (id, name) values (99, 'Marvin'); */
        Planner.Context ctx = newPlannerContext();
        UpsertById plan = new UpsertById(
            ctx.jobId(),
            ctx.nextExecutionPhaseId(),
            false,
            0,
            null,
            new Reference[]{idRef, nameRef});
        plan.add("characters", "99", "99", null, null, new Object[]{99, new BytesRef("Marvin")});

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(plan, rowReceiver);
        Bucket rows = rowReceiver.result();
        assertThat(rows, contains(isRow(1L)));

        // verify insertion
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Plan getPlan = newGetNode("characters", outputs, "99", ctx.nextExecutionPhaseId());
        rowReceiver = new CollectingRowReceiver();
        executor.execute(getPlan, rowReceiver);
        Bucket bucket = rowReceiver.result();

        assertThat(bucket, contains(isRow(99, "Marvin")));
    }

    @Test
    public void testInsertIntoPartitionedTableWithUpsertByIdTask() throws Exception {
        execute("create table parted (" +
            "  id int, " +
            "  name string, " +
            "  date timestamp" +
            ") partitioned by (date)");
        ensureGreen();

        /* insert into parted (id, name, date) values(0, 'Trillian', 13959981214861); */
        Planner.Context ctx = newPlannerContext();
        UpsertById upsertById = new UpsertById(
            ctx.jobId(),
            ctx.nextExecutionPhaseId(),
            true,
            0,
            null,
            new Reference[]{idRef, nameRef});

        PartitionName partitionName = new PartitionName("parted", Collections.singletonList(new BytesRef("13959981214861")));
        upsertById.add(partitionName.asIndexName(), "123", "123", null, null, new Object[]{0L, new BytesRef("Trillian")});

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(upsertById, rowReceiver);

        assertThat(rowReceiver.result(), contains(isRow(1L)));

        refresh();

        assertTrue(
            client().admin().indices().prepareExists(partitionName.asIndexName())
                .execute().actionGet().isExists()
        );
        assertTrue(
            client().admin().indices().prepareAliasesExist("parted")
                .execute().actionGet().exists()
        );
        SearchHits hits = client().prepareSearch(partitionName.asIndexName())
            .setTypes(Constants.DEFAULT_MAPPING_TYPE)
            .addFields("id", "name")
            .setQuery(new MapBuilder<String, Object>()
                .put("match_all", new HashMap<String, Object>())
                .map()
            ).execute().actionGet().getHits();
        assertThat(hits.getTotalHits(), is(1L));
        assertThat((Integer) hits.getHits()[0].field("id").getValues().get(0), is(0));
        assertThat((String) hits.getHits()[0].field("name").getValues().get(0), is("Trillian"));
    }

    @Test
    public void testInsertMultiValuesWithUpsertByIdTask() throws Exception {
        execute("create table characters (id int primary key, name string)");
        ensureYellow();

        /* insert into characters (id, name) values (99, 'Marvin'), (42, 'Deep Thought'); */
        Planner.Context ctx = newPlannerContext();
        UpsertById upsertById = new UpsertById(
            ctx.jobId(),
            ctx.nextExecutionPhaseId(),
            false,
            0,
            null,
            new Reference[]{idRef, nameRef});

        upsertById.add("characters", "99", "99", null, null, new Object[]{99, new BytesRef("Marvin")});
        upsertById.add("characters", "42", "42", null, null, new Object[]{42, new BytesRef("Deep Thought")});

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(upsertById, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(2L)));

        // verify insertion
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Plan getPlan = newGetNode("characters", outputs, Arrays.asList("99", "42"), ctx.nextExecutionPhaseId());

        rowReceiver = new CollectingRowReceiver();
        executor.execute(getPlan, rowReceiver);

        //noinspection unchecked
        assertThat(rowReceiver.result(), contains(
            isRow(99, "Marvin"),
            isRow(42, "Deep Thought")
        ));
    }

    @Test
    public void testUpdateWithUpsertByIdTask() throws Exception {
        setup.setUpCharacters();

        // update characters set name='Vogon lyric fan' where id=1
        Planner.Context ctx = newPlannerContext();
        UpsertById upsertById = new UpsertById(
            ctx.jobId(),
            ctx.nextExecutionPhaseId(),
            false,
            0,
            new String[]{nameRef.ident().columnIdent().fqn()},
            null);
        upsertById.add("characters", "1", "1", new Symbol[]{Literal.newLiteral("Vogon lyric fan")}, null);

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(upsertById, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(1L)));

        // verify update
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Plan getPlan = newGetNode("characters", outputs, "1", ctx.nextExecutionPhaseId());
        rowReceiver = new CollectingRowReceiver();
        executor.execute(getPlan, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(1, "Vogon lyric fan")));
    }

    @Test
    public void testInsertOnDuplicateWithUpsertByIdTask() throws Exception {
        setup.setUpCharacters();
        /* insert into characters (id, name, female) values (5, 'Zaphod Beeblebrox', false)
           on duplicate key update set name = 'Zaphod Beeblebrox'; */
        Object[] missingAssignments = new Object[]{5, new BytesRef("Zaphod Beeblebrox"), false};
        Planner.Context ctx = newPlannerContext();
        UpsertById upsertById = new UpsertById(
            ctx.jobId(),
            ctx.nextExecutionPhaseId(),
            false,
            0,
            new String[]{nameRef.ident().columnIdent().fqn()},
            new Reference[]{idRef, nameRef, femaleRef});

        upsertById.add("characters", "5", "5", new Symbol[]{Literal.newLiteral("Zaphod Beeblebrox")}, null, missingAssignments);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(upsertById, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(1L)));

        // verify insert
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef, femaleRef);
        Plan getPlan = newGetNode("characters", outputs, "5", ctx.nextExecutionPhaseId());
        rowReceiver = new CollectingRowReceiver();
        executor.execute(getPlan, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(5, "Zaphod Beeblebrox", false)));
    }

    @Test
    public void testUpdateOnDuplicateWithUpsertByIdTask() throws Exception {
        setup.setUpCharacters();
        /* insert into characters (id, name, female) values (1, 'Zaphod Beeblebrox', false)
           on duplicate key update set name = 'Zaphod Beeblebrox'; */
        Object[] missingAssignments = new Object[]{1, new BytesRef("Zaphod Beeblebrox"), true};
        Planner.Context ctx = newPlannerContext();
        UpsertById upsertById = new UpsertById(
            ctx.jobId(),
            ctx.nextExecutionPhaseId(),
            false,
            0,
            new String[]{femaleRef.ident().columnIdent().fqn()},
            new Reference[]{idRef, nameRef, femaleRef});
        upsertById.add("characters", "1", "1", new Symbol[]{Literal.newLiteral(true)}, null, missingAssignments);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(upsertById, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(1L)));

        // verify update
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef, femaleRef);
        Plan getPlan = newGetNode("characters", outputs, "1", ctx.nextExecutionPhaseId());
        rowReceiver = new CollectingRowReceiver();
        executor.execute(getPlan, rowReceiver);

        assertThat(rowReceiver.result(), contains(isRow(1, "Arthur", true)));
    }

    @Test
    public void testBulkUpdateByQueryTask() throws Exception {
        setup.setUpCharacters();
        /* update characters set name 'Zaphod Beeblebrox' where female = false
           update characters set name 'Zaphod Beeblebrox' where female = true
         */

        List<Plan> childNodes = new ArrayList<>();
        Planner.Context plannerContext = newPlannerContext();

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        assert tableInfo != null;
        Reference uidReference = new Reference(
            new ReferenceIdent(tableInfo.ident(), "_uid"), RowGranularity.DOC, DataTypes.STRING);

        // 1st collect and merge nodes
        Function query = new Function(new FunctionInfo(
            new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.BOOLEAN, DataTypes.BOOLEAN)),
            DataTypes.BOOLEAN),
            Arrays.asList(femaleRef, Literal.newLiteral(true)));

        UpdateProjection updateProjection = new UpdateProjection(
            new InputColumn(0, DataTypes.STRING),
            new String[]{"name"},
            new Symbol[]{Literal.newLiteral("Zaphod Beeblebrox")},
            null);

        WhereClause whereClause = new WhereClause(query);
        RoutedCollectPhase collectPhase1 = new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "collect",
            plannerContext.allocateRouting(tableInfo, whereClause, Preference.PRIMARY.type()),
            RowGranularity.DOC,
            ImmutableList.<Symbol>of(uidReference),
            ImmutableList.<Projection>of(updateProjection),
            whereClause,
            DistributionInfo.DEFAULT_BROADCAST
        );
        MergePhase mergeNode1 = MergePhase.localMerge(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            ImmutableList.<Projection>of(MergeCountProjection.INSTANCE),
            collectPhase1.executionNodes().size(),
            collectPhase1.outputTypes());
        childNodes.add(new CollectAndMerge(collectPhase1, mergeNode1));

        // 2nd collect and merge nodes
        Function query2 = new Function(new FunctionInfo(
            new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.BOOLEAN, DataTypes.BOOLEAN)),
            DataTypes.BOOLEAN),
            Arrays.asList(femaleRef, Literal.newLiteral(true)));

        final WhereClause whereClause1 = new WhereClause(query2);
        RoutedCollectPhase collectPhase2 = new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "collect",
            plannerContext.allocateRouting(tableInfo, whereClause1, Preference.PRIMARY.type()),
            RowGranularity.DOC,
            ImmutableList.<Symbol>of(uidReference),
            ImmutableList.<Projection>of(updateProjection),
            whereClause1,
            DistributionInfo.DEFAULT_BROADCAST
        );
        MergePhase mergeNode2 = MergePhase.localMerge(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            ImmutableList.<Projection>of(MergeCountProjection.INSTANCE),
            collectPhase2.executionNodes().size(),
            collectPhase2.outputTypes());
        childNodes.add(new CollectAndMerge(collectPhase2, mergeNode2));

        Upsert plan = new Upsert(childNodes, plannerContext.jobId());
        List<? extends ListenableFuture<TaskResult>> results = executor.executeBulk(plan);
        assertThat(results.size(), is(2));

        for (ListenableFuture<TaskResult> result1 : results) {
            TaskResult result = result1.get();
            assertThat(result, instanceOf(RowCountResult.class));
            // each of the bulk request hits 2 records
            assertThat(((RowCountResult) result).rowCount(), is(2L));
        }
    }
}
