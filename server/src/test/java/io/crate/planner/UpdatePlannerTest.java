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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersioningValidationException;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.UpdateProjection;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.consumer.UpdatePlanner;
import io.crate.planner.node.dml.UpdateById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.crate.expression.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES;
import static io.crate.expression.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class UpdatePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void prepare() throws IOException {
        e = buildExecutor(clusterService);
    }

    private static SQLExecutor buildExecutor(ClusterService clusterService) throws IOException {
        return SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .enableDefaultTables()
            .addPartitionedTable(
                TableDefinitions.PARTED_PKS_TABLE_DEFINITION,
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395961200000")).asIndexName())
            .addPartitionedTable(TableDefinitions.TEST_EMPTY_PARTITIONED_TABLE_DEFINITION)
            .build();
    }

    @Test
    public void testUpdateByQueryPlan() throws Exception {
        UpdatePlanner.Update plan = e.plan("update users set name='Vogon lyric fan'");
        Merge merge = (Merge) plan.createExecutionPlan.create(
            e.getPlannerContext(clusterService.state()), Row.EMPTY, SubQueryResults.EMPTY);

        Collect collect = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.where(), isSQL("true"));
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(UpdateProjection.class));
        assertThat(collectPhase.toCollect().size(), is(1));
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference) collectPhase.toCollect().get(0)).column().fqn(), is("_id"));

        UpdateProjection updateProjection = (UpdateProjection) collectPhase.projections().get(0);
        assertThat(updateProjection.uidSymbol(), instanceOf(InputColumn.class));

        assertThat(updateProjection.assignmentsColumns()[0], is("name"));
        Symbol symbol = updateProjection.assignments()[0];
        assertThat(symbol, isLiteral("Vogon lyric fan", DataTypes.STRING));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(1));
        assertThat(mergePhase.projections().get(0), instanceOf(MergeCountProjection.class));

        assertThat(mergePhase.outputTypes().size(), is(1));
    }

    @Test
    public void testUpdateByIdPlan() throws Exception {
        UpdateById updateById = e.plan("update users set name='Vogon lyric fan' where id=1");

        assertThat(updateById.assignmentByTargetCol().keySet(), contains(isReference("name")));
        assertThat(updateById.assignmentByTargetCol().values(), contains(isLiteral("Vogon lyric fan")));
        assertThat(updateById.docKeys().size(), is(1));

        assertThat(updateById.docKeys().getOnlyKey().getId(txnCtx, e.nodeCtx, Row.EMPTY, SubQueryResults.EMPTY), is("1"));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValues() throws Exception {
        UpdateById update = e.plan("update users set name='Vogon lyric fan' where id in (1,2,3)");
        assertThat(update.docKeys().size(), is(3));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValuesPartitioned() throws Exception {
        Plan update = e.plan("update parted_pks set name='Vogon lyric fan' where " +
                                     "(id=2 and date = 0) OR" +
                                     "(id=3 and date=123)");
        assertThat(update, instanceOf(UpdateById.class));
        assertThat(((UpdateById) update).docKeys().size(), is(2));
    }

    @Test
    public void testUpdateOnEmptyPartitionedTable() throws Exception {
        UpdatePlanner.Update update = e.plan("update empty_parted set name='Vogon lyric fan'");
        Collect collect = (Collect) update.createExecutionPlan.create(
            e.getPlannerContext(clusterService.state()), Row.EMPTY, SubQueryResults.EMPTY);
        assertThat(((RoutedCollectPhase) collect.collectPhase()).routing().nodes(), Matchers.emptyIterable());
    }

    @Test
    public void testUpdateUsingSeqNoRequiresPk() {
        expectedException.expect(VersioningValidationException.class);
        expectedException.expectMessage(VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG);
        UpdatePlanner.Update plan = e.plan("update users set name = 'should not update' where _seq_no = 11 and _primary_term = 1");
        plan.createExecutionPlan.create(
            e.getPlannerContext(clusterService.state()), Row.EMPTY, SubQueryResults.EMPTY);
    }

    @Test
    public void test_update_where_id_and_seq_missing_primary_term() throws Exception {
        assertThrowsMatches(
            () -> e.plan("update users set name = 'should not update' where id = 1 and _seq_no = 11"),
            VersioningValidationException.class,
            VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG
        );
    }

    @Test
    public void testMultiValueSubQueryWithinSingleValueSubQueryDoesNotInheritSoftLimit() {
        MultiPhasePlan plan = e.plan(
            "update users set ints = (" +
                "   select count(id) from users where id in (select unnest([1, 2, 3, 4])))");
        assertThat(plan.rootPlan, instanceOf(UpdatePlanner.Update.class));

        Map<LogicalPlan, SelectSymbol> rootPlanDependencies = plan.dependencies;
        LogicalPlan outerSubSelectPlan = rootPlanDependencies.keySet().iterator().next();
        SelectSymbol outerSubSelectSymbol = rootPlanDependencies.values().iterator().next();
        assertThat(outerSubSelectSymbol.getResultType(), is(SINGLE_COLUMN_SINGLE_VALUE));
        assertThat(outerSubSelectPlan.numExpectedRows(), is(2L));

        LogicalPlan innerSubSelectPlan = outerSubSelectPlan.dependencies().keySet().iterator().next();
        SelectSymbol innerSubSelectSymbol = outerSubSelectPlan.dependencies().values().iterator().next();
        assertThat(innerSubSelectSymbol.getResultType(), is(SINGLE_COLUMN_MULTIPLE_VALUES));
        assertThat(innerSubSelectPlan.numExpectedRows(), is(-1L));
    }

    @Test
    public void test_returning_for_update_throw_error_with_4_1_nodes() throws Exception {
        // Make sure the former initialized cluster service is shutdown
        cleanup();
        this.clusterService = createClusterService(additionalClusterSettings(), Metadata.EMPTY_METADATA, Version.V_4_1_0);
        e = buildExecutor(clusterService);
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(UpdatePlanner.RETURNING_VERSION_ERROR_MSG);
        e.plan("update users set name='test' where id=1 returning id");
    }
}
