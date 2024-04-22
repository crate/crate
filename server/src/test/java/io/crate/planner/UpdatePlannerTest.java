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

import static io.crate.expression.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES;
import static io.crate.expression.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static io.crate.testing.Asserts.toCondition;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

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
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class UpdatePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void prepare() throws IOException {
        e = buildExecutor(clusterService);
    }

    private static SQLExecutor buildExecutor(ClusterService clusterService) throws IOException {
        return SQLExecutor.builder(clusterService)
            .setNumNodes(2)
            .build()
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.PARTED_PKS_TABLE_DEFINITION,
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395961200000")).asIndexName())
            .addPartitionedTable(
                "create table doc.empty_parted (" +
                "  name text," +
                "  date timestamp with time zone" +
                ")" +
                " partitioned by (date)");
    }

    @Test
    public void testUpdateByQueryPlan() throws Exception {
        UpdatePlanner.Update plan = e.plan("update users set name='Vogon lyric fan'");
        Merge merge = (Merge) plan.createExecutionPlan.create(
            e.getPlannerContext(), Row.EMPTY, SubQueryResults.EMPTY);

        Collect collect = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        Asserts.assertThat(collectPhase.where()).isSQL("true");
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(UpdateProjection.class));
        assertThat(collectPhase.toCollect().size(), is(1));
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference) collectPhase.toCollect().get(0)).column().fqn(), is("_id"));

        UpdateProjection updateProjection = (UpdateProjection) collectPhase.projections().get(0);
        assertThat(updateProjection.uidSymbol(), instanceOf(InputColumn.class));

        assertThat(updateProjection.assignmentsColumns()[0], is("name"));
        Symbol symbol = updateProjection.assignments()[0];
        Asserts.assertThat(symbol).isLiteral("Vogon lyric fan", DataTypes.STRING);

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(1));
        assertThat(mergePhase.projections().get(0), instanceOf(MergeCountProjection.class));

        assertThat(mergePhase.outputTypes().size(), is(1));
    }

    @Test
    public void testUpdateByIdPlan() throws Exception {
        UpdateById updateById = e.plan("update users set name='Vogon lyric fan' where id=1");

        Asserts.assertThat(updateById.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("name")), toCondition(isLiteral("Vogon lyric fan")));
        assertThat(updateById.docKeys().size(), is(1));

        assertThat(updateById.docKeys().getOnlyKey().getId(txnCtx, e.nodeCtx, Row.EMPTY, SubQueryResults.EMPTY), is("1"));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValues() throws Exception {
        UpdateById update = e.plan("update users set name='Vogon lyric fan' where id in (1,2,3)");
        assertThat(update.docKeys().size(), is(3));
    }

    // bug: https://github.com/crate/crate/issues/14347
    @Test
    public void test_update_plan_with_where_clause_involving_pk_and_non_pk() throws Exception {
        Plan update = e.plan("update users set name='Vogon lyric fan' where id in (1,2,3) and name='dummy'");
        assertThat(update).isExactlyInstanceOf(UpdatePlanner.Update.class);
        update = e.plan("update users set name='Vogon lyric fan' where id in (1,2,3) or name='dummy'");
        assertThat(update).isExactlyInstanceOf(UpdatePlanner.Update.class);
    }

    @Test
    public void test_update_with_subquery_and_pk_does_not_update_by_id() throws Exception {
        Plan update = e.plan(
            "update users set name = 'No!' where id = 1 and not exists (select 1 from users where id = 1)");
        assertThat(update).isExactlyInstanceOf(MultiPhasePlan.class);

        MultiPhasePlan multiPhasePlan = (MultiPhasePlan) update;
        assertThat(multiPhasePlan.rootPlan).isExactlyInstanceOf(UpdatePlanner.Update.class);
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
            e.getPlannerContext(), Row.EMPTY, SubQueryResults.EMPTY);
        assertThat(((RoutedCollectPhase) collect.collectPhase()).routing().nodes(), Matchers.emptyIterable());
    }

    @Test
    public void testUpdateUsingSeqNoRequiresPk() {
        UpdatePlanner.Update plan = e.plan("update users set name = 'should not update' where _seq_no = 11 and _primary_term = 1");
        assertThatThrownBy(() -> plan.createExecutionPlan.create(
            e.getPlannerContext(), Row.EMPTY, SubQueryResults.EMPTY))
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG);
    }

    @Test
    public void test_update_where_id_and_seq_missing_primary_term() throws Exception {
        assertThatThrownBy(
            () -> e.plan("update users set name = 'should not update' where id = 1 and _seq_no = 11"))
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG);
    }

    @Test
    public void testMultiValueSubQueryWithinSingleValueSubQueryDoesNotInheritSoftLimit() {
        MultiPhasePlan plan = e.plan(
            "update users set ints = (" +
                "   select count(id) from users where id in (select unnest([1, 2, 3, 4])))");
        assertThat(plan.rootPlan, instanceOf(UpdatePlanner.Update.class));

        Map<LogicalPlan, SelectSymbol> rootPlanDependencies = plan.dependencies;
        LogicalPlan outerSubSelectPlan = rootPlanDependencies.keySet().iterator().next();
        Asserts.assertThat(outerSubSelectPlan).withPlanStats(e.planStats()).hasOperators(
            "Limit[2::bigint;0::bigint] (rows=1)",
            "  └ MultiPhase (rows=1)",
            "    └ HashAggregate[count(id)] (rows=1)",
            "      └ Collect[doc.users | [id] | (id = ANY((SELECT unnest([1, 2, 3, 4]) FROM (empty_row))))] (rows=unknown)",
            "    └ OrderBy[unnest([1, 2, 3, 4]) ASC] (rows=unknown)",
            "      └ ProjectSet[unnest([1, 2, 3, 4])] (rows=unknown)",
            "        └ TableFunction[empty_row | [] | true] (rows=unknown)");
        SelectSymbol outerSubSelectSymbol = rootPlanDependencies.values().iterator().next();
        assertThat(outerSubSelectSymbol.getResultType(), is(SINGLE_COLUMN_SINGLE_VALUE));
        assertThat(e.getStats(outerSubSelectPlan).numDocs(), is(1L));

        LogicalPlan innerSubSelectPlan = outerSubSelectPlan.dependencies().keySet().iterator().next();
        SelectSymbol innerSubSelectSymbol = outerSubSelectPlan.dependencies().values().iterator().next();
        assertThat(innerSubSelectSymbol.getResultType(), is(SINGLE_COLUMN_MULTIPLE_VALUES));
        assertThat(e.getStats(innerSubSelectPlan).numDocs(), is(-1L));
    }

    @Test
    public void test_returning_for_update_throw_error_with_4_1_nodes() throws Exception {
        // Make sure the former initialized cluster service is shutdown
        cleanup();
        this.clusterService = createClusterService(additionalClusterSettings(), Metadata.EMPTY_METADATA, Version.V_4_1_0);
        e = buildExecutor(clusterService);
        assertThatThrownBy(() -> e.plan("update users set name='test' where id=1 returning id"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage(UpdatePlanner.RETURNING_VERSION_ERROR_MSG);
    }
}
