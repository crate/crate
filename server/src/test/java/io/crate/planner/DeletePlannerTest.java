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

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.exactlyInstanceOf;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.VersioningValidationException;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.statement.DeletePlanner;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DeletePlannerTest extends CrateDummyClusterServiceUnitTest {

    private final TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.PARTED_PKS_TABLE_DEFINITION,
                new PartitionName(new RelationName("doc", "parted_pks"), List.of("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), List.of("1395961200000")).asIndexName());
    }

    @Test
    public void testDeletePlan() throws Exception {
        DeleteById plan = e.plan("delete from users where id = 1");
        assertThat(plan.table().ident().name()).isEqualTo("users");
        assertThat(plan.docKeys()).hasSize(1);
        assertThat(plan.docKeys().getOnlyKey()).isDocKey(1L);
    }

    @Test
    public void testBulkDeletePartitionedTable() throws Exception {
        DeletePartitions plan = e.plan("delete from parted_pks where date = ?");
        List<Symbol> partitionSymbols = plan.partitions().get(0);
        assertThat(partitionSymbols).satisfiesExactly(exactlyInstanceOf(ParameterSymbol.class));
    }

    // bug: https://github.com/crate/crate/issues/14347
    @Test
    public void test_delete_plan_with_where_clause_involving_pk_and_non_pk() throws Exception {
        Plan delete = e.plan("delete from users where id in (1,2,3) and name='dummy'");
        assertThat(delete).isExactlyInstanceOf(DeletePlanner.Delete.class);
        delete = e.plan("delete from users where id in (1,2,3) or name='dummy'");
        assertThat(delete).isExactlyInstanceOf(DeletePlanner.Delete.class);
    }

    @Test
    public void testMultiDeletePlan() throws Exception {
        DeleteById plan = e.plan("delete from users where id in (1, 2)");
        assertThat(plan.docKeys()).hasSize(2);
        List<String> docKeys = StreamSupport.stream(plan.docKeys().spliterator(), false)
            .map(x -> x.getId(txnCtx, e.nodeCtx, Row.EMPTY, SubQueryResults.EMPTY))
            .collect(Collectors.toList());

        assertThat(docKeys).containsExactlyInAnyOrder("1", "2");
    }

    @Test
    public void testDeleteWhereVersionIsNullPredicate() throws Exception {
        assertThatThrownBy(() -> e.plan("delete from users where _version is null"))
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void test_delete_where_id_and_seq_missing_primary_term() throws Exception {
        Assertions.assertThatThrownBy(() -> e.plan("delete from users where id = 1 and _seq_no = 11"))
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessageContaining(VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG);
    }

    @Test
    public void test_cannot_delete_from__all() throws Exception {
        assertThatThrownBy(() -> e.plan("delete from _all"))
            .isExactlyInstanceOf(RelationUnknown.class);
    }
}
