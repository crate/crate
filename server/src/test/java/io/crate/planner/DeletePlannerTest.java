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

import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.exceptions.VersioninigValidationException;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.TestingHelpers.isDocKey;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DeletePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addPartitionedTable(
                TableDefinitions.PARTED_PKS_TABLE_DEFINITION,
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395961200000")).asIndexName())
            .build();
    }

    @Test
    public void testDeletePlan() throws Exception {
        DeleteById plan = e.plan("delete from users where id = 1");
        assertThat(plan.table().ident().name(), is("users"));
        assertThat(plan.docKeys().size(), is(1));
        assertThat(plan.docKeys().getOnlyKey(), isDocKey(1L));
    }

    @Test
    public void testBulkDeletePartitionedTable() throws Exception {
        DeletePartitions plan = e.plan("delete from parted_pks where date = ?");
        List<Symbol> partitionSymbols = plan.partitions().get(0);
        assertThat(partitionSymbols, contains(instanceOf(ParameterSymbol.class)));
    }

    @Test
    public void testMultiDeletePlan() throws Exception {
        DeleteById plan = e.plan("delete from users where id in (1, 2)");
        assertThat(plan.docKeys().size(), is(2));
        List<String> docKeys = StreamSupport.stream(plan.docKeys().spliterator(), false)
            .map(x -> x.getId(txnCtx, e.nodeCtx, Row.EMPTY, SubQueryResults.EMPTY))
            .collect(Collectors.toList());

        assertThat(docKeys, Matchers.containsInAnyOrder("1", "2"));
    }

    @Test
    public void testDeleteWhereVersionIsNullPredicate() throws Exception {
        expectedException.expect(VersioninigValidationException.class);
        expectedException.expectMessage(VersioninigValidationException.VERSION_COLUMN_USAGE_MSG);
        e.plan("delete from users where _version is null");
    }

    @Test
    public void test_delete_where_id_and_seq_missing_primary_term() throws Exception {
        assertThrows(
            () -> e.plan("delete from users where id = 1 and _seq_no = 11"),
            VersioninigValidationException.class,
            VersioninigValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG
        );
    }
}
