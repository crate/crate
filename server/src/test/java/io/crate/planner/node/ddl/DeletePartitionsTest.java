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

package io.crate.planner.node.ddl;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.data.RowN;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DeletePartitionsTest extends CrateDummyClusterServiceUnitTest {

    private final TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Test
    public void testIndexNameGeneration() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addPartitionedTable(
                TableDefinitions.PARTED_PKS_TABLE_DEFINITION,
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395961200000")).asIndexName());
        DeletePartitions plan = e.plan("delete from parted_pks where date = ?");

        Object[] args1 = {"1395874800000"};
        assertThat(plan.getIndices(txnCtx, e.nodeCtx, new RowN(args1), SubQueryResults.EMPTY)).containsExactly(
            ".partitioned.parted_pks.04732cpp6ks3ed1o60o30c1g");

        Object[] args2 = {"1395961200000"};
        assertThat(plan.getIndices(txnCtx, e.nodeCtx, new RowN(args2), SubQueryResults.EMPTY)).containsExactly(
            ".partitioned.parted_pks.04732cpp6ksjcc9i60o30c1g");
    }
}
