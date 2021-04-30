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

package io.crate.planner.operators;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class CollectTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_prune_output_of_collect_updates_estimated_row_size() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, y int)")
            .build();
        TableStats tableStats = new TableStats();
        Symbol x = e.asSymbol("x");
        Collect collect = Collect.create(
            new DocTableRelation(e.resolveTableInfo("t")),
            List.of(x, e.asSymbol("y")),
            WhereClause.MATCH_ALL,
            Set.of(),
            tableStats,
            Row.EMPTY
        );
        assertThat(collect.estimatedRowSize(), is(DataTypes.INTEGER.fixedSize() * 2L));
        LogicalPlan prunedCollect = collect.pruneOutputsExcept(tableStats, List.of(x));
        assertThat(prunedCollect.estimatedRowSize(), is((long) DataTypes.INTEGER.fixedSize()));
    }
}
