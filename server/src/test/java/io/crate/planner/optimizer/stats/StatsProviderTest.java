/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.stats;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Lists2;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Eval;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class StatsProviderTest extends CrateDummyClusterServiceUnitTest {

    public void test_simple_collect() throws Exception{
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();

        DocTableInfo a = e.resolveTableInfo("a");
        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a), List.of(x), WhereClause.MATCH_ALL, 1L, DataTypes.INTEGER.fixedSize());
        var memo = new Memo(source);
        StatsProvider statsProvider = new StatsProvider(memo);
        var result = statsProvider.apply(source);
        assertThat(result.outputRowCount()).isEqualTo(1L);
    }

    public void test_group_reference() throws Exception{
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();
        DocTableInfo a = e.resolveTableInfo("a");
        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a), List.of(x), WhereClause.MATCH_ALL, 1L, DataTypes.INTEGER.fixedSize());
        var groupReference = new GroupReference(1, source.outputs(), Set.of());
        var memo = new Memo(source);
        StatsProvider statsProvider = new StatsProvider(memo);
        var result = statsProvider.apply(groupReference);
        assertThat(result.outputRowCount()).isEqualTo(1L);
    }

    public void test_tree_of_operators() throws Exception{
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();
        DocTableInfo a = e.resolveTableInfo("a");
        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a), List.of(x), WhereClause.MATCH_ALL, 1L, DataTypes.INTEGER.fixedSize());
        var eval = Eval.create(source, Lists2.concat(source.outputs(),source.outputs()));
        var memo = new Memo(source);
        StatsProvider statsProvider = new StatsProvider(memo);
        var result = statsProvider.apply(eval);
        assertThat(result.outputRowCount()).isEqualTo(1L);
    }
}
