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

package io.crate.planner.optimizer.costs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Limit;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class PlanStatsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_retrieve_number_of_docs_from_tablestats_in_collect_operator() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a),
                                 List.of(x),
                                 WhereClause.MATCH_ALL,
                                 10L,
                                 DataTypes.INTEGER.fixedSize());
        var memo = new Memo(source);
        // set number of docs in TableStats to 10
        PlanStats planStats = new PlanStats(memo);
        var result = planStats.apply(source);
        assertThat(result.numDocs()).isEqualTo(10L);
    }

    @Test
    public void test_retrieve_stats_for_group_reference() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a),
                                 List.of(x),
                                 WhereClause.MATCH_ALL,
                                 10L,
                                 DataTypes.INTEGER.fixedSize());
        var groupReference = new GroupReference(1, source.outputs(), Set.of());
        var memo = new Memo(source);
        PlanStats planStats = new PlanStats(memo);
        var result = planStats.apply(groupReference);
        assertThat(result.numDocs()).isEqualTo(10L);
    }

    @Test
    public void test_retrieve_stats_for_nested_operators() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a),
                                 List.of(x),
                                 WhereClause.MATCH_ALL,
                                 10L,
                                 DataTypes.INTEGER.fixedSize());
        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(a.ident(), new Stats(10L, 1, Map.of())));
        var limit = new Limit(source, Literal.of(5), Literal.of(0));

        var memo = new Memo(limit);
        PlanStats planStats = new PlanStats(memo);
        var result = planStats.apply(limit);
        assertThat(result.numDocs()).isEqualTo(5L);
    }
}
