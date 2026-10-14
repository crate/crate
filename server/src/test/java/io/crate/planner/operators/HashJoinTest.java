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

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isSQL;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.CharacterType;

public class HashJoinTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;
    private AnalyzedRelation tr1;
    private AnalyzedRelation tr2;
    private AnalyzedRelation tr3;

    @Before
    public void prepare() throws Exception {
        Map<RelationName, AnalyzedRelation> sources = T3.sources(clusterService);
        sqlExpressions = new SqlExpressions(sources);
        tr1 = sources.get(T3.T1);
        tr2 = sources.get(T3.T2);
        tr3 = sources.get(T3.T3);
    }

    @Test
    public void test_create_lsh_rhs_hash_symbols_from_single_eq_condition() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.a = t2.b");
        var result = HashJoin.createHashSymbols(
            List.of(tr1.relationName()),
            List.of(tr2.relationName()),
            joinCondition);

        assertThat(result.lhsHashSymbols()).satisfiesExactly(isSQL("doc.t1.a"));
        assertThat(result.rhsHashSymbols()).satisfiesExactly(isSQL("doc.t2.b"));
    }

    @Test
    public void test_create_lsh_rhs_hash_symbols_from_two_eq_conditions() {
        Symbol joinCondition = sqlExpressions.asSymbol("t3.c = t1.a AND t2.b = t1.a");
        var result = HashJoin.createHashSymbols(
            List.of(tr1.relationName()),
            List.of(tr2.relationName(), tr3.relationName()),
            joinCondition);

        assertThat(result.lhsHashSymbols()).satisfiesExactly(isSQL("doc.t1.a"), isSQL("doc.t1.a"));
        assertThat(result.rhsHashSymbols()).satisfiesExactly(isSQL("doc.t3.c"), isSQL("doc.t2.b"));
    }


    @Test
    public void test_create_lsh_rhs_hash_symbols_from_three_eq_conditions() {
        Symbol joinCondition = sqlExpressions.asSymbol("t3.c = t1.a AND t2.b = t1.a and t1.i = t2.i");
        var result = HashJoin.createHashSymbols(
            List.of(tr1.relationName()),
            List.of(tr2.relationName(), tr3.relationName()),
            joinCondition);

        assertThat(result.lhsHashSymbols()).satisfiesExactly(isSQL("doc.t1.a"), isSQL("doc.t1.a"), isSQL("doc.t1.i"));
        assertThat(result.rhsHashSymbols()).satisfiesExactly(isSQL("doc.t3.c"), isSQL("doc.t2.b"), isSQL("doc.t2.i"));

    }

    @Test
    public void test_hash_symbols_of_both_sides_are_paired_by_position() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.a = t3.c AND t2.i = t5.i AND t1.i = t5.i");
        var result = HashJoin.createHashSymbols(
            List.of(T3.T1, T3.T2),
            List.of(T3.T3, T3.T5),
            joinCondition);

        assertThat(result.lhsHashSymbols()).satisfiesExactly(
            isSQL("doc.t1.a"), isSQL("doc.t2.i"), isSQL("doc.t1.i"));
        assertThat(result.rhsHashSymbols()).satisfiesExactly(
            isSQL("doc.t3.c"), isSQL("doc.t5.i"), isSQL("doc.t5.i"));
    }

    @Test
    public void test_hash_symbols_of_character_columns_are_normalized_to_a_common_type() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl1 (a int, c1 char(2))")
            .addTable("create table tbl2 (b int, c2 char(3))");

        Symbol joinCondition = e.asSymbol("tbl1.c1 = tbl2.c2");
        var result = HashJoin.createHashSymbols(
            List.of(new RelationName("doc", "tbl1")),
            List.of(new RelationName("doc", "tbl2")),
            joinCondition);

        // character(n) values are blank padded to their declared length, so hashing them by value
        // can only find a match if both sides of a pair are normalized to the same length.
        assertThat(result.lhsHashSymbols()).zipSatisfy(
            result.rhsHashSymbols(),
            (lhs, rhs) -> {
                assertThat(lhs.valueType()).isEqualTo(CharacterType.of(3));
                assertThat(rhs.valueType()).isEqualTo(CharacterType.of(3));
            });
    }
}
