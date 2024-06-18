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

package io.crate.analyze.relations;

import static io.crate.testing.Asserts.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataTypes;


public class QuerySplitterTest extends CrateDummyClusterServiceUnitTest {

    private static final RelationName tr1 = new RelationName("doc", "t1");
    private static final RelationName tr2 = new RelationName("doc", "t2");
    private static final RelationName tr3 = new RelationName("doc", "t3");
    private SqlExpressions expressions;

    @Before
    public void prepare() throws Exception {
        expressions = new SqlExpressions(T3.sources(clusterService));
    }

    private Symbol asSymbol(String expression) {
        return expressions.asSymbol(expression);
    }

    @Test
    public void testSplitWithQueryMerge() throws Exception {
        Symbol symbol = asSymbol("t1.a = '10' and (t2.b = '30' or t2.b = '20') and t1.x = 1");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split).hasSize(2);
        assertThat(split.get(Set.of(tr1))).isSQL("((doc.t1.a = '10') AND (doc.t1.x = 1))");
        assertThat(split.get(Set.of(tr2))).isSQL("((doc.t2.b = '30') OR (doc.t2.b = '20'))");
    }

    @Test
    public void test_query_splitter_retains_literals() {
        expressions.context().allowEagerNormalize(false);
        Symbol symbol = asSymbol("t1.a = 10::text and t1.x = t2.y and (false)");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split).hasSize(2);
        assertThat(split.get(Set.of(tr1))).isSQL("(doc.t1.a = cast(10 AS text))");
        assertThat(split.get(Set.of(tr1, tr2))).isSQL("((doc.t1.x = doc.t2.y) AND false)");
    }

    @Test
    public void testSplitDownTo1Relation() throws Exception {
        Symbol symbol = asSymbol("t1.a = '10' and (t2.b = '30' or t2.b = '20')");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split).hasSize(2);
        assertThat(split.get(Set.of(tr1))).isSQL("(doc.t1.a = '10')");
        assertThat(split.get(Set.of(tr2))).isSQL("((doc.t2.b = '30') OR (doc.t2.b = '20'))");
    }

    @Test
    public void testMixedSplit() throws Exception {
        Symbol symbol = asSymbol("t1.a = '10' and (t2.b = t3.c)");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split).hasSize(2);
        assertThat(split.get(Set.of(tr1))).isSQL("(doc.t1.a = '10')");
        assertThat(split.get(Set.of(tr2, tr3))).isSQL("(doc.t2.b = doc.t3.c)");
    }

    @Test
    public void testSplitQueryWith3Relations() throws Exception {
        Symbol symbol = asSymbol("t1.a = t2.b and t2.b = t3.c");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split).hasSize(2);

        Symbol t1t2 = asSymbol("t1.a = t2.b");
        Symbol t2t3 = asSymbol("t2.b = t3.c");
        assertThat(split).containsExactlyInAnyOrderEntriesOf(Map.of(
            Set.of(tr1, tr2), t1t2,
            Set.of(tr2, tr3), t2t3));
    }

    @Test
    public void testSplitQueryWith2TableJoinAnd3TableJoin() throws Exception {
        Symbol symbol = asSymbol("t1.a = t2.b and t2.b = t3.c || t1.a");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split).hasSize(2);

        Symbol t1t2 = asSymbol("t1.a = t2.b");
        Symbol t1t2t3 = asSymbol("t2.b = t3.c || t1.a");
        assertThat(split).containsExactlyInAnyOrderEntriesOf(Map.of(
            Set.of(tr1, tr2), t1t2,
            Set.of(tr1, tr2, tr3), t1t2t3));
    }

    /**
     * https://github.com/crate/crate/issues/13888
     */
    @Test
    public void test_can_split_query_consist_of_multiple_types() {
        Symbol bool_a = new SimpleReference(
            new ReferenceIdent(tr1, "a"),
            RowGranularity.DOC,
            DataTypes.BOOLEAN,
            0,
            null
        );
        Symbol bool_b = new SimpleReference(
            new ReferenceIdent(tr1, "b"),
            RowGranularity.DOC,
            DataTypes.BOOLEAN,
            1,
            null
        );
        ScopedSymbol scopedSymbol = new ScopedSymbol(tr1, ColumnIdent.of("c"), DataTypes.BOOLEAN);
        Symbol matchPredicate = asSymbol("Match(t1.a, 'abc')");

        Symbol query = AndOperator.join(List.of(bool_a, bool_b, scopedSymbol, matchPredicate));
        assertThat(QuerySplitter.split(query)).containsExactlyEntriesOf(Map.of(Set.of(tr1), query));
    }
}
