/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.relations;

import com.google.common.collect.Sets;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.core.Is.is;


public class QuerySplitterTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions expressions;
    private RelationName tr1 = new RelationName("doc", "t1");
    private RelationName tr2 = new RelationName("doc", "t2");
    private RelationName tr3 = new RelationName("doc", "t3");

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
        assertThat(split.size(), is(2));
        assertThat(split.get(Sets.newHashSet(tr1)), isSQL("((doc.t1.a = '10') AND (doc.t1.x = 1))"));
        assertThat(split.get(Sets.newHashSet(tr2)), isSQL("((doc.t2.b = '30') OR (doc.t2.b = '20'))"));
    }

    @Test
    public void test_query_splitter_retains_literals() {
        expressions.context().allowEagerNormalize(false);
        Symbol symbol = asSymbol("t1.a = 10::text and t1.x = t2.y and (false)");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));
        assertThat(split.get(Set.of(tr1)), isSQL("(doc.t1.a = cast(10 AS text))"));
        assertThat(split.get(Set.of(tr1, tr2)), isSQL("((doc.t1.x = doc.t2.y) AND false)"));
    }

    @Test
    public void testSplitDownTo1Relation() throws Exception {
        Symbol symbol = asSymbol("t1.a = '10' and (t2.b = '30' or t2.b = '20')");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));
        assertThat(split.get(Sets.newHashSet(tr1)), isSQL("(doc.t1.a = '10')"));
        assertThat(split.get(Sets.newHashSet(tr2)), isSQL("((doc.t2.b = '30') OR (doc.t2.b = '20'))"));
    }

    @Test
    public void testMixedSplit() throws Exception {
        Symbol symbol = asSymbol("t1.a = '10' and (t2.b = t3.c)");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));
        assertThat(split.get(Sets.newHashSet(tr1)), isSQL("(doc.t1.a = '10')"));
        assertThat(split.get(Sets.newHashSet(tr2, tr3)), isSQL("(doc.t2.b = doc.t3.c)"));
    }

    @Test
    public void testSplitQueryWith3Relations() throws Exception {
        Symbol symbol = asSymbol("t1.a = t2.b and t2.b = t3.c");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));


        Symbol t1t2 = asSymbol("t1.a = t2.b");
        Symbol t2t3 = asSymbol("t2.b = t3.c");

        Set<RelationName> tr1AndTr2 = Sets.newHashSet(tr1, tr2);
        assertThat(split.containsKey(tr1AndTr2), is(true));
        assertThat(split.get(tr1AndTr2), is(t1t2));

        Set<RelationName> tr2AndTr3 = Sets.newHashSet(tr2, tr3);
        assertThat(split.containsKey(tr2AndTr3), is(true));
        assertThat(split.get(tr2AndTr3), is(t2t3));
    }

    @Test
    public void testSplitQueryWith2TableJoinAnd3TableJoin() throws Exception {
        Symbol symbol = asSymbol("t1.a = t2.b and t2.b = t3.c || t1.a");
        Map<Set<RelationName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));

        Symbol t1t2 = asSymbol("t1.a = t2.b");
        Symbol t1t2t3 = asSymbol("t2.b = t3.c || t1.a");

        Set<RelationName> tr1AndTr2 = Sets.newHashSet(tr1, tr2);
        assertThat(split.containsKey(tr1AndTr2), is(true));
        assertThat(split.get(tr1AndTr2), is(t1t2));

        Set<RelationName> tr1AndTr2AndTr3 = Sets.newHashSet(tr1, tr2, tr3);
        assertThat(split.containsKey(tr1AndTr2AndTr3), is(true));
        assertThat(split.get(tr1AndTr2AndTr3), is(t1t2t3));
    }
}
