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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.RelationColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class QuerySplitterTest {

    private SqlExpressions expressions;
    private QualifiedName tr1 = new QualifiedName("tr1");
    private QualifiedName tr2 = new QualifiedName("tr2");
    private QualifiedName tr3 = new QualifiedName("tr3");

    private Map<AnalyzedRelation, QualifiedName> relationToName = ImmutableMap.<AnalyzedRelation, QualifiedName>of(
            T3.TR_1, tr1,
            T3.TR_2, tr2,
            T3.TR_3, tr3);

    @Before
    public void setUp() throws Exception {
        expressions = new SqlExpressions(T3.SOURCES);
    }

    private Symbol asSymbol(String expression) {
        return ToRelationColumn.INSTANCE.process(expressions.asSymbol(expression), relationToName);
    }

    @Test
    public void testSplitWithQueryMerge() throws Exception {
        Symbol symbol = asSymbol("t1.a = 10 and (t2.b = 30 or t2.b = 20) and t1.x = 1");
        Map<Set<QualifiedName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));
        assertThat(split.get(Sets.newHashSet(tr1)), isSQL("((RELCOL(tr1, 0) = '10') AND (RELCOL(tr1, 1) = 1))"));
        assertThat(split.get(Sets.newHashSet(tr2)), isSQL("((RELCOL(tr2, 0) = '30') OR (RELCOL(tr2, 0) = '20'))"));
    }

    @Test
    public void testSplitDownTo1Relation() throws Exception {
        Symbol symbol = asSymbol("t1.a = 10 and (t2.b = 30 or t2.b = 20)");
        Map<Set<QualifiedName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));
        assertThat(split.get(Sets.newHashSet(tr1)), isSQL("(RELCOL(tr1, 0) = '10')"));
        assertThat(split.get(Sets.newHashSet(tr2)), isSQL("((RELCOL(tr2, 0) = '30') OR (RELCOL(tr2, 0) = '20'))"));
    }

    @Test
    public void testMixedSplit() throws Exception {
        Symbol symbol = asSymbol("t1.a = 10 and (t2.b = t3.c)");
        Map<Set<QualifiedName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));
        assertThat(split.get(Sets.newHashSet(tr1)), isSQL("(RELCOL(tr1, 0) = '10')"));
        assertThat(split.get(Sets.newHashSet(tr2, tr3)), isSQL("(RELCOL(tr2, 0) = RELCOL(tr3, 0))"));
    }

    @Test
    public void testSplitQueryWith3Relations() throws Exception {
        Symbol symbol = asSymbol("t1.a = t2.b and t2.b = t3.c");
        Map<Set<QualifiedName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));


        Symbol t1t2 = asSymbol("t1.a = t2.b");
        Symbol t2t3 = asSymbol("t2.b = t3.c");

        Set<QualifiedName> tr1AndTr2 = Sets.newHashSet(tr1, tr2);
        assertThat(split.containsKey(tr1AndTr2), is(true));
        assertThat(split.get(tr1AndTr2), is(t1t2));

        Set<QualifiedName> tr2AndTr3 = Sets.newHashSet(tr2, tr3);
        assertThat(split.containsKey(tr2AndTr3), is(true));
        assertThat(split.get(tr2AndTr3), is(t2t3));
    }

    @Test
    public void testSplitQueryWith2TableJoinAnd3TableJoin() throws Exception {
        Symbol symbol = asSymbol("t1.a = t2.b and t2.b = t3.c || t1.a");
        Map<Set<QualifiedName>, Symbol> split = QuerySplitter.split(symbol);
        assertThat(split.size(), is(2));

        Symbol t1t2 = asSymbol("t1.a = t2.b");
        Symbol t1t2t3 = asSymbol("t2.b = t3.c || t1.a");

        Set<QualifiedName> tr1AndTr2 = Sets.newHashSet(tr1, tr2);
        assertThat(split.containsKey(tr1AndTr2), is(true));
        assertThat(split.get(tr1AndTr2), is(t1t2));

        Set<QualifiedName> tr1AndTr2AndTr3 = Sets.newHashSet(tr1, tr2, tr3);
        assertThat(split.containsKey(tr1AndTr2AndTr3), is(true));
        assertThat(split.get(tr1AndTr2AndTr3), is(t1t2t3));
    }

    private static class ToRelationColumn extends ReplacingSymbolVisitor<Map<AnalyzedRelation, QualifiedName>> {

        private final static ToRelationColumn INSTANCE = new ToRelationColumn(ReplaceMode.MUTATE);

        private ToRelationColumn(ReplaceMode mode) {
            super(mode);
        }

        @Override
        public Symbol visitField(Field field, Map<AnalyzedRelation, QualifiedName> ctx) {
            return new RelationColumn(ctx.get(field.relation()), field.index(), field.valueType());
        }
    }
}
