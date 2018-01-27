/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.relations;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class RelationSplitterTest extends CrateUnitTest {

    private static final Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
        new QualifiedName(T3.T1_INFO.ident().name()), T3.TR_1,
        new QualifiedName(T3.T2_INFO.ident().name()), T3.TR_2,
        new QualifiedName(T3.T3_INFO.ident().name()), T3.TR_3
    );

    private static final SqlExpressions expressions = new SqlExpressions(sources);

    private RelationSplitter split(QuerySpec querySpec) {
        return split(querySpec, Collections.emptyList());
    }

    private RelationSplitter split(QuerySpec querySpec, List<JoinPair> joinPairs) {
        RelationSplitter splitter = new RelationSplitter(querySpec, T3.RELATIONS, joinPairs);
        splitter.process();
        return splitter;
    }

    private Symbol asSymbol(String expression) {
        return expressions.asSymbol(expression);
    }

    private List<Symbol> singleTrue() {
        return Collections.singletonList(Literal.BOOLEAN_TRUE);
    }

    private QuerySpec fromQuery(String query) {
        Symbol symbol = asSymbol(query);
        return new QuerySpec()
            .outputs(singleTrue())
            .where(new WhereClause(symbol));
    }

    @Test
    public void testWhereClauseSplitWithMatchFunction() throws Exception {
        QuerySpec querySpec = fromQuery("match (a, 'search term') and b=1");

        RelationSplitter splitter = split(querySpec);
        QuerySpec splitQuerySpec = splitter.getSpec(T3.TR_1);

        assertThat(querySpec.where().hasQuery(), is(false));

        assertThat(splitQuerySpec.where().query(), instanceOf(io.crate.expression.symbol.MatchPredicate.class));

        splitQuerySpec = splitter.getSpec(T3.TR_2);
        assertThat(splitQuerySpec.where().query(), isSQL("(doc.t2.b = '1')"));
    }

    @Test
    public void testMatchWithColumnsFrom2Relations() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "Cannot use MATCH predicates on columns of 2 different relations if it cannot be logically applied on each of them separately");
        QuerySpec querySpec = fromQuery("match ((a, b), 'search term')");
        split(querySpec);
    }

    @Test
    public void testQuerySpecSplit() throws Exception {
        QuerySpec querySpec = fromQuery("x = 1 and y = 3 and x + y = 4");
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true WHERE (to_long(add(doc.t1.x, doc.t2.y)) = 4)"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.x WHERE (doc.t1.x = 1)"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.y WHERE (doc.t2.y = 3)"));
    }


    @Test
    public void testQuerySpecSplitNoRelationQuery() throws Exception {
        QuerySpec querySpec = fromQuery("x + y + z = 5").limit(Literal.of(30));
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true WHERE (to_long(add(add(doc.t1.x, doc.t2.y), doc.t3.z)) = 5) LIMIT 30"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.x"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.y"));
        assertThat(splitter.getSpec(T3.TR_3), isSQL("SELECT doc.t3.z"));
    }

    @Test
    public void testSplitQuerySpecOutputsOnly() throws Exception {
        QuerySpec querySpec = new QuerySpec().outputs(Arrays.asList(asSymbol("x"), asSymbol("y")));
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT doc.t1.x, doc.t2.y"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.x"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.y"));

        querySpec.limit(Literal.of(10));
        splitter = split(querySpec);
        assertThat(querySpec, isSQL("SELECT doc.t1.x, doc.t2.y LIMIT 10"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.x LIMIT 10"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.y LIMIT 10"));

        querySpec.offset(Literal.of(10));
        splitter = split(querySpec);
        assertThat(querySpec, isSQL("SELECT doc.t1.x, doc.t2.y LIMIT 10 OFFSET 10"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.x LIMIT add(10, 10)"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.y LIMIT add(10, 10)"));
    }

    @Test
    public void testSplitOrderByThatIsPartiallyConsumed() throws Exception {
        // select a from t1, t2 order by a, b + x desc

        List<Symbol> orderBySymbols = Arrays.asList(asSymbol("a"), asSymbol("x + y"));
        OrderBy orderBy = new OrderBy(orderBySymbols, new boolean[]{true, false}, new Boolean[]{null, null});

        QuerySpec querySpec = new QuerySpec()
            .outputs(singleTrue())
            .orderBy(orderBy);

        RelationSplitter splitter = split(querySpec);
        assertThat(querySpec, isSQL("SELECT true ORDER BY doc.t1.a DESC, add(doc.t1.x, doc.t2.y)"));
        assertThat(querySpec.orderBy(), notNullValue());
        assertThat(querySpec.orderBy(), isSQL("doc.t1.a DESC, add(doc.t1.x, doc.t2.y)"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.a, doc.t1.x"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.y"));
    }

    @Test
    public void testSplitOrderByWith3RelationsButOutputsOnly2Relations() throws Exception {
        QuerySpec querySpec = fromQuery("x = 1 and y = 2 and z = 3").limit(Literal.of(30));
        List<Symbol> orderBySymbols = Arrays.asList(asSymbol("x"), asSymbol("y"), asSymbol("z"));
        OrderBy orderBy = new OrderBy(orderBySymbols, new boolean[]{false, false, false}, new Boolean[]{null, null, null});
        querySpec.orderBy(orderBy).limit(Literal.of(20)).outputs(Arrays.asList(asSymbol("x"), asSymbol("y")));

        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT doc.t1.x, doc.t2.y ORDER BY doc.t1.x, doc.t2.y, doc.t3.z LIMIT 20"));

        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.x WHERE (doc.t1.x = 1)"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.y WHERE (doc.t2.y = 2)"));
        assertThat(splitter.getSpec(T3.TR_3), isSQL("SELECT doc.t3.z WHERE (doc.t3.z = 3)"));
    }

    @Test
    public void testSplitOrderByCombiningColumnsFrom3Relations() throws Exception {
        // select a, b from t1, t2, t3 order by x, x - y + z, y, x + y
        QuerySpec querySpec = new QuerySpec().outputs(Arrays.asList(asSymbol("a"), asSymbol("b")));
        List<Symbol> orderBySymbols = Arrays.asList(asSymbol("x"),
            asSymbol("x - y + z"),
            asSymbol("y"),
            asSymbol("x+y"));
        OrderBy orderBy = new OrderBy(orderBySymbols,
            new boolean[]{false, false, false, false},
            new Boolean[]{null, null, null, null});
        querySpec.orderBy(orderBy);

        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT doc.t1.a, doc.t2.b " +
                                    "ORDER BY doc.t1.x, add(subtract(doc.t1.x, doc.t2.y), doc.t3.z), " +
                                    "doc.t2.y, add(doc.t1.x, doc.t2.y)"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.a, doc.t1.x"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.b, doc.t2.y"));
        assertThat(splitter.getSpec(T3.TR_3), isSQL("SELECT doc.t3.z"));

        assertThat(querySpec.orderBy(), notNullValue());
        assertThat(querySpec.orderBy(),
            isSQL("doc.t1.x, add(subtract(doc.t1.x, doc.t2.y), doc.t3.z), doc.t2.y, add(doc.t1.x, doc.t2.y)"));
    }


    @Test
    public void testSplitNotMultiRelation() throws Exception {
        QuerySpec querySpec = fromQuery("not (a = 1 and x = 2) and b = 2");
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT  WHERE (NOT ((doc.t1.a = '1') AND (doc.t1.x = 2)))"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT  WHERE (doc.t2.b = '2')"));
    }

    @Test
    public void testSplitNotMultiRelationInside() throws Exception {
        QuerySpec querySpec = fromQuery("not (a = 1 and b = 2)");
        assertThat(querySpec, isSQL("SELECT true WHERE (NOT ((doc.t1.a = '1') AND (doc.t2.b = '2')))"));
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true WHERE (NOT ((doc.t1.a = '1') AND (doc.t2.b = '2')))"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.a"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.b"));
    }

    @Test
    public void testSplitOfSingleRelationTree() throws Exception {
        QuerySpec querySpec = fromQuery("t1.a = t2.b and t1.a = 'employees'");
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true WHERE (doc.t1.a = doc.t2.b)"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.a WHERE (doc.t1.a = 'employees')"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.b"));
    }

    @Test
    public void testNoSplitOnOuterJoinRelation() throws Exception {
        QuerySpec querySpec = fromQuery("t2.y < 10");
        JoinPair joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.LEFT, asSymbol("t1.a = t2.b"));
        RelationSplitter splitter = split(querySpec, Collections.singletonList(joinPair));

        assertThat(querySpec, isSQL("SELECT true WHERE (doc.t2.y < 10)"));
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.a"));
        assertThat(splitter.getSpec(T3.TR_2), isSQL("SELECT doc.t2.y, doc.t2.b"));
    }

    @Test
    public void testGroupByOnlySymbolsAreAddedToOutputs() throws Exception {
        QuerySpec qs = new QuerySpec()
            .outputs(Collections.singletonList(asSymbol("max(t1.a)")))
            .groupBy(Collections.singletonList(asSymbol("t1.x")));
        RelationSplitter splitter = split(qs);

        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.x, doc.t1.a"));
    }

    @Test
    public void testHavingOnlyFieldsAreAddedToOutputs() throws Exception {
        QuerySpec qs = new QuerySpec()
            .outputs(Collections.singletonList(asSymbol("max(t1.a)")))
            .having(new HavingClause(asSymbol("t1.x = 10")));
        RelationSplitter splitter = split(qs);
        assertThat(splitter.getSpec(T3.TR_1), isSQL("SELECT doc.t1.x, doc.t1.a"));
    }

    @Test
    public void testNoMatchPushedDownToAllRelations() throws Exception {
        QuerySpec qs = new QuerySpec()
            .outputs(Arrays.asList(asSymbol("t1.a"), asSymbol("t2.b")))
            .where(WhereClause.NO_MATCH);
        RelationSplitter splitter = split(qs);
        assertThat(splitter.getSpec(T3.TR_1).where().noMatch(), is(true));
        assertThat(splitter.getSpec(T3.TR_2).where().noMatch(), is(true));
    }
}
