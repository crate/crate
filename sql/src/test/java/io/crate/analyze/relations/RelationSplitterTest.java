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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.isFunction;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RelationSplitterTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TableInfo t1Info = new TestingTableInfo.Builder(new TableIdent(null, "t1"), null)
            .add("a", DataTypes.STRING)
            .add("x", DataTypes.INTEGER)
            .build();
    private final TableRelation tr1 = new TableRelation(t1Info);

    private final TableInfo t2Info = new TestingTableInfo.Builder(new TableIdent(null, "t2"), null)
            .add("b", DataTypes.STRING)
            .add("y", DataTypes.INTEGER)
            .build();
    private final TableRelation tr2 = new TableRelation(t2Info);

    private final TableInfo t3Info = new TestingTableInfo.Builder(new TableIdent(null, "t3"), null)
            .add("c", DataTypes.STRING)
            .add("z", DataTypes.INTEGER)
            .build();
    private final TableRelation tr3 = new TableRelation(t3Info);
    private SqlExpressions expressions;

    @Before
    public void setUp() throws Exception {
        Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                new QualifiedName("t1"), tr1,
                new QualifiedName("t2"), tr2,
                new QualifiedName("t3"), tr3
        );
        expressions = new SqlExpressions(sources);
        super.setUp();
    }

    private RelationSplitter split(QuerySpec querySpec) {
        return RelationSplitter.process(querySpec, ImmutableList.of(tr1, tr2, tr3));
    }

    private Symbol asSymbol(String expression) {
        return expressions.asSymbol(expression);
    }

    private List<Symbol> singleTrue() {
        return Arrays.<Symbol>asList(Literal.BOOLEAN_TRUE);
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
        QuerySpec splitQuerySpec = splitter.getSpec(tr1);

        assertThat(querySpec.where().hasQuery(), is(false));

        assertThat(splitQuerySpec.where().query(), instanceOf(io.crate.analyze.symbol.MatchPredicate.class));

        splitQuerySpec = splitter.getSpec(tr2);
        assertThat(splitQuerySpec.where().query(), isFunction("op_and"));
    }

    @Test
    public void testMatchWithColumnsFrom2Relations() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Must not use columns from more than 1 relation inside the MATCH predicate");
        QuerySpec querySpec = fromQuery("match ((a, b), 'search term')");
        split(querySpec);
    }

    @Test
    public void testQuerySpecSplit() throws Exception {
        QuerySpec querySpec = fromQuery("x = 1 and y = 3 and x + y = 4");
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true WHERE true and add(doc.t1.x, doc.t2.y) = 4"));
        assertThat(splitter.getSpec(tr1), isSQL("SELECT doc.t1.x WHERE doc.t1.x = 1"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT doc.t2.y WHERE true and doc.t2.y = 3"));
    }


    @Test
    public void testQuerySpecSplitNoRelationQuery() throws Exception {
        QuerySpec querySpec = fromQuery("x + y + z = 5").limit(30);
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true WHERE add(add(doc.t1.x, doc.t2.y), doc.t3.z) = 5 LIMIT 30"));
        assertThat(splitter.getSpec(tr1), isSQL("SELECT doc.t1.x"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT doc.t2.y"));
        assertThat(splitter.getSpec(tr3), isSQL("SELECT doc.t3.z"));
    }

    @Test
    public void testSplitQuerySpecOutputsOnly() throws Exception {
        QuerySpec querySpec = new QuerySpec().outputs(Arrays.asList(asSymbol("x"), asSymbol("y")));
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT doc.t1.x, doc.t2.y"));
        assertThat(splitter.getSpec(tr1), isSQL("SELECT doc.t1.x"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT doc.t2.y"));


        querySpec.limit(10);
        splitter = split(querySpec);
        assertThat(querySpec, isSQL("SELECT doc.t1.x, doc.t2.y LIMIT 10"));
        assertThat(splitter.getSpec(tr1), isSQL("SELECT doc.t1.x LIMIT 10"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT doc.t2.y LIMIT 10"));

        querySpec.offset(10);
        splitter = split(querySpec);
        assertThat(querySpec, isSQL("SELECT doc.t1.x, doc.t2.y LIMIT 10 OFFSET 10"));
        assertThat(splitter.getSpec(tr1), isSQL("SELECT doc.t1.x LIMIT 20"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT doc.t2.y LIMIT 20"));

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
        assertThat(splitter.remainingOrderBy(), isSQL("add(doc.t1.x, doc.t2.y)"));

        assertThat(splitter.getSpec(tr1), isSQL("SELECT doc.t1.x, doc.t1.a ORDER BY doc.t1.a DESC"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT doc.t2.y"));

    }

    @Test
    public void testSplitOrderByWith3RelationsButOutputsOnly2Relations() throws Exception {
        QuerySpec querySpec = fromQuery("x = 1 and y = 2 and z = 3").limit(30);
        List<Symbol> orderBySymbols = Arrays.asList(asSymbol("x"), asSymbol("y"), asSymbol("z"));
        OrderBy orderBy = new OrderBy(orderBySymbols, new boolean[]{false, false, false}, new Boolean[]{null, null, null});
        querySpec.orderBy(orderBy).limit(20).outputs(Arrays.asList(asSymbol("x"), asSymbol("y")));

        RelationSplitter splitter = split(querySpec);


        assertThat(querySpec, isSQL("SELECT doc.t1.x, doc.t2.y ORDER BY doc.t1.x, doc.t2.y, doc.t3.z LIMIT 20"));
        assertNull(splitter.remainingOrderBy());


        assertThat(splitter.getSpec(tr1), isSQL("SELECT doc.t1.x WHERE doc.t1.x = 1 ORDER BY doc.t1.x LIMIT 20"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT doc.t2.y WHERE true and doc.t2.y = 2 ORDER BY doc.t2.y LIMIT 20"));
        assertThat(splitter.getSpec(tr3), isSQL("SELECT doc.t3.z WHERE true and doc.t3.z = 3 ORDER BY doc.t3.z LIMIT 20"));
    }

    @Test
    public void testSplitNotMultiRelation() throws Exception {
        QuerySpec querySpec = fromQuery("not (a = 1 and x = 2) and b = 2");
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true"));
        assertThat(splitter.getSpec(tr1), isSQL("SELECT  WHERE not(doc.t1.a = '1' and doc.t1.x = 2)"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT  WHERE true and doc.t2.b = '2'"));
    }

    @Test
    public void testSplitNotMultiRelationInside() throws Exception {
        QuerySpec querySpec = fromQuery("not (a = 1 and b = 2)");
        assertThat(querySpec, isSQL("SELECT true WHERE not(doc.t1.a = '1' and doc.t2.b = '2')"));
        RelationSplitter splitter = split(querySpec);

        assertThat(querySpec, isSQL("SELECT true WHERE not(doc.t1.a = '1' and doc.t2.b = '2')"));
        assertThat(splitter.getSpec(tr1), isSQL("SELECT doc.t1.a"));
        assertThat(splitter.getSpec(tr2), isSQL("SELECT doc.t2.b"));
    }
}