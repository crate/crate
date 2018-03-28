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

package io.crate.analyze;

import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.TransactionContext;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class WhereClauseTest {

    private SqlExpressions sqlExpressions = new SqlExpressions(T3.SOURCES);

    @Test
    public void testReplaceWithLiteralTrueSemantics() throws Exception {
        Symbol query = sqlExpressions.asSymbol("x = 10");

        WhereClause whereReplaced = new WhereClause(query);
        whereReplaced.replace(s -> Literal.BOOLEAN_TRUE);
        WhereClause whereLiteralTrue = new WhereClause(Literal.BOOLEAN_TRUE);

        assertThat(whereLiteralTrue.hasQuery(), is(whereReplaced.hasQuery()));
        assertThat(whereLiteralTrue.noMatch(), is(whereReplaced.noMatch()));
        assertThat(whereLiteralTrue.query(), is(whereReplaced.query()));
    }

    @Test
    public void testReplaceWithLiteralFalseSemantics() throws Exception {
        Symbol query = sqlExpressions.asSymbol("x = 10");

        WhereClause whereReplaced = new WhereClause(query);
        whereReplaced.replace(s -> Literal.BOOLEAN_FALSE);
        WhereClause whereLiteralFalse = new WhereClause(Literal.BOOLEAN_FALSE);

        assertThat(whereLiteralFalse.hasQuery(), is(whereReplaced.hasQuery()));
        assertThat(whereLiteralFalse.noMatch(), is(whereReplaced.noMatch()));
        assertThat(whereLiteralFalse.query(), is(whereReplaced.query()));
    }

    @Test
    public void testNormalizeEliminatesNulls() {
        WhereClause where = new WhereClause(sqlExpressions.asSymbol("null or x = 10 or a = null"));
        WhereClause normalizedWhere = where.normalize(
            EvaluatingNormalizer.functionOnlyNormalizer(getFunctions()), new TransactionContext());
        assertThat(normalizedWhere.query(), is(sqlExpressions.asSymbol("x = 10")));
    }

    @Test
    public void testAddWithNoMatch() {
        WhereClause where1 = WhereClause.NO_MATCH;
        WhereClause where2 = new WhereClause(sqlExpressions.asSymbol("x = 10"));
        WhereClause where1_where2 = where1.add(where2.query);

        assertThat(where1_where2.hasQuery(), is(false));
        assertThat(where1_where2.noMatch, is(true));
    }

    @Test
    public void testAddWithMatchAll() {
        WhereClause where1 = WhereClause.MATCH_ALL;
        WhereClause where2 = new WhereClause(sqlExpressions.asSymbol("x = 10"));
        WhereClause where1_where2 = where1.add(where2.query);

        assertThat(where1_where2.hasQuery(), is(true));
        assertThat(where1_where2.noMatch, is(false));
        assertThat(where1_where2.query(), is(where2.query()));
    }

    @Test
    public void testAddWithNullQuery() {
        WhereClause where1 = new WhereClause((Symbol) null);
        WhereClause where2 = new WhereClause(sqlExpressions.asSymbol("x = 10"));
        WhereClause where1_where2 = where1.add(where2.query);

        assertThat(where1_where2.hasQuery(), is(true));
        assertThat(where1_where2.noMatch, is(false));
        assertThat(where1_where2.query(), is(where2.query()));
    }
}
