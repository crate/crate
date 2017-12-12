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

import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Test;

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
    public void testMerge() {
        WhereClause whereLiteralTrue = new WhereClause(Literal.BOOLEAN_TRUE);
        WhereClause whereLiteralFalse = new WhereClause(Literal.BOOLEAN_FALSE);
        Symbol query1 = sqlExpressions.asSymbol("x = 1");
        WhereClause where1 = new WhereClause(query1);
        Symbol query2 = sqlExpressions.asSymbol("x = 2");
        WhereClause where2 = new WhereClause(query2);
        Symbol queryMerged = sqlExpressions.asSymbol("x = 1 and x =2");
        WhereClause whereMerged = new WhereClause(queryMerged);

        assertThat(whereLiteralFalse.mergeWhere(whereLiteralTrue), is(WhereClause.NO_MATCH));
        assertThat(whereLiteralTrue.mergeWhere(whereLiteralTrue), is(WhereClause.MATCH_ALL));
        assertThat(where1.mergeWhere(whereLiteralTrue), is(where1));
        assertThat(where1.mergeWhere(whereLiteralFalse), is(WhereClause.NO_MATCH));
        assertThat(where2.mergeWhere(where1), is(whereMerged));
    }
}
