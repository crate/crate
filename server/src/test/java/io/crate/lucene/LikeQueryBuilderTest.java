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

package io.crate.lucene;

import io.crate.lucene.match.CrateRegexQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.junit.Test;

import static io.crate.expression.operator.LikeOperators.convertSqlLikeToLuceneWildcard;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class LikeQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testLikeAnyOnArrayLiteral() throws Exception {
        Query likeQuery = convert("name like any (['a', 'b', 'c'])");
        assertThat(likeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery likeBQuery = (BooleanQuery) likeQuery;
        assertThat(likeBQuery.clauses().size(), is(3));
        for (int i = 0; i < 2; i++) {
            // like --> ConstantScoreQuery with regexp-filter
            Query filteredQuery = likeBQuery.clauses().get(i).getQuery();
            assertThat(filteredQuery, instanceOf(WildcardQuery.class));
        }
    }

    @Test
    public void testILikeAnyOnArrayLiteral() throws Exception {
        Query likeQuery = convert("name ilike any (['A', 'B', 'B'])");
        assertThat(likeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery likeBQuery = (BooleanQuery) likeQuery;
        assertThat(likeBQuery.clauses().size(), is(3));
        for (int i = 0; i < 2; i++) {
            Query filteredQuery = likeBQuery.clauses().get(i).getQuery();
            assertThat(filteredQuery, instanceOf(CrateRegexQuery.class));
        }
    }

    @Test
    public void testNotLikeAnyOnArrayLiteral() throws Exception {
        Query notLikeQuery = convert("name not like any (['a', 'b', 'c'])");
        assertThat(notLikeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery notLikeBQuery = (BooleanQuery) notLikeQuery;
        assertThat(notLikeBQuery.clauses(), hasSize(2));
        BooleanClause clause = notLikeBQuery.clauses().get(1);
        assertThat(clause.getOccur(), is(BooleanClause.Occur.MUST_NOT));
        assertThat(((BooleanQuery) clause.getQuery()).clauses(), hasSize(3));
        for (BooleanClause innerClause : ((BooleanQuery) clause.getQuery()).clauses()) {
            assertThat(innerClause.getOccur(), is(BooleanClause.Occur.MUST));
            assertThat(innerClause.getQuery(), instanceOf(WildcardQuery.class));
        }
    }

    @Test
    public void testNotILikeAnyOnArrayLiteral() throws Exception {
        Query notLikeQuery = convert("name not ilike any (['A', 'B', 'C'])");
        assertThat(notLikeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery notLikeBQuery = (BooleanQuery) notLikeQuery;
        assertThat(notLikeBQuery.clauses(), hasSize(2));
        BooleanClause clause = notLikeBQuery.clauses().get(1);
        assertThat(clause.getOccur(), is(BooleanClause.Occur.MUST_NOT));
        assertThat(((BooleanQuery) clause.getQuery()).clauses(), hasSize(3));
        for (BooleanClause innerClause : ((BooleanQuery) clause.getQuery()).clauses()) {
            assertThat(innerClause.getOccur(), is(BooleanClause.Occur.MUST));
            assertThat(innerClause.getQuery(), instanceOf(CrateRegexQuery.class));
        }
    }

    @Test
    public void testLikeWithBothSidesReferences() throws Exception {
        Query query = convert("name ilike name");
        assertThat(query, instanceOf(GenericFunctionQuery.class));
    }


    @Test
    public void testSqlLikeToLuceneWildcard() throws Exception {
        assertThat(convertSqlLikeToLuceneWildcard("%\\\\%"), is("*\\\\*"));
        assertThat(convertSqlLikeToLuceneWildcard("%\\\\_"), is("*\\\\?"));
        assertThat(convertSqlLikeToLuceneWildcard("%\\%"), is("*%"));

        assertThat(convertSqlLikeToLuceneWildcard("%me"), is("*me"));
        assertThat(convertSqlLikeToLuceneWildcard("\\%me"), is("%me"));
        assertThat(convertSqlLikeToLuceneWildcard("*me"), is("\\*me"));

        assertThat(convertSqlLikeToLuceneWildcard("_me"), is("?me"));
        assertThat(convertSqlLikeToLuceneWildcard("\\_me"), is("_me"));
        assertThat(convertSqlLikeToLuceneWildcard("?me"), is("\\?me"));
    }

    @Test
    public void test_like_on_varchar_column_uses_wildcard_query() throws Exception {
        Query query = convert("vchar_name LIKE 'Trillian%'");
        assertThat(query.toString(), is("vchar_name:Trillian*"));
        assertThat(query, instanceOf(WildcardQuery.class));
    }
}
