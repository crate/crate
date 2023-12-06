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

import static io.crate.expression.operator.LikeOperators.convertSqlLikeToLuceneWildcard;
import static io.crate.testing.Asserts.assertThat;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.junit.Test;

import io.crate.lucene.match.CrateRegexQuery;

public class LikeQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testLikeAnyOnArrayLiteral() throws Exception {
        Query likeQuery = convert("name like any (['a', 'b', 'c'])");
        assertThat(likeQuery).isExactlyInstanceOf(BooleanQuery.class);
        BooleanQuery likeBQuery = (BooleanQuery) likeQuery;
        assertThat(likeBQuery.clauses()).hasSize(3);
        for (int i = 0; i < 2; i++) {
            // like --> ConstantScoreQuery with regexp-filter
            Query filteredQuery = likeBQuery.clauses().get(i).getQuery();
            assertThat(filteredQuery).isExactlyInstanceOf(WildcardQuery.class);
        }
    }

    @Test
    public void testILikeAnyOnArrayLiteral() throws Exception {
        Query likeQuery = convert("name ilike any (['A', 'B', 'B'])");
        assertThat(likeQuery).isExactlyInstanceOf(BooleanQuery.class);
        BooleanQuery likeBQuery = (BooleanQuery) likeQuery;
        assertThat(likeBQuery.clauses()).hasSize(3);
        for (int i = 0; i < 2; i++) {
            Query filteredQuery = likeBQuery.clauses().get(i).getQuery();
            assertThat(filteredQuery).isExactlyInstanceOf(CrateRegexQuery.class);
        }
    }

    @Test
    public void testNotLikeAnyOnArrayLiteral() throws Exception {
        Query notLikeQuery = convert("name not like any (['a', 'b', 'c'])");
        assertThat(notLikeQuery).isExactlyInstanceOf(BooleanQuery.class);
        BooleanQuery notLikeBQuery = (BooleanQuery) notLikeQuery;
        assertThat(notLikeBQuery.clauses()).hasSize(2);
        BooleanClause clause = notLikeBQuery.clauses().get(1);
        assertThat(clause.getOccur()).isEqualTo(BooleanClause.Occur.MUST_NOT);
        assertThat(((BooleanQuery) clause.getQuery()).clauses()).hasSize(3);
        for (BooleanClause innerClause : ((BooleanQuery) clause.getQuery()).clauses()) {
            assertThat(innerClause.getOccur()).isEqualTo(BooleanClause.Occur.MUST);
            assertThat(innerClause.getQuery()).isExactlyInstanceOf(WildcardQuery.class);
        }
    }

    @Test
    public void testNotILikeAnyOnArrayLiteral() throws Exception {
        Query notLikeQuery = convert("name not ilike any (['A', 'B', 'C'])");
        assertThat(notLikeQuery).isExactlyInstanceOf(BooleanQuery.class);
        BooleanQuery notLikeBQuery = (BooleanQuery) notLikeQuery;
        assertThat(notLikeBQuery.clauses()).hasSize(2);
        BooleanClause clause = notLikeBQuery.clauses().get(1);
        assertThat(clause.getOccur()).isEqualTo(BooleanClause.Occur.MUST_NOT);
        assertThat(((BooleanQuery) clause.getQuery()).clauses()).hasSize(3);
        for (BooleanClause innerClause : ((BooleanQuery) clause.getQuery()).clauses()) {
            assertThat(innerClause.getOccur()).isEqualTo(BooleanClause.Occur.MUST);
            assertThat(innerClause.getQuery()).isExactlyInstanceOf(CrateRegexQuery.class);
        }
    }

    @Test
    public void testLikeWithBothSidesReferences() throws Exception {
        Query query = convert("name ilike name");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }


    @Test
    public void testSqlLikeToLuceneWildcard() throws Exception {
        assertThat(convertSqlLikeToLuceneWildcard("%\\\\%")).isEqualTo("*\\\\*");
        assertThat(convertSqlLikeToLuceneWildcard("%\\\\_")).isEqualTo("*\\\\?");
        assertThat(convertSqlLikeToLuceneWildcard("%\\%")).isEqualTo("*%");

        assertThat(convertSqlLikeToLuceneWildcard("%me")).isEqualTo("*me");
        assertThat(convertSqlLikeToLuceneWildcard("\\%me")).isEqualTo("%me");
        assertThat(convertSqlLikeToLuceneWildcard("*me")).isEqualTo("\\*me");

        assertThat(convertSqlLikeToLuceneWildcard("_me")).isEqualTo("?me");
        assertThat(convertSqlLikeToLuceneWildcard("\\_me")).isEqualTo("_me");
        assertThat(convertSqlLikeToLuceneWildcard("?me")).isEqualTo("\\?me");
    }

    @Test
    public void test_like_on_varchar_column_uses_wildcard_query() throws Exception {
        Query query = convert("vchar_name LIKE 'Trillian%'");
        assertThat(query).hasToString("vchar_name:Trillian*");
        assertThat(query).isExactlyInstanceOf(WildcardQuery.class);

        // Verify that version with ESCAPE doesn't lose it on rewriting function.
        query = convert("vchar_name LIKE 'Trillian%' ESCAPE '\\'");
        assertThat(query).hasToString("vchar_name:Trillian*");
        assertThat(query).isExactlyInstanceOf(WildcardQuery.class);
    }

    @Test
    public void test_like_on_index_off_column_falls_back_to_generic_query() {
        Query query = convert("text_no_index LIKE '%abc%'");
        assertThat(query).hasToString("(text_no_index LIKE '%abc%')");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_ilike_on_index_off_column_falls_back_to_generic_query() {
        Query query = convert("text_no_index ILIKE '%abc%'");
        assertThat(query).hasToString("(text_no_index ILIKE '%abc%')");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_not_like_any_on_index_off_column_falls_back_to_generic_query() {
        Query query = convert("text_no_index not LIKE any(['%abc%'])");
        assertThat(query).hasToString("(text_no_index NOT LIKE ANY(['%abc%']))");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_ilike_any_on_index_off_column_falls_back_to_generic_query() {
        Query query = convert("text_no_index ILIKE any(['%abc%'])");
        assertThat(query).hasToString("(text_no_index ILIKE ANY(['%abc%']))");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }
}
