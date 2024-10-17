/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.junit.Test;

public class CharacterEqQueryTest extends LuceneQueryBuilderTest {
    @Override
    protected String createStmt() {
        return """
                create table m (
                c1 char(3),
                c2 char(3) index off,
                c3 char(3) storage with (columnstore = false),
                c4 char(3) index off storage with (columnstore = false),
                arr1 array(char(3)),
                arr2 array(char(3)) index off,
                arr3 array(char(3)) storage with (columnstore = false),
                arr4 array(char(3)) index off storage with (columnstore = false)
            )
            """;
    }

    @Test
    public void test_StringEqQuery_termQuery() {
        Query query = convert("c1 = 'abc   '");
        assertThat(query)
            .isExactlyInstanceOf(TermQuery.class)
            .hasToString("c1:abc");

        query = convert("c2 = 'ab  c  '");
        // SortedSetDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedSetDocValuesRangeQuery");
        assertThat(query).hasToString("c2:[[61 62 20 20 63] TO [61 62 20 20 63]]");

        query = convert("c3 = 'ab'");
        assertThat(query)
            .isExactlyInstanceOf(TermQuery.class)
            .hasToString("c3:ab ");

        query = convert("c4 = 'abc  '");
        assertThat(query)
            .isExactlyInstanceOf(GenericFunctionQuery.class)
            .hasToString("(c4 = 'abc  ')"); // trailing whitespace are stripped in CharacterType#compare()
    }

    @Test
    public void test_StringEqQuery_rangeQuery() {
        Query query = convert("c1 > 'ab   '");
        assertThat(query)
            .isExactlyInstanceOf(TermRangeQuery.class)
            .hasToString("c1:{ab  TO *}");

        query = convert("c2 < 'ab  c  '");
        // SortedSetDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedSetDocValuesRangeQuery");
        assertThat(query).hasToString("c2:{* TO [61 62 20 20 63]}");

        query = convert("c3 >= 'abc  '");
        assertThat(query)
            .isExactlyInstanceOf(TermRangeQuery.class)
            .hasToString("c3:[abc TO *}");

        query = convert("c4 <= 'abc  '");
        assertThat(query)
            .isExactlyInstanceOf(GenericFunctionQuery.class)
            .hasToString("(c4 <= 'abc  ')"); // trailing whitespace are stripped in CharacterType#compare()
    }

    @Test
    public void test_StringEqQuery_termsQuery() {
        Query query = convert("arr1 = ['abc  ']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        BooleanClause clause = ((BooleanQuery) query).clauses().getFirst();
        query = clause.getQuery();
        assertThat(query)
            .isExactlyInstanceOf(TermInSetQuery.class)
            .hasToString("arr1:(abc)");

        query = convert("arr2 = ['ab  c  ']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().getFirst();
        query = clause.getQuery();
        // SortedSetDocValuesField.newSlowSetQuery is equal to TermInSetQuery + MultiTermQuery.DOC_VALUES_REWRITE
        assertThat(query).isExactlyInstanceOf(TermInSetQuery.class);
        assertThat(((TermInSetQuery) query).getRewriteMethod()).isEqualTo(MultiTermQuery.DOC_VALUES_REWRITE);
        assertThat(query).hasToString("arr2:(ab  c)");

        query = convert("arr3 = ['abc   ']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().getFirst();
        query = clause.getQuery();
        assertThat(query)
            .isExactlyInstanceOf(TermInSetQuery.class)
            .hasToString("arr3:(abc)");

        query = convert("arr4 = ['abc  ']");
        assertThat(query)
            .isExactlyInstanceOf(GenericFunctionQuery.class)
            .hasToString("(arr4 = ['abc  '])"); // trailing whitespace are stripped in CharacterType#compare()
    }
}
