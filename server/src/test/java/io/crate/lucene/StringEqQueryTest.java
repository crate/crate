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

public class StringEqQueryTest extends LuceneQueryBuilderTest {
    @Override
    protected String createStmt() {
        return """
                create table m (
                a1 text,
                a2 text index off,
                a3 text storage with (columnstore = false),
                a4 text index off storage with (columnstore = false),
                a5 text INDEX using fulltext,
                a6 text INDEX using fulltext storage with (columnstore = false),
                arr1 array(text),
                arr2 array(text) index off,
                arr3 array(text) storage with (columnstore = false),
                arr4 array(text) index off storage with (columnstore = false),
                arr5 array(text) INDEX using fulltext,
                arr6 array(text) INDEX using fulltext storage with (columnstore = false)
            )
            """;
    }

    @Test
    public void test_StringEqQuery_termQuery() {
        Query query = convert("a1 = 'abc'");
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
        assertThat(query).hasToString("a1:abc");

        query = convert("a2 = 'abc'");
        // SortedSetDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedSetDocValuesRangeQuery");
        assertThat(query).hasToString("a2:[[61 62 63] TO [61 62 63]]");

        query = convert("a3 = 'abc'");
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
        assertThat(query).hasToString("a3:abc");

        query = convert("a4 = 'abc'");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(a4 = 'abc')");

        query = convert("a5 = 'abc'");
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
        assertThat(query).hasToString("a5:abc");

        query = convert("a6 = 'abc'");
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
        assertThat(query).hasToString("a6:abc");
    }

    @Test
    public void test_StringEqQuery_rangeQuery() {
        Query query = convert("a1 > 'abc'");
        assertThat(query).isExactlyInstanceOf(TermRangeQuery.class);
        assertThat(query).hasToString("a1:{abc TO *}");

        query = convert("a2 < 'abc'");
        // SortedSetDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedSetDocValuesRangeQuery");
        assertThat(query).hasToString("a2:{* TO [61 62 63]}");

        query = convert("a3 >= 'abc'");
        assertThat(query).isExactlyInstanceOf(TermRangeQuery.class);
        assertThat(query).hasToString("a3:[abc TO *}");

        query = convert("a4 <= 'abc'");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(a4 <= 'abc')");

        query = convert("a5 >= 'abc'");
        assertThat(query).isExactlyInstanceOf(TermRangeQuery.class);
        assertThat(query).hasToString("a5:[abc TO *}");

        query = convert("a6 > 'abc'");
        assertThat(query).isExactlyInstanceOf(TermRangeQuery.class);
        assertThat(query).hasToString("a6:{abc TO *}");
    }

    @Test
    public void test_StringEqQuery_termsQuery() {
        Query query = convert("arr1 = ['abc']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        BooleanClause clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.getQuery();
        assertThat(query).isExactlyInstanceOf(TermInSetQuery.class);
        assertThat(query).hasToString("arr1:(abc)");

        query = convert("arr2 = ['abc']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.getQuery();
        // SortedSetDocValuesField.newSlowSetQuery is equal to TermInSetQuery + MultiTermQuery.DOC_VALUES_REWRITE
        assertThat(query).isExactlyInstanceOf(TermInSetQuery.class);
        assertThat(((TermInSetQuery) query).getRewriteMethod()).isEqualTo(MultiTermQuery.DOC_VALUES_REWRITE);
        assertThat(query).hasToString("arr2:(abc)");

        query = convert("arr3 = ['abc']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.getQuery();
        assertThat(query).isExactlyInstanceOf(TermInSetQuery.class);
        assertThat(query).hasToString("arr3:(abc)");

        query = convert("arr4 = ['abc']");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(arr4 = ['abc'])");

        query = convert("arr5 = ['abc']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.getQuery();
        assertThat(query).isExactlyInstanceOf(TermInSetQuery.class);
        assertThat(query).hasToString("arr5:(abc)");

        query = convert("arr6 = ['abc']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.getQuery();
        assertThat(query).isExactlyInstanceOf(TermInSetQuery.class);
        assertThat(query).hasToString("arr6:(abc)");
    }
}
