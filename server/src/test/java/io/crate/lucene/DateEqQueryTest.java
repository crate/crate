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
import org.apache.lucene.search.Query;
import org.junit.Test;

public class DateEqQueryTest extends LuceneQueryBuilderTest {

    @Override
    protected String createStmt() {
        return """
                create table m (
                a1 date,
                a2 date index off,
                a3 date storage with (columnstore = false),
                a4 date index off storage with (columnstore = false),
                arr1 date[],
                arr2 date[] index off,
                arr3 date[] storage with (columnstore = false),
                arr4 date[] index off storage with (columnstore = false)
            )
            """;
    }

    // '2020-01-01' is stored as 1577836800000L (midnight UTC of 2020-01-01)
    private static final long DATE_2020_01_01 = 1577836800000L;

    @Test
    public void test_DateEqQuery_termQuery() {
        Query query = convert("a1 = '2020-01-01'");
        assertThat(query.getClass().getName()).endsWith("LongPoint$1");
        assertThat(query).hasToString("a1:[" + DATE_2020_01_01 + " TO " + DATE_2020_01_01 + "]");

        query = convert("a2 = '2020-01-01'");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        assertThat(query).hasToString("a2:[" + DATE_2020_01_01 + " TO " + DATE_2020_01_01 + "]");

        query = convert("a3 = '2020-01-01'");
        assertThat(query.getClass().getName()).endsWith("LongPoint$1");
        assertThat(query).hasToString("a3:[" + DATE_2020_01_01 + " TO " + DATE_2020_01_01 + "]");

        query = convert("a4 = '2020-01-01'");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_DateEqQuery_rangeQuery() {
        Query query = convert("a1 > '2020-01-01'");
        assertThat(query.getClass().getName()).endsWith("LongPoint$1");
        assertThat(query).hasToString("a1:[" + (DATE_2020_01_01 + 1) + " TO " + Long.MAX_VALUE + "]");

        query = convert("a2 < '2020-01-01'");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        assertThat(query).hasToString("a2:[" + Long.MIN_VALUE + " TO " + (DATE_2020_01_01 - 1) + "]");

        query = convert("a3 >= '2020-01-01'");
        assertThat(query.getClass().getName()).endsWith("LongPoint$1");
        assertThat(query).hasToString("a3:[" + DATE_2020_01_01 + " TO " + Long.MAX_VALUE + "]");

        query = convert("a4 <= '2020-01-01'");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_DateEqQuery_termsQuery() {
        Query query = convert("arr1 = ['2020-01-01', '2020-02-01', '2020-03-01']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        BooleanClause clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.query();
        assertThat(query.getClass().getName()).endsWith("LongPoint$3");

        query = convert("arr2 = ['2020-01-01', '2020-02-01', '2020-03-01']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.query();
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesSetQuery");

        query = convert("arr3 = ['2020-01-01', '2020-02-01', '2020-03-01']");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.query();
        assertThat(query.getClass().getName()).endsWith("LongPoint$3");

        query = convert("arr4 = ['2020-01-01', '2020-02-01', '2020-03-01']");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }
}
