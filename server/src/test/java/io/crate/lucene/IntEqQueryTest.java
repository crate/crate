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

public class IntEqQueryTest extends LuceneQueryBuilderTest {
    @Override
    protected String createStmt() {
        return """
                create table m (
                a1 int,
                a2 int index off,
                a3 int storage with (columnstore = false),
                a4 int index off storage with (columnstore = false),
                arr1 int[],
                arr2 int[] index off,
                arr3 int[] storage with (columnstore = false),
                arr4 int[] index off storage with (columnstore = false)
            )
            """;
    }

    @Test
    public void test_IntEqQuery_termQuery() {
        Query query = convert("a1 = 1");
        assertThat(query.getClass().getName()).endsWith("IntPoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a1:[1 TO 1]");

        query = convert("a2 = 1");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        assertThat(query).hasToString("a2:[1 TO 1]");

        query = convert("a3 = 1");
        assertThat(query.getClass().getName()).endsWith("IntPoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a3:[1 TO 1]");

        query = convert("a4 = 1");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(a4 = 1)");
    }

    @Test
    public void test_IntEqQuery_rangeQuery() {
        Query query = convert("a1 > 1");
        assertThat(query.getClass().getName()).endsWith("IntPoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a1:[2 TO 2147483647]");

        query = convert("a2 < 1");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        assertThat(query).hasToString("a2:[-2147483648 TO 0]");

        query = convert("a3 >= 1");
        assertThat(query.getClass().getName()).endsWith("IntPoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a3:[1 TO 2147483647]");

        query = convert("a4 <= 1");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(a4 <= 1)");
    }

    @Test
    public void test_IntEqQuery_termsQuery() {
        Query query = convert("arr1 = [1,2,3]");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        BooleanClause clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.getQuery();
        assertThat(query.getClass().getName()).endsWith("IntPoint$3");
        assertThat(query).hasToString("arr1:{1 2 3}");

        query = convert("arr2 = [1,2,3]");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.getQuery();
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesSetQuery");
        assertThat(query).hasToString("arr2: [1, 2, 3]");

        query = convert("arr3 = [1,2,3]");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        clause = ((BooleanQuery) query).clauses().get(0);
        query = clause.getQuery();
        assertThat(query.getClass().getName()).endsWith("IntPoint$3");
        assertThat(query).hasToString("arr3:{1 2 3}");

        query = convert("arr4 = [1,2,3]");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(arr4 = [1, 2, 3])");
    }
}
