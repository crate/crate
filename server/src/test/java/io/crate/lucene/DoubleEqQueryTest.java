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

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

public class DoubleEqQueryTest extends LuceneQueryBuilderTest {
    @Override
    protected String createStmt() {
        return """
                create table m (
                a1 double,
                a2 double index off,
                a3 double storage with (columnstore = false),
                a4 double index off storage with (columnstore = false)
            )
            """;
    }

    @Test
    public void test_DoubleEqQuery_termQuery() {
        Query query = convert("a1 = 1.1");
        assertThat(query.getClass().getName()).endsWith("DoublePoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a1:[1.1 TO 1.1]");

        query = convert("a2 = 1.1");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        long l = NumericUtils.doubleToSortableLong(1.1);
        assertThat(query).hasToString("a2:[" + l + " TO " + l + "]");

        query = convert("a3 = 1.1");
        assertThat(query.getClass().getName()).endsWith("DoublePoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a3:[1.1 TO 1.1]");

        query = convert("a4 = 1.1");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(a4 = 1.1)");
    }

    @Test
    public void test_DoubleEqQuery_rangeQuery() {
        Query query = convert("a1 > 1.1");
        assertThat(query.getClass().getName()).endsWith("DoublePoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a1:[1.1000000000000003 TO Infinity]");

        query = convert("a2 < 1.1");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        long l = NumericUtils.doubleToSortableLong(Double.NEGATIVE_INFINITY);
        long l2 = NumericUtils.doubleToSortableLong(DoublePoint.nextDown(1.1));
        assertThat(query).hasToString("a2:[" + l + " TO " + l2 + "]");

        query = convert("a3 >= 1.1");
        assertThat(query.getClass().getName()).endsWith("DoublePoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a3:[1.1 TO Infinity]");

        query = convert("a4 <= 1.1");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(a4 <= 1.1)");
    }
}
