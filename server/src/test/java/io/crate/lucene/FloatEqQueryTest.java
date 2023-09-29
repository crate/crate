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

import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

public class FloatEqQueryTest extends LuceneQueryBuilderTest {
    @Override
    public String createStmt() {
        return """
                create table m (
                a1 float,
                a2 float index off,
                a3 float storage with (columnstore = false),
                a4 float index off storage with (columnstore = false)
            )
            """;
    }

    @Test
    public void test_FloatEqQuery_termQuery() {
        Query query = convert("a1 = 1.1");
        assertThat(query.getClass().getName()).endsWith("FloatPoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a1:[1.1 TO 1.1]");

        query = convert("a2 = 1.1");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        long l = NumericUtils.floatToSortableInt(1.1f);
        assertThat(query).hasToString("a2:[" + l + " TO " + l + "]");

        query = convert("a3 = 1.1");
        assertThat(query.getClass().getName()).endsWith("FloatPoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a3:[1.1 TO 1.1]");

        query = convert("a4 = 1.1");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(a4 = 1.1)");
    }

    @Test
    public void test_FloatEqQuery_rangeQuery() {
        Query query = convert("a1 > 1.1");
        assertThat(query.getClass().getName()).endsWith("FloatPoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a1:[1.1000001 TO Infinity]");

        query = convert("a2 < 1.1");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        long l = NumericUtils.floatToSortableInt(Float.NEGATIVE_INFINITY);
        long l2 = NumericUtils.floatToSortableInt(FloatPoint.nextDown(1.1f));
        assertThat(query).hasToString("a2:[" + l + " TO " + l2 + "]");

        query = convert("a3 >= 1.1");
        assertThat(query.getClass().getName()).endsWith("FloatPoint$1"); // the query class is anonymous
        assertThat(query).hasToString("a3:[1.1 TO Infinity]");

        query = convert("a4 <= 1.1");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(a4 <= 1.1)");
    }
}
