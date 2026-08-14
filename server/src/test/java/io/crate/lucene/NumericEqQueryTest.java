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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

public class NumericEqQueryTest extends LuceneQueryBuilderTest {

    @Override
    protected String createStmt() {
        return """
            create table n (
                x numeric(18, 2),
                y numeric(38, 2),
                xarr numeric(18, 2)[],
                yarr numeric(38, 2)[],
                x_no_index numeric(18, 2) index off,
                x_no_columnstore numeric(18, 2) storage with (columnstore = false),
                x_no_index_no_columnstore numeric(18, 2) index off storage with (columnstore = false),
                y_no_index numeric(38, 2) index off
            )
            """;
    }

    @Test
    public void test_uses_point_range_queries_for_compact_numeric() throws Exception {
        Query query = convert("x = '2746799837116176.76'");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
        assertThat(query.toString()).isEqualTo("x:[274679983711617676 TO 274679983711617676]");

        query = convert("x > '2746799837116176.76'");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
        assertThat(query.toString()).isEqualTo("x:[274679983711617677 TO 999999999999999999]");

        query = convert("x <= '2746799837116176.76'");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
        assertThat(query.toString()).isEqualTo("x:[-999999999999999999 TO 274679983711617676]");
    }

    @Test
    public void test_uses_doc_values_range_queries_for_compact_numeric_without_index() {
        // Without the index there are no points to query, the range must be resolved via doc values
        Query query = convert("x_no_index < 0.00");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        assertThat(query).hasToString("x_no_index:[-999999999999999999 TO -1]");

        query = convert("x_no_index >= 12.34");
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        assertThat(query).hasToString("x_no_index:[1234 TO 999999999999999999]");

        // The index is still used if it is available
        query = convert("x_no_columnstore > 0.00");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
        assertThat(query).hasToString("x_no_columnstore:[1 TO 999999999999999999]");

        // Without an index and without doc values a generic filter is used
        query = convert("x_no_index_no_columnstore <= 0.00");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);

        // A large numeric has no doc values based range query, it falls back to the generic filter
        query = convert("y_no_index < 0.00");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_numeric_comparisons_with_different_precision() {
        String col = randomBoolean() ? "x" : "y";
        assertThat(convert(col + " > 1.111")).isEqualTo(convert(col + " > 1.11"));
        assertThat(convert(col + " > 1.119")).isEqualTo(convert(col + " > 1.11"));
        assertThat(convert(col + " > 1.1100")).isEqualTo(convert(col + " > 1.11"));
        assertThat(convert(col + " > 1.105")).isEqualTo(convert(col + " > 1.10"));
        assertThat(convert(col + " > 1.1")).isEqualTo(convert(col + " > 1.10"));
        assertThat(convert(col + " > 1")).isEqualTo(convert(col + " > 1.00"));

        assertThat(convert(col + " >= 1.111")).isEqualTo(convert(col + " > 1.11"));
        assertThat(convert(col + " >= 1.119")).isEqualTo(convert(col + " > 1.11"));
        assertThat(convert(col + " >= 1.1100")).isEqualTo(convert(col + " > 1.10"));
        assertThat(convert(col + " >= 1.105")).isEqualTo(convert(col + " > 1.10"));
        assertThat(convert(col + " >= 1.1")).isEqualTo(convert(col + " > 1.09"));
        assertThat(convert(col + " >= 1")).isEqualTo(convert(col + " > 0.99"));

        assertThat(convert(col + " < 1.111")).isEqualTo(convert(col + " < 1.12"));
        assertThat(convert(col + " < 1.119")).isEqualTo(convert(col + " < 1.12"));
        assertThat(convert(col + " < 1.1100")).isEqualTo(convert(col + " < 1.11"));
        assertThat(convert(col + " < 1.105")).isEqualTo(convert(col + " < 1.11"));
        assertThat(convert(col + " < 1.1")).isEqualTo(convert(col + " < 1.10"));
        assertThat(convert(col + " < 1")).isEqualTo(convert(col + " < 1.00"));

        assertThat(convert(col + " <= 1.111")).isEqualTo(convert(col + " < 1.12"));
        assertThat(convert(col + " <= 1.119")).isEqualTo(convert(col + " < 1.12"));
        assertThat(convert(col + " <= 1.1100")).isEqualTo(convert(col + " < 1.12"));
        assertThat(convert(col + " <= 1.105")).isEqualTo(convert(col + " < 1.11"));
        assertThat(convert(col + " <= 1.1")).isEqualTo(convert(col + " < 1.11"));
        assertThat(convert(col + " <= 1")).isEqualTo(convert(col + " < 1.01"));

        // negative values
        assertThat(convert(col + " > -1.111")).isEqualTo(convert(col + " > -1.12"));
        assertThat(convert(col + " > -1.119")).isEqualTo(convert(col + " > -1.12"));
        assertThat(convert(col + " > -1.1100")).isEqualTo(convert(col + " > -1.11"));
        assertThat(convert(col + " > -1.105")).isEqualTo(convert(col + " > -1.11"));
        assertThat(convert(col + " > -1.1")).isEqualTo(convert(col + " > -1.10"));
        assertThat(convert(col + " > -1")).isEqualTo(convert(col + " > -1.00"));

        assertThat(convert(col + " >= -1.111")).isEqualTo(convert(col + " > -1.12"));
        assertThat(convert(col + " >= -1.119")).isEqualTo(convert(col + " > -1.12"));
        assertThat(convert(col + " >= -1.1100")).isEqualTo(convert(col + " > -1.12"));
        assertThat(convert(col + " >= -1.105")).isEqualTo(convert(col + " > -1.11"));
        assertThat(convert(col + " >= -1.1")).isEqualTo(convert(col + " > -1.11"));
        assertThat(convert(col + " >= -1")).isEqualTo(convert(col + " > -1.01"));

        assertThat(convert(col + " < -1.111")).isEqualTo(convert(col + " < -1.11"));
        assertThat(convert(col + " < -1.119")).isEqualTo(convert(col + " < -1.11"));
        assertThat(convert(col + " < -1.1100")).isEqualTo(convert(col + " < -1.11"));
        assertThat(convert(col + " < -1.105")).isEqualTo(convert(col + " < -1.10"));
        assertThat(convert(col + " < -1.1")).isEqualTo(convert(col + " < -1.10"));
        assertThat(convert(col + " < -1")).isEqualTo(convert(col + " < -1.00"));

        assertThat(convert(col + " <= -1.111")).isEqualTo(convert(col + " < -1.11"));
        assertThat(convert(col + " <= -1.119")).isEqualTo(convert(col + " < -1.11"));
        assertThat(convert(col + " <= -1.1100")).isEqualTo(convert(col + " < -1.10"));
        assertThat(convert(col + " <= -1.105")).isEqualTo(convert(col + " < -1.10"));
        assertThat(convert(col + " <= -1.1")).isEqualTo(convert(col + " < -1.09"));
        assertThat(convert(col + " <= -1")).isEqualTo(convert(col + " < -0.99"));
    }

    @Test
    public void test_equals_unbounded_numeric_with_larger_scale() {
        assertThat(convert("x = 1.111::numeric")).isExactlyInstanceOf(MatchNoDocsQuery.class);
        assertThat(convert("y = 1.111::numeric")).isExactlyInstanceOf(MatchNoDocsQuery.class);
    }

    @Test
    public void test_term_query_with_same_significant_digits() {
        String col = randomBoolean() ? "x" : "y";
        assertThat(convert(col + " = 1.11::numeric").toString()).isEqualTo(col + ":[111 TO 111]");
        assertThat(convert(col + " = 11.1::numeric").toString()).isEqualTo(col + ":[1110 TO 1110]");
        assertThat(convert(col + " = 111::numeric").toString()).isEqualTo(col + ":[11100 TO 11100]");
    }

    @Test
    public void test_terms_query_with_same_significant_digits() {
        assertThat(convert("xarr = [1.11, 11.1, 111]::numeric[]").toString())
            .isEqualTo("+xarr:{111 1110 11100} +(xarr = [1.11, 11.1, 111.0])");
        assertThat(convert("yarr = [1.11, 11.1, 111]::numeric[]").toString())
            .isEqualTo("+yarr:{111 1110 11100} +(yarr = [1.11, 11.1, 111.0])");
    }

    @Test
    public void test_uses_binary_encoded_range_queries_for_large_numeric() throws Exception {
        Query query = convert("y = '2746799837116176.76'");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
        assertThat(query.toString()).isEqualTo("y:[274679983711617676 TO 274679983711617676]");

        query = convert("y > '2746799837116176.76'");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
        assertThat(query.toString()).isEqualTo("y:[274679983711617677 TO 99999999999999999999999999999999999999]");

        query = convert("y <= '2746799837116176.76'");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
        assertThat(query.toString()).isEqualTo("y:[-99999999999999999999999999999999999999 TO 274679983711617676]");
    }

    @Test
    public void test_out_of_bounds_numeric_values_are_filtered_from_numeric_array_literal() {
        // removing 1.111 because it will never match numeric values with scale = 2; 1.110 is kept since it is equivalent to 1.11
        assertThat(convert("xarr = [1, 1.1, 1.110, 1.111]::numeric[]").toString())
            .isEqualTo("+xarr:{100 110 111} +(xarr = [1.0, 1.1, 1.11, 1.111])"); // generic query still contains 1.111
        assertThat(convert("yarr = [1, 1.1, 1.110, 1.111]::numeric[]").toString())
            .isEqualTo("+yarr:{100 110 111} +(yarr = [1.0, 1.1, 1.11, 1.111])");

        // after removing out of bound values, if the array becomes empty, return MatchNoDocsQuery
        assertThat(convert("xarr = [1.111]::numeric[]").toString())
            .isEqualTo("+MatchNoDocsQuery(\"The given values are out of bounds\") +(xarr = [1.111])");
        assertThat(convert("yarr = [1.111]::numeric[]").toString())
            .isEqualTo("+MatchNoDocsQuery(\"The given values are out of bounds\") +(yarr = [1.111])");
    }
}
