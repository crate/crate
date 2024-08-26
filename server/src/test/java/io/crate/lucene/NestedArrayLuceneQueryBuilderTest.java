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

import org.junit.Test;

public class NestedArrayLuceneQueryBuilderTest extends LuceneQueryBuilderTest {

    @Override
    protected String createStmt() {
        return """
            create table t (
                a int[][]
            )
            """;
    }

    @Test
    public void test_nested_array_equals() {
        var query = convert("a = [[1], [1, 2], null]");
        // pre-filter by a terms query with 1 and 2 then a generic function query to make sure an exact match
        assertThat(query.toString()).isEqualTo("+a:{1 2} +(a = [[1], [1, 2], NULL])");
    }

    @Test
    public void test_empty_nested_array_equals() {
        var query = convert("a = [[]]");
        assertThat(query.toString()).isEqualTo("+_array_length_a:[0 TO 0] +(a = [[]])");
    }

    @Test
    public void test_subscript_nested_array_equals() {
        var query = convert("a[1] = [1, 2]");
        // pre-filter by a terms query with 1 and 2 then a generic function query to make sure an exact match
        assertThat(query.toString()).isEqualTo("+a:{1 2} #(a[1] = [1, 2])");
    }

    @Test
    public void test_any_equals_nested_array_literal() {
        var query = convert("a = any([ [ [1], [1, 2] ], [ [3], [4, 5] ] ])");
        // pre-filter by a terms query with 1, 2, 3, 4, 5 then a generic function query to make sure an exact match
        assertThat(query.toString()).isEqualTo("+a:{1 2 3 4 5} #(a = ANY([[[1], [1, 2]], [[3], [4, 5]]]))");
    }
}
