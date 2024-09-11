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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.lucene.search.Query;
import org.junit.Test;

public class ThreeValuedLogicQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testNotAnyEqWith3vl() {
        assertThat(convert("NOT 10 = ANY(y_array)")).hasToString(
            "+(+*:* -y_array:[10 TO 10]) #(NOT (10::bigint = ANY(y_array)))");
        assertThat(convert("NOT d = ANY([1,2,3])")).hasToString(
            "+(+*:* -d:{1.0 2.0 3.0}) #(NOT (d = ANY([1.0, 2.0, 3.0])))");
    }

    @Test
    public void testNotAnyEqWithout3vl() {
        assertThat(convert("NOT ignore3vl(20 = ANY(y_array))")).hasToString(
            "+(+*:* -y_array:[20 TO 20])");
        assertThat(convert("NOT ignore3vl(d = ANY([1,2,3]))")).hasToString(
            "+(+*:* -d:{1.0 2.0 3.0})");
    }

    @Test
    public void testComplexOperatorTreeWith3vlAndIgnore3vl() {
        assertThat(convert("NOT name = 'foo' AND NOT ignore3vl(name = 'bar')")).hasToString(
            "+(+(+*:* -name:foo) +FieldExistsQuery [field=name]) +(+(+*:* -name:bar))");
    }

    @Test
    public void testNullIsReplacedWithFalseToCreateOptimizedQuery() {
        Query q1 = convert("null or name = 'foo'");
        Query q2 = convert("name = 'foo'");

        assertThat(q1)
            .isEqualTo(q2)
            .hasToString("name:foo");
    }

    @Test
    public void test_negated_and_three_value_query() {
        // make sure there is no field-exists-query for the references
        assertThat(convert("NOT (x AND f)")).hasToString(
            "+(+*:* -(+x +f)) #(NOT (x AND f))");
    }

    @Test
    public void test_negated_concat_with_three_valued_logic() {
        assertThat(convert("NOT (x || 1) < -1")).hasToString(
            "+(+*:* -((x || '1') < -1))");
    }

    @Test
    public void test_negated_concat_ws_with_three_valued_logic() {
        // Important, y is nullable column.
        assertThat(convert("NOT CONCAT_WS('dummy', y, false)")).hasToString(
            "+(+*:* -concat_ws('dummy', y, 'f')) #(NOT concat_ws('dummy', y, 'f'))");
    }

    @Test
    public void test_nullif() {
        assertThat(convert("NULLIF(2, x) != 1")).hasToString(
            "+(+*:* -(nullif(2, x) = 1)) #(NOT (nullif(2, x) = 1))");
    }

    @Test
    public void test_negated_or() {
        assertThat(convert("NOT (x OR y) > true")).hasToString(
            "+(+*:* -((x OR y) > true)) #(NOT ((x OR y) > true))");
    }

    @Test
    public void test_not_on_pg_encoding_to_char() {
        assertThat(convert("NOT (PG_ENCODING_TO_CHAR(x))"))
            .hasToString("+(+*:* -pg_catalog.pg_encoding_to_char(x)) #(NOT pg_catalog.pg_encoding_to_char(x))");
    }

    @Test
    public void test_not_on_pg_get_function_result() {
        assertThat(convert("NOT (PG_GET_FUNCTION_RESULT(x))"))
            .hasToString("+(+*:* -pg_catalog.pg_get_function_result(x)) #(NOT pg_catalog.pg_get_function_result(x))");
    }

    @Test
    public void test_not_on_pg_get_partkeydef() {
        assertThat(convert("NOT (PG_GET_PARTKEYDEF(x))"))
            .hasToString("+(+*:* -pg_catalog.pg_get_partkeydef(x)) #(NOT pg_catalog.pg_get_partkeydef(x))");
    }

    @Test
    public void test_not_on_current_setting() {
        assertThat(convert("NOT (CURRENT_SETTING(name))"))
            .hasToString("+(+*:* -pg_catalog.current_setting(name)) #(NOT pg_catalog.current_setting(name))");

        // overload with 2 arguments
        assertThat(convert("NOT (CURRENT_SETTING(name, true))"))
            .hasToString("+(+*:* -pg_catalog.current_setting(name, true)) #(NOT pg_catalog.current_setting(name, true))");
    }

    @Test
    public void test_not_on_has_privilege_functions() {
        assertThat(convert("NOT (has_database_privilege(name, 'connect'))"))
            .hasToString("+(+*:* -pg_catalog.has_database_privilege(name, 'connect')) +FieldExistsQuery [field=name]");
        assertThat(convert("NOT (has_schema_privilege(name, 'usage'))"))
            .hasToString("+(+*:* -pg_catalog.has_schema_privilege(name, 'usage')) +FieldExistsQuery [field=name]");
        assertThat(convert("NOT (has_table_privilege(name, 'select'))"))
            .hasToString("+(+*:* -pg_catalog.has_table_privilege(name, 'select')) +FieldExistsQuery [field=name]");
    }

    @Test
    public void test_negated_format_type_with_three_valued_logic() {
        assertThat(convert("NOT pg_catalog.format_type(x, null)")).hasToString(
            "+(+*:* -pg_catalog.format_type(x, NULL)) #(NOT pg_catalog.format_type(x, NULL))");
    }

    @Test
    public void test_negated_cast_on_object() {
        assertThat(convert("NOT (cast(obj as string))")).hasToString(
            "+(+*:* -cast(obj AS text)) #(NOT cast(obj AS text))");
    }
}
