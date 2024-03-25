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

package io.crate.operation.language;


import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.module.PolyglotLanguagesModule;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

@IntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class JavaScriptUDFIntegrationTest extends IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(PolyglotLanguagesModule.LANG_JS_ENABLED.getKey(), true)
            .put(PolyglotLanguagesModule.LANG_PYTHON_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(JavaScriptProxyTestPlugin.class);
        return plugins;
    }

    @Before
    public void beforeTest() {
        execute("create table test (a long, b long) clustered by(a) into 2 shards");
        execute("insert into test (a, b) values (?, ?)", new Object[][]{
            new Object[]{5L, 3L},
            new Object[]{10L, 7L}
        });
        refresh();
    }

    @Test
    public void testJavascriptFunction() throws Exception {
        execute("CREATE FUNCTION subtract_js(LONG, LONG) " +
                "RETURNS LONG LANGUAGE JAVASCRIPT AS 'function subtract_js(x, y) { return x-y; }'");
        assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "subtract_js", List.of(DataTypes.LONG, DataTypes.LONG));
        execute("SELECT SUBTRACT_JS(a, b) FROM test ORDER BY a ASC");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(2L);
        assertThat(response.rows()[1][0]).isEqualTo(3L);
    }

    @Test
    public void test_python_function() throws Exception {
        execute("""
            create function subtract_py(long, long)
            returns long language python as $$
def subtract_py(x, y):
    return x - y
$$
            """
        );
        assertFunctionIsCreatedOnAll(
            sqlExecutor.getCurrentSchema(),
            "subtract_py",
            List.of(DataTypes.LONG, DataTypes.LONG)
        );
        execute("SELECT subtract_py(a, b) FROM test ORDER BY a ASC");
        assertThat(response).hasRows(
            "2",
            "3"
        );
    }

    @Test
    public void testBuiltinFunctionOverloadWithOrderBy() throws Exception {
        // this is a regression test that shows that the correct user-defined function implementations are returned
        // and not the built-in ones
        // the query got stuck because we used on built-in function lookup (return type long) and one udf lookup (return type int)
        // which caused a type mismatch when comparing values in ORDER BY
        execute("CREATE TABLE test.t (a INTEGER, b INTEGER) WITH (number_of_replicas=0)");
        execute("INSERT INTO test.t (a, b) VALUES (1, 1), (2, 1), (3, 1)");
        refresh("test.t");
        execute("CREATE FUNCTION test.subtract(integer, integer) RETURNS INTEGER LANGUAGE javascript AS 'function subtract(x, y){ return x-y; }'");
        assertFunctionIsCreatedOnAll("test", "subtract", List.of(DataTypes.INTEGER, DataTypes.INTEGER));
        execute("SELECT test.subtract(a, b) FROM test.t ORDER BY 1");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("0\n1\n2\n");
    }

    @Test
    public void test_udf_that_requires_array_arg_can_be_used_as_generated_column() throws Exception {
        execute(
            "CREATE OR REPLACE FUNCTION arr_max(xs array(real)) " +
            " RETURNS real " +
            " LANGUAGE JAVASCRIPT " +
            " AS 'function arr_max(xs) { " +
            "   return Math.max.apply(null, xs); " +
            " }'"
        );
        assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "arr_max", List.of(new ArrayType<>(DataTypes.FLOAT)));
        execute("create table tbl (xs real[], x as arr_max(xs))");
        execute("insert into tbl (xs) values ([10.5, 27.4])");
        execute("refresh table tbl");
        execute("select x from tbl");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("27.4\n");
    }

    // This tracks a bug: https://github.com/crate/crate/issues/13386
    @Test
    public void test_udf_nested_object_accesses() {
        execute("""
                    CREATE FUNCTION nested (obj OBJECT)
                        RETURNS STRING
                        LANGUAGE JAVASCRIPT
                        AS 'function nested(obj){
                            return obj["l1"]["l2"]["l3"];
                        }';
                        """);
        execute("SELECT nested({l1={l2={l3='Hello'}}})");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("Hello\n");
    }
}
