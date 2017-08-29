/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.language;

import com.google.common.collect.ImmutableList;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.Schemas;
import io.crate.module.JavaScriptLanguageModule;
import io.crate.settings.SharedSettings;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, randomDynamicTemplates = false)
public class JavaScriptUDFIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(JavaScriptLanguageModule.LANG_JS_ENABLED.getKey(), true)
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), true).build();
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
        assertFunctionIsCreatedOnAll(Schemas.DOC_SCHEMA_NAME, "subtract_js", ImmutableList.of(DataTypes.LONG, DataTypes.LONG));
        execute("SELECT SUBTRACT_JS(a, b) FROM test ORDER BY a ASC");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows()[0][0], is(2L));
        assertThat(response.rows()[1][0], is(3L));
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
        assertFunctionIsCreatedOnAll("test", "subtract", ImmutableList.of(DataTypes.INTEGER, DataTypes.INTEGER));
        execute("SELECT test.subtract(a, b) FROM test.t ORDER BY 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("0\n1\n2\n"));
    }
}
