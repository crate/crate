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
        assertFunctionIsCreatedOnAll(Schemas.DEFAULT_SCHEMA_NAME, "subtract_js", ImmutableList.of(DataTypes.LONG, DataTypes.LONG));
        execute("SELECT SUBTRACT_JS(a, b) FROM test ORDER BY a ASC");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows()[0][0], is(2L));
        assertThat(response.rows()[1][0], is(3L));
    }
}
