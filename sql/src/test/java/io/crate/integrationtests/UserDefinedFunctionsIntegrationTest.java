/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.integrationtests;

import com.google.common.collect.ImmutableList;
import io.crate.types.DataTypes;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, randomDynamicTemplates = false)
public class UserDefinedFunctionsIntegrationTest extends SQLTransportIntegrationTest {

    private final Object[][] rows = new Object[][]{
        new Object[]{1L, "foo", new HashMap<String, Long>() {{
            put("foo", 2L);
        }}, new Object[]{1L, 2L}
        },
        new Object[]{2L, "bar", new HashMap<String, Long>() {{
            put("foo", 2L);
        }}, new Object[]{2L, 3L}
        }
    };

    @Before
    public void beforeTest() {
        execute("create table test " +
            "(id long, str string, obj object, arr array(long)) " +
            "clustered by(id) into 2 shards");

        execute("insert into test (id, str, obj, arr) values (?, ?, ?, ?)", rows);
        refresh();
    }

    @Test
    public void testCreateFunctionWithObjectInputType() throws Exception {
        execute("create function test(object)" +
            " returns long language javascript as 'function test(x) { return x.foo; }'");
        waitForFunctionCreatedOnAll("doc", "test", ImmutableList.of(DataTypes.OBJECT));

        execute("select test(obj) from test where id = 1");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(2L));
    }

    @Test
    public void testCreateOverloadedFunction() throws Exception {
        execute("create function foo(object)" +
            " returns string language javascript as 'function foo(x) { return \"1\"; }'");
        waitForFunctionCreatedOnAll("doc", "foo", ImmutableList.of(DataTypes.OBJECT));

        execute("create function foo(string)" +
            " returns string language javascript as 'function foo(x) { return x; }'");
        waitForFunctionCreatedOnAll("doc", "foo", ImmutableList.of(DataTypes.STRING));

        execute("select foo(str) from test where id = 2");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is("bar"));
    }

    @Test
    public void testCreateFunctionInCustomSchema() throws Exception {
        execute("create function test.custom_function(string)" +
            " returns string language javascript as 'function custom_function(x) { return x; }'");
        waitForFunctionCreatedOnAll("test", "custom_function", ImmutableList.of(DataTypes.STRING));

        execute("select test.custom_function(str) from test where id = 2");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is("bar"));
    }
}
