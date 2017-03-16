/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, randomDynamicTemplates = false)
public class CreateFunctionIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Before
    public void beforeTest() {
        execute("create table test " +
            "(id integer, s string, obj object, arr array(integer)) " +
            "clustered by(id) into 2 shards");
        execute("insert into test (id, s, obj, arr) values (1, 'foo', {foo='bar'}, [1, 2]), (1, 'bar', {foo='bar'}, [1, 2])");
        refresh();
    }

    @Test
    public void testCreateFunction() throws Exception {
        execute("create function foo(bar string) returns string language javascript as 'function foo(x) { return x; }'");
        execute("select foo(s) from test");
        assertEquals(2, response.rowCount());
    }
}
