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

import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.ClassLifecycleIntegrationTest;
import io.crate.testing.SQLTransportExecutor;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.core.Is.is;

public class SysOperationsTest extends ClassLifecycleIntegrationTest {

    private SQLTransportExecutor executor;

    @Before
    public void before() throws Exception {
        executor = SQLTransportExecutor.create(ClassLifecycleIntegrationTest.GLOBAL_CLUSTER);
        executor.exec("set global stats.enabled = true");
    }

    @After
    public void after() {
        executor.exec("set global stats.enabled = false");
    }

    @Test
    public void testDistinctSysOperations() throws Exception {
        // this tests a distributing collect without shards but DOC level granularity
        SQLResponse response = executor.exec("select distinct name  from sys.operations limit 1");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testQueryNameFromSysOperations() throws Exception {
        SQLResponse resp = executor.exec("select name, job_id from sys.operations order by name asc");

        // usually this should return collect on 2 nodes, localMerge on 1 node
        // but it could be that the collect is finished before the localMerge task is started in which
        // case it is missing.

        assertThat(resp.rowCount(), Matchers.greaterThanOrEqualTo(2L));
        List<String> names = new ArrayList<>();
        for (Object[] objects : resp.rows()) {
            names.add((String) objects[0]);
        }
        Collections.sort(names);
        assertTrue(names.contains("collect"));
    }

    @Test
    public void testNodeExpressionOnSysOperations() throws Exception {
        executor.exec("select * from sys.nodes");
        SQLResponse response = executor.exec("select _node['name'], id from sys.operations limit 1");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0].toString(), startsWith("node"));
    }


}
