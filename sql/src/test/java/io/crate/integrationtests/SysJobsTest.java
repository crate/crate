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
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SysJobsTest extends ClassLifecycleIntegrationTest {

    private SQLTransportExecutor e;

    @Before
    public void before() throws Exception {
        e = SQLTransportExecutor.create(ClassLifecycleIntegrationTest.GLOBAL_CLUSTER);
    }

    @Test
    public void testQueryAllColumns() throws Exception {
        e.exec("set global stats.enabled = true");
        String stmt = "select * from sys.jobs";

        // the response contains all current jobs, if the tests are executed in parallel
        // this might be more then only the "select * from sys.jobs" statement.
        SQLResponse response = e.exec(stmt);
        List<String> statements = new ArrayList<>();

        for (Object[] objects : response.rows()) {
            assertNotNull(objects[0]);
            statements.add((String)objects[2]);
        }
        assertTrue(statements.contains(stmt));
        e.exec("set global stats.enabled = false");
    }
}
