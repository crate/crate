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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.testing.SQLResponse;

public class SysJobsTest extends IntegTestCase {

    @After
    public void resetStatsEnabled() throws Exception {
        execute("reset global stats.enabled");
    }

    @Test
    public void testQueryAllColumns() throws Exception {
        String stmt = "select * from sys.jobs";

        // the response contains all current jobs, if the tests are executed in parallel
        // this might be more then only the "select * from sys.jobs" statement.
        SQLResponse response = execute(stmt);
        List<String> statements = new ArrayList<>();

        for (Object[] objects : response.rows()) {
            assertNotNull(objects[0]);
            statements.add((String) objects[3]);
        }
        assertThat(statements.contains(stmt)).isTrue();
    }
}
