/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.ClassLifecycleIntegrationTest;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.SQLTransportExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class NodeStatsTest extends ClassLifecycleIntegrationTest {

    private static SQLTransportExecutor executor;

    @Before
    public void before() throws Exception {
        executor = SQLTransportExecutor.create(ClassLifecycleIntegrationTest.GLOBAL_CLUSTER);
    }

    @Test
    public void testSysNodesMem() throws Exception {
        SQLResponse response = executor.exec("select mem['free'], mem['used'], mem['free_percent'], mem['used_percent'] from sys.nodes limit 1");
        long free = (long)response.rows()[0][0];
        long used = (long)response.rows()[0][1];

        double free_percent = ((Number) response.rows()[0][2]).intValue() * 0.01;
        double used_percent = ((Number) response.rows()[0][3]).intValue() * 0.01;

        double calculated_free_percent = free / (double)(free + used) ;
        double calculated_used_percent = used/ (double)(free + used) ;

        double max_delta = 0.02; // result should not differ from calculated result more than 2%
        double free_delta = Math.abs(calculated_free_percent - free_percent);
        double used_delta = Math.abs(calculated_used_percent - used_percent);
        assertThat(free_delta, is(lessThan(max_delta)));
        assertThat(used_delta, is(lessThan(max_delta)));

    }

    @Test
    public void testThreadPools() throws Exception {
        SQLResponse response = executor.exec("select thread_pools from sys.nodes limit 1");

        Object[] threadPools = (Object[]) response.rows()[0][0];
        assertThat(threadPools.length, greaterThanOrEqualTo(1));

        Map<String, Object> threadPool = (Map<String, Object>) threadPools[0];
        assertThat((String) threadPool.get("name"), is("generic"));
        assertThat((Integer) threadPool.get("active"), greaterThanOrEqualTo(0));
        assertThat((Long) threadPool.get("rejected"), greaterThanOrEqualTo(0L));
        assertThat((Integer) threadPool.get("largest"), greaterThanOrEqualTo(0));
        assertThat((Long) threadPool.get("completed"), greaterThanOrEqualTo(0L));
        assertThat((Integer) threadPool.get("threads"), greaterThanOrEqualTo(0));
        assertThat((Integer) threadPool.get("queue"), greaterThanOrEqualTo(0));
    }

    @Test
    public void testThreadPoolValue() throws Exception {
        SQLResponse response = executor.exec("select thread_pools['name'], thread_pools['queue'] from sys.nodes limit 1");
        assertThat(response.rowCount(), is(1L));

        Object[] names = (Object[]) response.rows()[0][0];
        assertThat(names.length, greaterThanOrEqualTo(1));
        assertThat((String) names[0], is("generic"));

        Object[] queues = (Object[]) response.rows()[0][1];
        assertThat(queues.length, greaterThanOrEqualTo(1));
        assertThat((Integer) queues[0], greaterThanOrEqualTo(0));
    }
}
