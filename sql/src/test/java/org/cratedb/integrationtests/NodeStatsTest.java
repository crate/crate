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

package org.cratedb.integrationtests;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL, numNodes = 1)
public class NodeStatsTest extends SQLTransportIntegrationTest {

    @Test
    @Repeat(iterations = 100)
    public void testSysNodesMem() throws Exception {
        SQLResponse response = execute("select mem['free'], mem['used'], mem['free_percent'], mem['used_percent'] from sys.nodes limit 1");
        long free = (long)response.rows()[0][0];
        long used = (long)response.rows()[0][1];

        double free_percent = (int) response.rows()[0][2] * 0.01;
        double used_percent = (int) response.rows()[0][3] * 0.01;

        double calculated_free_percent = free / (double)(free + used) ;
        double calculated_used_percent = used/ (double)(free + used) ;

        double max_delta = 0.02; // result should not differ from calculated result more than 2%
        double free_delta = Math.abs(calculated_free_percent - free_percent);
        double used_delta = Math.abs(calculated_used_percent - used_percent);
        assertThat(free_delta, is(lessThan(max_delta)));
        assertThat(used_delta, is(lessThan(max_delta)));

    }

}
