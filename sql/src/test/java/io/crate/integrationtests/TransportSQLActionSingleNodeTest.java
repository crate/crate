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


import io.crate.test.integration.CrateIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class TransportSQLActionSingleNodeTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testUnassignedShards() throws Exception {
        int numReplicas = 3;
        int numShards = 5;
        long expectedUnassignedShards = numShards * numReplicas; // calculation is correct for cluster.numNodes = 1

        execute("create table locations (id integer primary key, name string) " +
                "clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        ensureYellow();

        execute("select count(*) from sys.shards where table_name = 'locations' and state = 'UNASSIGNED'");
        assertThat(response.rowCount(), is(1L));
        assertEquals(1, response.cols().length);
        assertThat((Long) response.rows()[0][0], is(expectedUnassignedShards));
    }

}
