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

import org.elasticsearch.client.Client;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.action.sql.Sessions;
import io.crate.testing.SQLTransportExecutor;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 2, supportsDedicatedMasters = false)
public class ClientNodeIntegrationTest extends IntegTestCase {

    public ClientNodeIntegrationTest() {
        super(new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    // make sure we use a client node (started with client=true)
                    return cluster().coordOnlyNodeClient();
                }

                @Override
                public String pgUrl() {
                    return null;
                }

                @Override
                public Sessions sqlOperations() {
                    return cluster().getInstance(Sessions.class);
                }
            }
        ));
    }

    /**
     * Test that requests to all nodes (e.g. sys.nodes) will work if run on a client node
     * (Without a patch, ES will prevent client-to-client connections)
     * related patch: https://github.com/crate/elasticsearch/commit/f0c180e9ea5f84a4540e982c9e5c6af0be2c8143
     */
    @Test
    public void testNodesSQLRequestOnClientNode() throws Exception {
        execute("select * from sys.nodes");
        assertThat(response.rowCount()).isEqualTo(3L);
    }
}
