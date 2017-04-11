/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;


import io.crate.action.sql.SQLOperations;
import io.crate.testing.SQLBulkResponse;
import io.crate.testing.SQLTransportExecutor;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.HashMap;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 1, transportClientRatio = 0)
public class BulkInsertOnClientNodeTest extends SQLTransportIntegrationTest {

    public BulkInsertOnClientNodeTest() {
        super(new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {

                private String nodeName;

                @Override
                public Client client() {
                    // make sure we use a client node (started with client=true)
                    Client client = internalCluster().client();
                    nodeName = client.settings().get("node.name");
                    return client;
                }

                @Override
                public String pgUrl() {
                    return null;
                }

                @Override
                public SQLOperations sqlOperations() {
                    return internalCluster().getInstance(SQLOperations.class, nodeName());
                }

                private String nodeName() {
                    if (nodeName == null) {
                        Client client= client();
                        nodeName = client.settings().get("name");
                    }
                    return nodeName;
                }
            }
        ));
    }

    @Test
    public void testInsertBulkDifferentTypesResultsInStreamingFailure() throws Exception {
        execute("create table test (id integer primary key) clustered into 2 shards with (number_of_replicas=0)");
        ensureYellow();
        SQLBulkResponse response = execute("insert into test (id, value) values (?, ?)",
            new Object[][]{
                new Object[]{1, 1},                                 // use id 1 to ensure shard 0
                new Object[]{3, new HashMap<String, Object>() {{    // use id 3 to ensure shard 1
                    put("foo", 127);
                }}},
            });
        // ensure row count is adjusted and no exception is thrown
        // 1 shard must succeed (rowCount 1) while other must fail (rowCount -2)
        assertThat(response.results()[0].rowCount() + response.results()[1].rowCount(), is(-1L));
    }
}
