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
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.client.Client;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.action.sql.Sessions;
import io.crate.testing.SQLTransportExecutor;

@IntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 1)
public class BulkInsertOnClientNodeTest extends IntegTestCase {

    public BulkInsertOnClientNodeTest() {
        super(new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {

                private String nodeName;

                @Override
                public Client client() {
                    // make sure we use a client node (started with client=true)
                    Client client = cluster().client();
                    nodeName = client.settings().get("node.name");
                    return client;
                }

                @Override
                public String pgUrl() {
                    return null;
                }

                @Override
                public Sessions sqlOperations() {
                    return cluster().getInstance(Sessions.class, nodeName());
                }

                private String nodeName() {
                    if (nodeName == null) {
                        Client client = client();
                        nodeName = client.settings().get("name");
                    }
                    return nodeName;
                }
            }
        ));
    }

    @Test
    public void testInsertBulkDifferentTypesResultsInStreamingFailure() throws Exception {
        execute("create table test (id integer primary key) " +
                "clustered into 2 shards with (column_policy='dynamic', number_of_replicas=0)");
        List<Long> rowCounts = Arrays.stream(
            execute("insert into test (id, value) values (?, ?)",
                new Object[][]{
                    new Object[]{1, 1},                                 // use id 1 to ensure shard 0
                    new Object[] {3, Map.of("foo", 127)}         // use id 3 to ensure shard 1
                })).boxed().collect(Collectors.toList());
        assertThat(rowCounts).hasSize(2);
        assertThat(rowCounts, Matchers.anyOf(
            contains(1L, -2L),
            contains(-2L, 1L)
        ));
    }
}
