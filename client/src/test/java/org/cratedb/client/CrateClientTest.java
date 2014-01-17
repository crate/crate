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

package org.cratedb.client;

import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 0)
public class CrateClientTest extends CrateIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testCreateClient() throws Exception {

        String nodeName = cluster().startNode();

        int port = ((InetSocketTransportAddress) cluster()
            .getInstance(TransportService.class)
            .boundAddress().boundAddress()).address().getPort();

        client(nodeName).prepareIndex("test", "default", "1")
            .setRefresh(true)
            .setSource("{}")
            .execute()
            .actionGet();

        CrateClient client = new CrateClient("localhost:" +  port);
        SQLResponse r = client.sql("select \"_id\" from test").actionGet();

        assertEquals(1, r.rows().length);
        assertEquals("_id", r.cols()[0]);
        assertEquals("1", r.rows()[0][0]);

        System.out.println(Arrays.toString(r.cols()));
        for (Object[] row: r.rows()){
            System.out.println(Arrays.toString(row));
        }

    }
}
