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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.is;

public class SysClusterTest extends SQLTransportIntegrationTest {

    @Test
    public void testSysCluster() throws Exception {
        execute("select id from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat(((String) response.rows()[0][0]).length(), is(22)); // looks like a uuid generated by UUIDs.randomBase64UUID
    }

    public void testSysClusterMasterNode() throws Exception {
        execute("select id from sys.nodes");
        List<String> nodes = new ArrayList<>();
        for (Object[] nodeId : response.rows()) {
            nodes.add((String) nodeId[0]);
        }

        execute("select master_node from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        String node = (String) response.rows()[0][0];
        assertTrue(nodes.contains(node));
    }

    @Test
    public void testExplainSysCluster() throws Exception {
        execute("explain select * from sys.cluster limit 2");
        assertThat(response.rowCount(), is(1L));
        assertThat(
            printedTable(response.rows()),
            is("Limit[2::bigint;0]\n" +
               "  └ Collect[sys.cluster | [id, license, master_node, name, settings] | true]\n")
        );
    }

    @Test
    public void testScalarEvaluatesInErrorOnSysCluster() throws Exception {
        assertThrows(() -> execute("select 1/0 from sys.cluster"),
                     isSQLError(is("/ by zero"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }
}
