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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.Asserts;

@IntegTestCase.ClusterScope(numClientNodes = 0, supportsDedicatedMasters = false)
public class SysNodesITest extends IntegTestCase {

    /**
     * See {@link #test_node_attributes()}
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        return Settings.builder().put(settings)
            .put("node.attr.color", getColor(nodeOrdinal))
            .put("node.attr.ordinal", nodeOrdinal)
            .build();
    }

    private static String getColor(int nodeOrdinal) {
        return nodeOrdinal % 2 == 0 ? "blue" : "red";
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Test
    public void testNoMatchingNode() throws Exception {
        execute("select id, name, hostname from sys.nodes where id = 'does-not-exist'");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testScalarEvaluatesInErrorOnSysNodes() throws Exception {
        Asserts.assertSQLError(() -> execute("select 1/0 from sys.nodes"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(BAD_REQUEST, 4000)
                .hasMessageContaining("/ by zero");

    }

    @Test
    public void testRestUrl() throws Exception {
        execute("select rest_url from sys.nodes");
        assertThat((String) response.rows()[0][0]).startsWith("127.0.0.1:");
    }

    @Test
    public void test_node_attributes() throws Exception {
        execute("SELECT attributes FROM sys.nodes ORDER BY name");
        int numDataNodes = cluster().numDataNodes();
        String[] expectedRows = new String[numDataNodes];
        for (int i = 0; i < numDataNodes; i++) {
            expectedRows[i] = "{color=" + getColor(i) + ", ordinal=" + i + "}";
        }
        assertThat(response).hasRows(expectedRows);
    }

    @Test
    public void test_filter_on_not_selected_column_on_sys_nodes_returns_record() throws Exception {
        execute("select fs from sys.nodes where name = 'node_s0'");
        assertThat(response.rowCount()).isEqualTo(1L);
    }
}
