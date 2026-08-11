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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.node.Node;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.IntegTestCase.Slow;
import org.junit.Test;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@Slow
public class DisruptedRestSQLActionITest extends SQLHttpIntegrationTest {

    private List<String> dataNodes;

    @Override
    protected String setupNodes() {
        cluster().startMasterOnlyNode();
        dataNodes = cluster().startDataOnlyNodes(2);
        return dataNodes.get(1);
    }

    @Test
    public void test_bulk_insert_from_values_reports_errors_per_item_on_unavailable_shard_failurs() throws Exception {
        execute("CREATE TABLE doc.tbl (x INT) CLUSTERED INTO 4 SHARDS with (number_of_replicas = 0)");
        ensureGreen();
        ensureStableCluster(3);
        execute("set global \"cluster.routing.allocation.enable\" = 'none'");
        String dataNode = dataNodes.get(0);
        cluster().stopRandomNode(settings -> Node.NODE_NAME_SETTING.get(settings).equals(dataNode));
        assertBusy(() -> {
            execute("select health from sys.health where table_name = 'tbl'");
            assertThat(response).hasRows("RED");
        });
        // Sending a couple of values to ensure it hits a shard that was on the stopped node
        var body = """
            {
              "stmt": "INSERT INTO doc.tbl (x) VALUES (?)",
              "bulk_args": [[1], [2], [3], [4], [5], [6]]
            }
            """;
        var response = post(body);
        assertThat(response.body()).contains("UnavailableShardsException");
        assertThat(response.body()).contains("\"rowcount\":-2");
        assertThat(response.body()).contains("\"rowcount\":1");
        assertThat(response.statusCode()).isEqualTo(200);
    }
}
