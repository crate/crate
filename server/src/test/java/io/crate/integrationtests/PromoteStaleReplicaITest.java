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
import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.elasticsearch.env.Environment.PATH_DATA_SETTING;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class PromoteStaleReplicaITest extends IntegTestCase {

    @Test
    public void test_stale_replica_can_manually_be_promoted() throws Exception {
        cluster().startMasterOnlyNode();
        Settings s1 = Settings.builder()
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .put(PATH_DATA_SETTING.getKey(), createTempDir())
            .build();
        Settings s2 = Settings.builder()
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .put(PATH_DATA_SETTING.getKey(), createTempDir())
            .build();

        String n1 = cluster().startNode(s1);
        String n2 = cluster().startNode(s2);
        execute("create table t1 (x int) " +
                "clustered into 1 shards with (number_of_replicas = 1, \"write.wait_for_active_shards\" = 1)");

        execute("insert into t1 (x) values (1)");
        ensureGreen();
        cluster().stopRandomNode(s -> Node.NODE_NAME_SETTING.get(s).equals(n1));
        execute("insert into t1 (x) values (2)");
        cluster().stopRandomNode(s -> Node.NODE_NAME_SETTING.get(s).equals(n2));
        String newN1 = cluster().startNode(s1);

        execute("select shard_id, primary, current_state from sys.allocations order by 1, 2");
        assertThat(
            printedTable(response.rows())).isEqualTo("0| false| UNASSIGNED\n" +
               "0| true| UNASSIGNED\n");

        execute("alter table t1 reroute promote replica shard 0 on ? with (accept_data_loss = true)", $(newN1));
        execute("select * from t1");
        assertThat(
            printedTable(response.rows()),
            is("1\n")
        );
    }
}
