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

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.UseRandomizedSchema;

// ensure that only data nodes are picked for rerouting
@IntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
@UseRandomizedSchema(random = false)
public class AlterTableRerouteIntegrationTest extends IntegTestCase {

    @Test
    public void testAlterTableRerouteMoveShard() throws Exception {
        int shardId = 0;
        String tableName = "my_table";
        execute("create table " + tableName + " (" +
            "id int primary key," +
            "date timestamp with time zone" +
            ") clustered into 1 shards " +
            "with (number_of_replicas=0)");
        ensureGreen();
        execute("select node['id'] from sys.shards where id = ? and table_name = ?", new Object[]{shardId, tableName});
        String fromNode = (String) response.rows()[0][0];
        execute("select id from sys.nodes where id != ?", new Object[]{fromNode});
        String toNode = (String) response.rows()[0][0];

        execute("ALTER TABLE my_table REROUTE MOVE SHARD ? FROM ? TO ?", new Object[]{shardId, fromNode, toNode});
        assertThat(response).hasRowCount(1);
        ensureGreen();
        execute("select * from sys.shards where id = ? and node['id'] = ? and table_name = ?", new Object[]{shardId, toNode, tableName});
        assertThat(response).hasRowCount(1);
        execute("select * from sys.shards where id = ? and node['id'] = ? and table_name = ?", new Object[]{shardId, fromNode, tableName});
        assertThat(response).hasRowCount(0);
    }
}
