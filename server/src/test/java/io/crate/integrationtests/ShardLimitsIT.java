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
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.junit.After;
import org.junit.Test;

import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.Asserts;
import io.netty.handler.codec.http.HttpResponseStatus;

@ClusterScope(numDataNodes = 10, supportsDedicatedMasters = false, numClientNodes = 0)
public class ShardLimitsIT extends IntegTestCase {

    @After
    public void reset_settings() {
        execute("reset global \"cluster.max_shards_per_node\"");
    }

//    @Test
//    public void test_shard_limit_is_checked_on_create_table() throws Exception {
//        execute("set global \"cluster.max_shards_per_node\" = 1");
//        Asserts.assertSQLError(() -> execute(
//                        "create table tbl (x int) clustered into 4 shards with (number_of_replicas = 0)"))
//                .hasPGError(PGErrorStatus.INTERNAL_ERROR)
//                .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
//                .hasMessageContaining(
//                        "this action would add [4] total shards, but this cluster currently has [0]/[2] maximum shards open;");
//    }
//
//
//    @Test
//    public void test_shard_limit_is_checked_on_partition_creation() throws Exception {
//        execute("""
//                    create table tbl (x int, p int)
//                    clustered into 4 shards
//                    partitioned by (p)
//                    with (number_of_replicas = 0)
//                    """);
//        execute("set global \"cluster.max_shards_per_node\" = 1");
//        Asserts.assertSQLError(() -> execute("insert into tbl (x, p) values (1, 1)"))
//                .hasPGError(PGErrorStatus.INTERNAL_ERROR)
//                .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
//                .hasMessageContaining(
//                        "this action would add [4] total shards, but this cluster currently has [0]/[2] maximum shards open;");
//    }
//
//    @Test
//    public void test_shard_limit_is_checked_on_alter_table() throws Exception {
//        execute("set global \"cluster.max_shards_per_node\" = 1");
//        execute("create table tbl (x int) clustered into 2 shards with (number_of_replicas = 0)");
//        Asserts.assertSQLError(() -> execute("alter table tbl set (number_of_replicas = 1)"))
//                .hasPGError(PGErrorStatus.INTERNAL_ERROR)
//                .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
//                .hasMessageContaining(
//                        "this action would add [2] total shards, but this cluster currently has [2]/[2] maximum shards open;");
//    }

    @Test
    public void test_auto_expanding_replicas() throws Exception {
        execute("set global \"cluster.max_shards_per_node\" = 1");

        // 10 data nodes, 9 replicas.
        execute("create table t (a int) clustered into 10 shards with (number_of_replicas = '0-all')");
        ensureGreen();

        execute("select node['name'], closed, count(*) from sys.shards group by node['name'], closed order by node['name']");
        assertThat(response).hasRows(
            "node_s0| false| 10",
            "node_s1| false| 10",
            "node_s2| false| 10",
            "node_s3| false| 10",
            "node_s4| false| 10",
            "node_s5| false| 10",
            "node_s6| false| 10",
            "node_s7| false| 10",
            "node_s8| false| 10",
            "node_s9| false| 10"
        );
    }
}
