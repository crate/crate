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

import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.core.Is.is;

public class ResizeShardsITest extends SQLTransportIntegrationTest {

    @Test
    public void testShrinkShardsOfTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "clustered into 3 shards");
        ensureYellow();

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );

        execute("refresh table quotes");

        execute("alter table quotes set (\"blocks.write\"=?)", $(true));
        execute("alter table quotes set (number_of_shards=?)", $(1));

        execute("select number_of_shards from information_schema.tables where table_name = 'quotes'");
        assertThat(response.rows()[0][0], is(1));
        execute("select id from quotes");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testShrinkShardsEnsureLeftOverIndicesAreRemoved() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "clustered into 3 shards");
        ensureYellow();

        final String resizeIndex = ".resized." + getFqn("quotes");
        createIndex(resizeIndex);

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        assertThat(clusterService.state().metaData().hasIndex(resizeIndex), is(true));

        execute("alter table quotes set (\"blocks.write\"=?)", $(true));
        execute("alter table quotes set (number_of_shards=?)", $(1));

        execute("select number_of_shards from information_schema.tables where table_name = 'quotes'");
        assertThat(response.rows()[0][0], is(1));

        assertThat(clusterService.state().metaData().hasIndex(resizeIndex), is(false));
    }

    @Test
    public void testShrinkShardsOfPartition() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards");
        ensureYellow();

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );

        execute("refresh table quotes");

        execute("alter table quotes partition (date=1395874800000) set (\"blocks.write\"=?)",
            $(true));
        execute("alter table quotes partition (date=1395874800000) set (number_of_shards=?)",
            $(1));

        execute("select number_of_shards from information_schema.table_partitions " +
                "where table_name = 'quotes' " +
                "and values = '{\"date\": 1395874800000}'");
        assertThat(response.rows()[0][0], is(1));
        execute("select id from quotes");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testNumberOfShardsOfATableCanBeIncreased() {
        execute("create table t1 (x int) clustered into 1 shards with (number_of_routing_shards = 10, \"blocks.write\" = true)");
        execute("alter table t1 set (number_of_shards = 2)");

        execute("select number_of_shards from information_schema.tables where table_name = 't1'");
        assertThat(printedTable(response.rows()), is("2\n"));
    }

    @Test
    public void testNumberOfShardsOfAPartitionCanBeIncreased() {
        execute("create table t1 (x int, p int) partitioned by (p) clustered into 1 shards " +
                "with (number_of_routing_shards = 10)");
        execute("insert into t1 (x, p) values (1, 1), (2, 1)");
        execute("alter table t1 partition (p = 1) set (\"blocks.write\" = true)");

        execute("alter table t1 partition (p = 1) set (number_of_shards = 2)");
        execute("select number_of_shards from information_schema.table_partitions where table_name = 't1'");
        assertThat(printedTable(response.rows()), is("2\n"));
    }
}
