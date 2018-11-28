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
import static org.hamcrest.core.Is.is;

public class DDLShardResizeIntegrationTest extends SQLTransportIntegrationTest {

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

        final String targetIndexName = ".shrinked." + getFqn("quotes");
        final String backupIndexName = ".backup." + getFqn("quotes");
        logger.info("targetIndexName:" + targetIndexName );

        createIndex(targetIndexName, backupIndexName);

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        assertThat(clusterService.state().metaData().hasIndex(targetIndexName), is(true));
        assertThat(clusterService.state().metaData().hasIndex(backupIndexName), is(true));

        execute("alter table quotes set (\"blocks.write\"=?)", $(true));
        execute("alter table quotes set (number_of_shards=?)", $(1));

        execute("select number_of_shards from information_schema.tables where table_name = 'quotes'");
        assertThat(response.rows()[0][0], is(1));

        assertThat(clusterService.state().metaData().hasIndex(targetIndexName), is(false));
        assertThat(clusterService.state().metaData().hasIndex(backupIndexName), is(false));
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
}
