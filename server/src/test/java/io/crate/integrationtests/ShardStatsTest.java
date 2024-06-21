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

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class ShardStatsTest extends IntegTestCase {

    @Test
    public void testSelectZeroLimit() throws Exception {
        execute("create table test (col1 int) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("select * from sys.shards limit 0");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testShardSelect() throws Exception {
        execute("create table test (col1 int) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();

        execute("select count(*) from sys.shards where table_name='test'");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(3L);
    }

    @Test
    public void testSelectIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) " +
                "clustered into 2 shards " +
                "with (number_of_replicas=2, \"write.wait_for_active_shards\"=1)");

        assertBusy(() -> {
            execute("select state, \"primary\", recovery['stage'] from sys.shards where table_name = 'locations' order by state, \"primary\"");
            assertThat(response).hasRows(
                "STARTED| false| DONE",
                "STARTED| false| DONE",
                "STARTED| true| DONE",
                "STARTED| true| DONE",
                "UNASSIGNED| false| NULL",
                "UNASSIGNED| false| NULL");
        });
    }

    @Test
    public void testSelectGroupByIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) " +
                "clustered into 5 shards " +
                "with(number_of_replicas=2 , \"write.wait_for_active_shards\"=1)");
        ensureYellow();

        execute("select count(*), state, \"primary\" from sys.shards " +
                "group by state, \"primary\" order by state desc");
        assertThat(response.rowCount()).isGreaterThanOrEqualTo(2L);
        assertThat(response.cols().length).isEqualTo(3);
        assertThat((Long) response.rows()[0][0]).isLessThanOrEqualTo(5L);
        assertThat(response.rows()[0][1]).isEqualTo("UNASSIGNED");
        assertThat(response.rows()[0][2]).isEqualTo(false);
    }

    @Test
    public void testSelectCountIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) " +
                "clustered into 5 shards " +
                "with(number_of_replicas=2, \"write.wait_for_active_shards\"=1)");
        ensureYellow();

        execute("select count(*) from sys.shards where table_name='locations'");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(15L);
    }

    @Test
    public void testTableNameBlobTable() throws Exception {
        execute("create blob table blobs clustered into 2 shards with (number_of_replicas=0)");
        ensureGreen();

        execute("select schema_name, table_name from sys.shards where table_name = 'blobs'");
        assertThat(response.rowCount()).isEqualTo(2L);
        for (int i = 0; i < response.rowCount(); i++) {
            assertThat((String) response.rows()[i][0]).isEqualTo("blob");
            assertThat((String) response.rows()[i][1]).isEqualTo("blobs");
        }

        execute("create blob table sbolb clustered into 4 shards " +
            "with (number_of_replicas=0)");
        ensureYellow();

        execute("select schema_name, table_name from sys.shards where table_name = 'sbolb'");
        assertThat(response.rowCount()).isEqualTo(4L);
        for (int i = 0; i < response.rowCount(); i++) {
            assertThat((String) response.rows()[i][0]).isEqualTo("blob");
            assertThat((String) response.rows()[i][1]).isEqualTo("sbolb");
        }
        execute("select count(*) from sys.shards " +
                "where schema_name='blob' and table_name != 'blobs' " +
                "and table_name != 'sbolb'");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(0L);
    }
}
