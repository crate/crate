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

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.execution.ddl.tables.AlterTableOperation;

@IntegTestCase.ClusterScope(numDataNodes = 1)
public class DanglingIndicesIntegrationTest extends IntegTestCase {

    @After
    public void cleanupDangling() {
        execute("alter cluster gc dangling artifacts");
    }

    @Test
    public void testDanglingIndicesAreFilteredOutFromDBCatalog() throws Exception {
        execute("create table doc.t1 (id int) clustered into 3 shards with (number_of_replicas=0)");
        execute("create table doc.t2 (id int) partitioned by(id) clustered into 3 shards with (number_of_replicas=0)");
        execute("insert into doc.t2 values (1), (2)");

        execute("refresh table doc.t2");
        execute("create blob table blobs clustered into 3 shards with (number_of_replicas=0)");

        final String dangling1 = AlterTableOperation.RESIZE_PREFIX + "t1";
        final String dangling2 = AlterTableOperation.RESIZE_PREFIX + ".partitioned.t2.ident";

        createIndex(dangling1, dangling2);

        ClusterService clusterService = cluster().getInstance(ClusterService.class);
        assertThat(clusterService.state().metadata().hasIndex(dangling1)).isTrue();
        assertThat(clusterService.state().metadata().hasIndex(dangling2)).isTrue();

        execute("select id, table_name, state from sys.shards where table_name = 't1'");
        assertThat(response).hasRowCount(3L);

        execute("select * from sys.health where table_name = 't1'");
        assertThat(response).hasRowCount(1L);

        execute("select * from sys.allocations where table_name = 't1'");
        assertThat(response).hasRowCount(3L);

        execute("select * from information_schema.tables where table_name = 't1'");
        assertThat(response).hasRowCount(1L);

        execute("select * from information_schema.tables where table_schema = 'blob'");
        assertThat(response).hasRowCount(1L);

        execute("select table_schema, table_name, values from information_schema.table_partitions " +
                "where table_name = 't2'");
        assertThat(response).hasRowCount(2L);
    }
}
