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

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class PartitionInfosTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setup() {
        e = SQLExecutor.builder(clusterService)
            .build();
    }

    @Test
    public void testIgnoreNoPartitions() throws Exception {
        e.addTable("create table tbl (x int)");
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService, e.schemas());
        assertThat(partitioninfos.iterator().hasNext()).isFalse();
    }


    @Test
    public void testPartitionWithMeta() throws Exception {
        PartitionName partitionName = new PartitionName(new RelationName("doc", "tbl"), List.of("1"));
        e.addPartitionedTable(
            """
                create table doc.tbl (
                    x int,
                    p int
                )
                clustered into 10 shards
                partitioned by (p)
                with (number_of_replicas = 4)
            """,
            partitionName.asIndexName()
        );
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService, e.schemas());
        Iterator<PartitionInfo> iter = partitioninfos.iterator();
        PartitionInfo partitioninfo = iter.next();
        assertThat(partitioninfo.name()).isEqualTo(partitionName);
        assertThat(partitioninfo.numberOfShards()).isEqualTo(10);
        assertThat(partitioninfo.numberOfReplicas()).isEqualTo("4");
        assertThat(partitioninfo.values()).containsOnly(Map.entry("p", 1));
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void testPartitionWithMetaMultiCol() throws Exception {
        PartitionName partitionName = new PartitionName(new RelationName("doc", "tbl"), List.of("foo", "2"));
        e.addPartitionedTable(
            """
                create table doc.tbl (
                    x int,
                    p1 text,
                    p2 int
                )
                clustered into 10 shards
                partitioned by (p1, p2)
                with (number_of_replicas = 4)
            """,
            partitionName.asIndexName()
        );
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService, e.schemas());
        Iterator<PartitionInfo> iter = partitioninfos.iterator();
        PartitionInfo partitioninfo = iter.next();
        assertThat(partitioninfo.name().asIndexName()).isEqualTo(partitionName.asIndexName());
        assertThat(partitioninfo.numberOfShards()).isEqualTo(10);
        assertThat(partitioninfo.numberOfReplicas()).isEqualTo("4");
        assertThat(partitioninfo.values()).containsExactlyInAnyOrderEntriesOf(Map.of(
            "p1", "foo",
            "p2", 2));
        assertThat(iter.hasNext()).isFalse();
    }
}
