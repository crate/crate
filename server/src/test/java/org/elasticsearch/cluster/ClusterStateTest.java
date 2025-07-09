/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.cluster;

import static io.crate.testing.DiscoveryNodes.newNode;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class ClusterStateTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_bwc_reading_routing_table_from_5_nodes() throws Exception {
        RelationName relationName = new RelationName("my_schema", "my_table");
        String indexUUID = UUIDs.randomBase64UUID();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexUUID)
            .indexName(relationName.indexNameOrAlias())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                    .put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_10_0)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // We must manually register the IndexRoutingTable by the index name, as the builder will always
        // register it by the index UUID.
        IndexRoutingTable indexRouting = new IndexRoutingTable.Builder(indexMetadata.getIndex())
            .initializeAsNew(indexMetadata).build();
        Field indicesRoutingField
            = RoutingTable.Builder.class.getDeclaredField("indicesRouting");
        indicesRoutingField.setAccessible(true);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        //noinspection unchecked
        ImmutableOpenMap.Builder<String, IndexRoutingTable> indicesRouting =
            (ImmutableOpenMap.Builder<String, IndexRoutingTable>) indicesRoutingField.get(routingTableBuilder);
        indicesRouting.put(indexMetadata.getIndex().getName(), indexRouting);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder()
                .put(indexMetadata, false)
                .build())
            .routingTable(routingTableBuilder.build())
            .build();

        // IndexRouting is registered by the index name.
        assertThat(clusterState.routingTable().hasIndex(indexMetadata.getIndex().getName())).isTrue();
        assertThat(clusterState.routingTable().hasIndex(indexMetadata.getIndexUUID())).isFalse();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_5_10_0);
        clusterState.writeTo(out);
        var in = out.bytes().streamInput();
        in.setVersion(Version.V_5_10_0);

        ClusterState recievedClusterState = ClusterState.readFrom(in, newNode("node1", "node1"));

        // IndexRouting is registered by the index UUID now.
        assertThat(recievedClusterState.routingTable().hasIndex(indexMetadata.getIndex().getName())).isFalse();
        assertThat(recievedClusterState.routingTable().hasIndex(indexMetadata.getIndexUUID())).isTrue();
    }
}
