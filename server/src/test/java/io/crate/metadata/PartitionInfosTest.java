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

import io.crate.Constants;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class PartitionInfosTest extends CrateDummyClusterServiceUnitTest {

    private static Settings defaultSettings() {
        return Settings.builder().put("index.version.created", Version.CURRENT).build();
    }

    private void addIndexMetadataToClusterState(IndexMetadata.Builder imdBuilder) throws Exception {
        CompletableFuture<Boolean> processed = new CompletableFuture<>();
        ClusterState currentState = clusterService.state();
        ClusterState newState = ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata())
                .put(imdBuilder))
            .build();
        clusterService.addListener(event -> {
            if (event.state() == newState) {
                processed.complete(true);
            }
        });
        clusterService.getClusterApplierService()
            .onNewClusterState("test", () -> newState, (source, e) -> processed.completeExceptionally(e));
        processed.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testIgnoreNoPartitions() throws Exception {
        addIndexMetadataToClusterState(
            IndexMetadata.builder("test1").settings(defaultSettings()).numberOfShards(10).numberOfReplicas(4));
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService);
        assertThat(partitioninfos.iterator().hasNext(), is(false));
    }

    @Test
    public void testPartitionWithoutMapping() throws Exception {
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "test1"), List.of("foo"));
        addIndexMetadataToClusterState(IndexMetadata.builder(partitionName.asIndexName())
            .settings(defaultSettings()).numberOfShards(10).numberOfReplicas(4));
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService);
        assertThat(partitioninfos.iterator().hasNext(), is(false));
    }

    @Test
    public void testPartitionWithMeta() throws Exception {
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "test1"), List.of("foo"));
        IndexMetadata.Builder indexMetadata = IndexMetadata
            .builder(partitionName.asIndexName())
            .settings(defaultSettings())
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, "{\"_meta\":{\"partitioned_by\":[[\"col\", \"string\"]]}}")
            .numberOfShards(10)
            .numberOfReplicas(4);
        addIndexMetadataToClusterState(indexMetadata);
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService);
        Iterator<PartitionInfo> iter = partitioninfos.iterator();
        PartitionInfo partitioninfo = iter.next();
        assertThat(partitioninfo.name().asIndexName(), is(partitionName.asIndexName()));
        assertThat(partitioninfo.numberOfShards(), is(10));
        assertThat(partitioninfo.numberOfReplicas(), is("4"));
        assertThat(partitioninfo.values(), hasEntry("col", "foo"));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void testPartitionWithMetaMultiCol() throws Exception {
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "test1"), List.of("foo", "1"));
        IndexMetadata.Builder indexMetadata = IndexMetadata
            .builder(partitionName.asIndexName())
            .settings(defaultSettings())
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, "{\"_meta\":{\"partitioned_by\":[[\"col\", \"string\"], [\"col2\", \"integer\"]]}}")
            .numberOfShards(10)
            .numberOfReplicas(4);
        addIndexMetadataToClusterState(indexMetadata);
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService);
        Iterator<PartitionInfo> iter = partitioninfos.iterator();
        PartitionInfo partitioninfo = iter.next();
        assertThat(partitioninfo.name().asIndexName(), is(partitionName.asIndexName()));
        assertThat(partitioninfo.numberOfShards(), is(10));
        assertThat(partitioninfo.numberOfReplicas(), is("4"));
        assertThat(partitioninfo.values(), hasEntry("col", "foo"));
        assertThat(partitioninfo.values(), hasEntry("col2", 1));
        assertThat(iter.hasNext(), is(false));
    }
}
