/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class PartitionInfosTest extends CrateDummyClusterServiceUnitTest {

    private static Settings defaultSettings() {
        return Settings.builder().put("index.version.created", Version.CURRENT).build();
    }

    private void addIndexMetaDataToClusterState(IndexMetaData.Builder imdBuilder) throws Exception {
        CompletableFuture<Boolean> processed = new CompletableFuture<>();
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState)
                    .metaData(MetaData.builder(currentState.metaData()).put(imdBuilder)).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                processed.completeExceptionally(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                processed.complete(true);
            }
        });
        processed.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testIgnoreNoPartitions() throws Exception {
        addIndexMetaDataToClusterState(
            IndexMetaData.builder("test1").settings(defaultSettings()).numberOfShards(10).numberOfReplicas(4));
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService);
        assertThat(partitioninfos.iterator().hasNext(), is(false));
    }

    @Test
    public void testPartitionWithoutMapping() throws Exception {
        PartitionName partitionName = new PartitionName("test1", ImmutableList.of(new BytesRef("foo")));
        addIndexMetaDataToClusterState(IndexMetaData.builder(partitionName.asIndexName())
            .settings(defaultSettings()).numberOfShards(10).numberOfReplicas(4));
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService);
        assertThat(partitioninfos.iterator().hasNext(), is(false));
    }

    @Test
    public void testPartitionWithMeta() throws Exception {
        PartitionName partitionName = new PartitionName("test1", ImmutableList.of(new BytesRef("foo")));
        IndexMetaData.Builder indexMetaData = IndexMetaData
            .builder(partitionName.asIndexName())
            .settings(defaultSettings())
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, "{\"_meta\":{\"partitioned_by\":[[\"col\", \"string\"]]}}")
            .numberOfShards(10)
            .numberOfReplicas(4);
        addIndexMetaDataToClusterState(indexMetaData);
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService);
        Iterator<PartitionInfo> iter = partitioninfos.iterator();
        PartitionInfo partitioninfo = iter.next();
        assertThat(partitioninfo.name().asIndexName(), is(partitionName.asIndexName()));
        assertThat(partitioninfo.numberOfShards(), is(10));
        assertThat(partitioninfo.numberOfReplicas().utf8ToString(), is("4"));
        assertThat(partitioninfo.values(), hasEntry("col", "foo"));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void testPartitionWithMetaMultiCol() throws Exception {
        PartitionName partitionName = new PartitionName("test1", ImmutableList.of(new BytesRef("foo"), new BytesRef("1")));
        IndexMetaData.Builder indexMetaData = IndexMetaData
            .builder(partitionName.asIndexName())
            .settings(defaultSettings())
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, "{\"_meta\":{\"partitioned_by\":[[\"col\", \"string\"], [\"col2\", \"integer\"]]}}")
            .numberOfShards(10)
            .numberOfReplicas(4);
        addIndexMetaDataToClusterState(indexMetaData);
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(clusterService);
        Iterator<PartitionInfo> iter = partitioninfos.iterator();
        PartitionInfo partitioninfo = iter.next();
        assertThat(partitioninfo.name().asIndexName(), is(partitionName.asIndexName()));
        assertThat(partitioninfo.numberOfShards(), is(10));
        assertThat(partitioninfo.numberOfReplicas().utf8ToString(), is("4"));
        assertThat(partitioninfo.values(), hasEntry("col", "foo"));
        assertThat(partitioninfo.values(), hasEntry("col2", 1));
        assertThat(iter.hasNext(), is(false));
    }
}
