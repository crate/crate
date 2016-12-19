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
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class PartitionInfosTest extends CrateUnitTest {

    private ClusterService mockService(Map<String, IndexMetaData> indices) {
        ImmutableOpenMap<String, IndexMetaData> indicesMap =
            ImmutableOpenMap.<String, IndexMetaData>builder().putAll(indices).build();
        return new NoopClusterService(
            ClusterState.builder(ClusterName.DEFAULT).metaData(
                MetaData.builder().indices(indicesMap).build()).build());
    }

    private static Settings defaultSettings() {
        return Settings.builder().put("index.version.created", Version.CURRENT).build();
    }

    @Test
    public void testIgnoreNoPartitions() throws Exception {
        Map<String, IndexMetaData> indices = new HashMap<>();
        indices.put("test1", IndexMetaData.builder("test1").settings(defaultSettings()).numberOfShards(10).numberOfReplicas(4).build());
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(mockService(indices));
        assertThat(partitioninfos.iterator().hasNext(), is(false));
    }

    @Test
    public void testPartitionWithoutMapping() throws Exception {
        Map<String, IndexMetaData> indices = new HashMap<>();
        PartitionName partitionName = new PartitionName("test1", ImmutableList.of(new BytesRef("foo")));
        indices.put(partitionName.asIndexName(), IndexMetaData.builder(partitionName.asIndexName())
            .settings(defaultSettings()).numberOfShards(10).numberOfReplicas(4).build());
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(mockService(indices));
        assertThat(partitioninfos.iterator().hasNext(), is(false));
    }

    @Test
    public void testPartitionWithMeta() throws Exception {
        Map<String, IndexMetaData> indices = new HashMap<>();
        PartitionName partitionName = new PartitionName("test1", ImmutableList.of(new BytesRef("foo")));
        IndexMetaData indexMetaData = IndexMetaData
            .builder(partitionName.asIndexName())
            .settings(defaultSettings())
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, "{\"_meta\":{\"partitioned_by\":[[\"col\", \"string\"]]}}")
            .numberOfShards(10)
            .numberOfReplicas(4).build();
        indices.put(partitionName.asIndexName(), indexMetaData);
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(mockService(indices));
        Iterator<PartitionInfo> iter = partitioninfos.iterator();
        PartitionInfo partitioninfo = iter.next();
        assertThat(partitioninfo.name().asIndexName(), is(partitionName.asIndexName()));
        assertThat(partitioninfo.numberOfShards(), is(10));
        assertThat(partitioninfo.numberOfReplicas().utf8ToString(), is("4"));
        assertThat(partitioninfo.values(), hasEntry("col", (Object) "foo"));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void testPartitionWithMetaMultiCol() throws Exception {
        Map<String, IndexMetaData> indices = new HashMap<>();
        PartitionName partitionName = new PartitionName("test1", ImmutableList.of(new BytesRef("foo"), new BytesRef("1")));
        IndexMetaData indexMetaData = IndexMetaData
            .builder(partitionName.asIndexName())
            .settings(defaultSettings())
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, "{\"_meta\":{\"partitioned_by\":[[\"col\", \"string\"], [\"col2\", \"integer\"]]}}")
            .numberOfShards(10)
            .numberOfReplicas(4).build();
        indices.put(partitionName.asIndexName(), indexMetaData);
        Iterable<PartitionInfo> partitioninfos = new PartitionInfos(mockService(indices));
        Iterator<PartitionInfo> iter = partitioninfos.iterator();
        PartitionInfo partitioninfo = iter.next();
        assertThat(partitioninfo.name().asIndexName(), is(partitionName.asIndexName()));
        assertThat(partitioninfo.numberOfShards(), is(10));
        assertThat(partitioninfo.numberOfReplicas().utf8ToString(), is("4"));
        assertThat(partitioninfo.values(), hasEntry("col", (Object) "foo"));
        assertThat(partitioninfo.values(), hasEntry("col2", (Object) 1));
        assertThat(iter.hasNext(), is(false));
    }
}
