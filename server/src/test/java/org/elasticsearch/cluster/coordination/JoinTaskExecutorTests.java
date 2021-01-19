/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Test;

public class JoinTaskExecutorTests extends ESTestCase {

    public void testPreventJoinClusterWithNewerIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);

        expectThrows(IllegalStateException.class, () ->
        JoinTaskExecutor.ensureIndexCompatibility(VersionUtils.getPreviousMinorVersion(),
            metadata));
    }

    public void testSuccess() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(VersionUtils.randomVersionBetween(random(),
                Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT)))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        indexMetadata = IndexMetadata.builder("test1")
            .settings(settings(VersionUtils.randomVersionBetween(random(),
                Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT)))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
            JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT,
                metadata);
    }

    @Test
    public void test_nodes_with_same_major_and_minor_version_can_join() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.V_4_3_3))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();

        JoinTaskExecutor.ensureIndexCompatibility(Version.V_4_3_2,
            metadata);
    }
}
