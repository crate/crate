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

package io.crate.replication.logical;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_CHANGE_BATCH_SIZE;
import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_READ_POLL_DURATION;
import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_RECOVERY_CHUNK_SIZE;
import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS;
import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class LogicalReplicationSettingsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSettingsChanged() {
        var replicationSettings = new LogicalReplicationSettings(Settings.EMPTY, clusterService);

        ClusterState newState = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder().transientSettings(
                Settings.builder()
                    .put(REPLICATION_CHANGE_BATCH_SIZE.getKey(), 20)
                    .put(REPLICATION_READ_POLL_DURATION.getKey(), "1s")
                    .put(REPLICATION_RECOVERY_CHUNK_SIZE.getKey(), "10MB")
                    .put(REPLICATION_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS.getKey(), 3)
                    .build()
            ))
            .build();
        ClusterServiceUtils.setState(clusterService, newState);
        assertThat(replicationSettings.batchSize()).isEqualTo(20);
        assertThat(replicationSettings.pollDelay().millis()).isEqualTo(1000L);
        assertThat(replicationSettings.recoveryChunkSize()).isEqualTo(new ByteSizeValue(10, ByteSizeUnit.MB));
        assertThat(replicationSettings.maxConcurrentFileChunks()).isEqualTo(3);
    }
}
