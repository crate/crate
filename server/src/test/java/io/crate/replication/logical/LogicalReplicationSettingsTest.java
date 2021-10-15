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

import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Test;

import java.util.Collection;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_CHANGE_BATCH_SIZE;
import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_READ_POLL_DURATION;
import static org.hamcrest.Matchers.is;

public class LogicalReplicationSettingsTest extends CrateDummyClusterServiceUnitTest {

    @Override
    protected Collection<Setting<?>> additionalClusterSettings() {
        return new SQLPlugin(Settings.EMPTY).getSettings();
    }

    @Test
    public void testSettingsChanged() {
        var replicationSettings = new LogicalReplicationSettings(Settings.EMPTY, clusterService);

        ClusterState newState = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder().transientSettings(
                Settings.builder()
                    .put(REPLICATION_CHANGE_BATCH_SIZE.getKey(), 20)
                    .put(REPLICATION_READ_POLL_DURATION.getKey(), "1s")
                    .build()
            ))
            .build();
        ClusterServiceUtils.setState(clusterService, newState);
        assertThat(replicationSettings.batchSize(), is(20L));
        assertThat(replicationSettings.pollDelay().millis(), is(1000L));
    }
}
