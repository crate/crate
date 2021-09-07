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

import io.crate.common.unit.TimeValue;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

@Singleton
public class LogicalReplicationSettings {

    public static final Setting<Integer> REPLICATION_CHANGE_BATCH_SIZE = Setting.intSetting(
        "replication.logical.ops_batch_size", 50000, 16,
        Setting.Property.Dynamic, Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> REPLICATION_READ_POLL_DURATION = Setting.timeSetting(
        "replication.logical.reads_poll_duration",
        TimeValue.timeValueMillis(50),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Internal index setting marking an index as subscribed/replicated
     */
    public static final Setting<String> REPLICATION_SUBSCRIBED_INDEX = Setting.simpleString(
        "index.replication.logical.subscribed",
        Setting.Property.InternalIndex,
        Setting.Property.IndexScope
    );


    private long batchSize;
    private TimeValue pollDelay;

    @Inject
    public LogicalReplicationSettings(Settings settings, ClusterService clusterService) {
        batchSize = REPLICATION_CHANGE_BATCH_SIZE.get(settings);
        pollDelay = REPLICATION_READ_POLL_DURATION.get(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(REPLICATION_CHANGE_BATCH_SIZE, this::batchSize);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REPLICATION_READ_POLL_DURATION, this::pollDelay);
    }

    public long batchSize() {
        return batchSize;
    }

    public void batchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public TimeValue pollDelay() {
        return pollDelay;
    }

    public void pollDelay(TimeValue pollDelay) {
        this.pollDelay = pollDelay;
    }
}
