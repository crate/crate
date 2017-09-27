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

package io.crate.metadata.settings;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.crate.analyze.TableParameterInfo;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.translog.Translog;

import static org.elasticsearch.index.IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING;

public final class CrateTableSettings {

    private CrateTableSettings() {
    }

    public static final BoolSetting READ_ONLY = new BoolSetting(TableParameterInfo.READ_ONLY, false);

    public static final BoolSetting BLOCKS_READ = new BoolSetting(TableParameterInfo.BLOCKS_READ, false);

    public static final BoolSetting BLOCKS_WRITE = new BoolSetting(TableParameterInfo.BLOCKS_WRITE, false);

    public static final BoolSetting BLOCKS_METADATA = new BoolSetting(TableParameterInfo.BLOCKS_METADATA, false);

    public static final IntSetting TOTAL_SHARDS_PER_NODE = new IntSetting(TableParameterInfo.TOTAL_SHARDS_PER_NODE, -1);

    public static final IntSetting TOTAL_FIELDS_LIMIT = new IntSetting(TableParameterInfo.MAPPING_TOTAL_FIELDS_LIMIT, 1000);

    public static final StringSetting ROUTING_ALLOCATION_ENABLE = new StringSetting(
        TableParameterInfo.ROUTING_ALLOCATION_ENABLE,
        ImmutableSet.of("primaries",
            "new_primaries",
            "none",
            "all"
        ),
        "all"
    );

    public static final StringSetting SETTING_WAIT_FOR_ACTIVE_SHARDS = new StringSetting(
        TableParameterInfo.SETTING_WAIT_FOR_ACTIVE_SHARDS,
        null,
        IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS.getDefaultRaw(Settings.EMPTY));

    public static final StringSetting RECOVERY_INITIAL_SHARDS = new StringSetting(
        TableParameterInfo.RECOVERY_INITIAL_SHARDS, null, "quorum");


    public static final ByteSizeSetting FLUSH_THRESHOLD_SIZE = new ByteSizeSetting(
        TableParameterInfo.FLUSH_THRESHOLD_SIZE, INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getDefault(Settings.EMPTY));

    public static final BoolSetting WARMER_ENABLED = new BoolSetting(TableParameterInfo.WARMER_ENABLED, true);

    public static final TimeSetting TRANSLOG_SYNC_INTERVAL = new TimeSetting() {
        @Override
        public String name() {
            return TableParameterInfo.TRANSLOG_SYNC_INTERVAL;
        }

        @Override
        public TimeValue defaultValue() {
            return TimeValue.timeValueSeconds(5);
        }
    };

    public static final StringSetting TRANSLOG_DURABILITY = new StringSetting(TableParameterInfo.TRANSLOG_DURABILITY,
        Sets.newHashSet(Translog.Durability.REQUEST.name(), Translog.Durability.ASYNC.name()),
        Translog.Durability.REQUEST.name());

    public static final TimeSetting REFRESH_INTERVAL = new TimeSetting() {
        @Override
        public String name() {
            return TableParameterInfo.REFRESH_INTERVAL;
        }

        @Override
        public TimeValue defaultValue() {
            return TimeValue.timeValueMillis(1000);
        }
    };

    public static final TimeSetting UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = new TimeSetting() {
        @Override
        public String name() {
            return TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT;
        }

        @Override
        public TimeValue defaultValue() {
            return TimeValue.timeValueMinutes(1);
        }
    };
}
