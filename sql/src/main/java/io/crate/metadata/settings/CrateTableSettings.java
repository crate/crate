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
import io.crate.analyze.TableParameterInfo;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class CrateTableSettings {

    public static final BoolSetting READ_ONLY = new BoolSetting(TableParameterInfo.READ_ONLY, false);

    public static final BoolSetting BLOCKS_READ = new BoolSetting(TableParameterInfo.BLOCKS_READ, false);

    public static final BoolSetting BLOCKS_WRITE = new BoolSetting(TableParameterInfo.BLOCKS_WRITE, false);

    public static final BoolSetting BLOCKS_METADATA = new BoolSetting(TableParameterInfo.BLOCKS_METADATA, false);

    public static final IntSetting TOTAL_SHARDS_PER_NODE = new IntSetting(TableParameterInfo.TOTAL_SHARDS_PER_NODE, -1);

    public static final StringSetting ROUTING_ALLOCATION_ENABLE = new StringSetting(
        TableParameterInfo.ROUTING_ALLOCATION_ENABLE,
        ImmutableSet.of("primaries",
            "new_primaries",
            "none",
            "all"
        ),
        "all"
    );

    public static final StringSetting RECOVERY_INITIAL_SHARDS = new StringSetting(
        TableParameterInfo.RECOVERY_INITIAL_SHARDS, null, "quorum");


    public static final ByteSizeSetting FLUSH_THRESHOLD_SIZE = new ByteSizeSetting(
        TableParameterInfo.FLUSH_THRESHOLD_SIZE, new ByteSizeValue(200, ByteSizeUnit.MB));

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
