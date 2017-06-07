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

    public static final BoolSetting READ_ONLY = new BoolSetting(TableParameterInfo.READ_ONLY, false, true);

    public static final BoolSetting BLOCKS_READ = new BoolSetting(TableParameterInfo.BLOCKS_READ, false, true);

    public static final BoolSetting BLOCKS_WRITE = new BoolSetting(TableParameterInfo.BLOCKS_WRITE, false, true);

    public static final BoolSetting BLOCKS_METADATA = new BoolSetting(TableParameterInfo.BLOCKS_METADATA, false, true);

    public static final BoolSetting FLUSH_DISABLE = new BoolSetting(TableParameterInfo.FLUSH_DISABLE, false, true);

    public static final TimeSetting TRANSLOG_INTERVAL = new TimeSetting() {
        @Override
        public String name() {
            return TableParameterInfo.TRANSLOG_INTERVAL;
        }

        @Override
        public TimeValue defaultValue() {
            return TimeValue.timeValueSeconds(5);
        }

        @Override
        public boolean isRuntime() {
            return true;
        }
    };

    public static final IntSetting TOTAL_SHARDS_PER_NODE = new IntSetting(TableParameterInfo.TOTAL_SHARDS_PER_NODE, -1, true);

    public static final StringSetting ROUTING_ALLOCATION_ENABLE = new StringSetting(
        TableParameterInfo.ROUTING_ALLOCATION_ENABLE,
        ImmutableSet.of("primaries",
            "new_primaries",
            "none",
            "all"
        ),
        true,
        "all",
        null
    );

    public static final StringSetting RECOVERY_INITIAL_SHARDS = new StringSetting(
        TableParameterInfo.RECOVERY_INITIAL_SHARDS, null, true, "quorum", null);


    public static final ByteSizeSetting FLUSH_THRESHOLD_SIZE = new ByteSizeSetting(
        TableParameterInfo.FLUSH_THRESHOLD_SIZE, new ByteSizeValue(512, ByteSizeUnit.MB), true);

    public static final IntSetting FLUSH_THRESHOLD_OPS = new IntSetting(TableParameterInfo.FLUSH_THRESHOLD_OPS, Integer.MAX_VALUE, true);

    public static final TimeSetting FLUSH_THRESHOLD_PERIOD = new TimeSetting() {

        @Override
        public String name() {
            return TableParameterInfo.FLUSH_THRESHOLD_PERIOD;
        }

        @Override
        public TimeValue defaultValue() {
            return TimeValue.timeValueMinutes(30);
        }

        @Override
        public boolean isRuntime() {
            return true;
        }
    };

    public static final BoolSetting WARMER_ENABLED = new BoolSetting(TableParameterInfo.WARMER_ENABLED, true, true);

    public static final TimeSetting TRANSLOG_SYNC_INTERVAL = new TimeSetting() {
        @Override
        public String name() {
            return TableParameterInfo.TRANSLOG_SYNC_INTERVAL;
        }

        @Override
        public TimeValue defaultValue() {
            return TimeValue.timeValueSeconds(5);
        }

        @Override
        public boolean isRuntime() {
            return false;
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

        @Override
        public boolean isRuntime() {
            return true;
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

        @Override
        public boolean isRuntime() {
            return true;
        }
    };
}
