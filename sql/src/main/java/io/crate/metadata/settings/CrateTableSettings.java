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

    public static final BoolSetting READ_ONLY = new BoolSetting() {

        @Override
        public String name() {
            return TableParameterInfo.READ_ONLY;
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }
    };

    public static final BoolSetting BLOCKS_READ = new BoolSetting() {
        @Override
        public String name() {
            return TableParameterInfo.BLOCKS_READ;
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }
    };

    public static final BoolSetting BLOCKS_WRITE = new BoolSetting() {
        @Override
        public String name() {
            return TableParameterInfo.BLOCKS_WRITE;
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }
    };

    public static final BoolSetting BLOCKS_METADATA = new BoolSetting() {
        @Override
        public String name() {
            return TableParameterInfo.BLOCKS_METADATA;
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }
    };

    public static final BoolSetting FLUSH_DISABLE = new BoolSetting() {
        @Override
        public String name() {
            return TableParameterInfo.FLUSH_DISABLE;
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }
    };

    public static final TimeSetting TRANSLOG_INTERVAL = new TimeSetting() {
        @Override
        public String name() {
            return TableParameterInfo.TRANSLOG_INTERVAL;
        }

        @Override
        public TimeValue defaultValue() {
            return TimeValue.timeValueSeconds(5);
        }
    };

    public static final IntSetting TOTAL_SHARDS_PER_NODE = new IntSetting() {
        @Override
        public String name() {
            return TableParameterInfo.TOTAL_SHARDS_PER_NODE;
        }

        @Override
        public Integer defaultValue() {
            return -1;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_ENABLE = new StringSetting(ImmutableSet.of(
            "primaries",
            "new_primaries",
            "none",
            "all"
    )) {

        @Override
        public String name() {
            return TableParameterInfo.ROUTING_ALLOCATION_ENABLE;
        }

        @Override
        public String defaultValue() {
            return "all";
        }


    };

    public static final StringSetting RECOVERY_INITIAL_SHARDS = new StringSetting() {

        @Override
        public String name() {
            return TableParameterInfo.RECOVERY_INITIAL_SHARDS;
        }

        @Override
        public String defaultValue() {
            return "quorum";
        }
    };


    public static final ByteSizeSetting FLUSH_THRESHOLD_SIZE = new ByteSizeSetting() {
        @Override
        public String name() {
            return TableParameterInfo.FLUSH_THRESHOLD_SIZE;
        }

        @Override
        public ByteSizeValue defaultValue() {
            return new ByteSizeValue(200, ByteSizeUnit.MB);
        }
    };


    public static final IntSetting FLUSH_THRESHOLD_OPS = new IntSetting() {
        @Override
        public String name() {
            return TableParameterInfo.FLUSH_THRESHOLD_OPS;
        }

        @Override
        public Integer defaultValue() {
            return Integer.MAX_VALUE;
        }
    };

    public static final TimeSetting FLUSH_THRESHOLD_PERIOD = new TimeSetting() {

        @Override
        public String name() {
            return TableParameterInfo.FLUSH_THRESHOLD_PERIOD;
        }

        @Override
        public TimeValue defaultValue() {
            return TimeValue.timeValueMinutes(30);
        }

    };

    public static final BoolSetting WARMER_ENABLED = new BoolSetting() {
        @Override
        public String name() {
            return TableParameterInfo.WARMER_ENABLED;
        }

        @Override
        public Boolean defaultValue() {
            return true;
        }
    };

}
