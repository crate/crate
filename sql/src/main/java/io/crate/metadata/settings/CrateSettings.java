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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

public class CrateSettings {

    public static final NestedSetting STATS = new NestedSetting() {
        @Override
        public String name() {
            return "stats";
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(STATS_ENABLED, STATS_JOBS_LOG_SIZE, STATS_OPERATIONS_LOG_SIZE);
        }
    };

    public static final BoolSetting STATS_ENABLED = new BoolSetting() {
        @Override
        public String name() {
            return "enabled";
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }

        @Override
        public Setting parent() {
            return STATS;
        }
    };

    public static final IntSetting STATS_JOBS_LOG_SIZE = new IntSetting() {
        @Override
        public String name() {
            return "jobs_log_size";
        }

        @Override
        public Integer defaultValue() {
            return 10_000;
        }

        @Override
        public Integer minValue() {
            return 0;
        }

        @Override
        public Setting parent() {
            return STATS;
        }
    };

    public static final IntSetting STATS_OPERATIONS_LOG_SIZE = new IntSetting() {
        @Override
        public String name() {
            return "operations_log_size";
        }

        @Override
        public Integer defaultValue() {
            return 10_000;
        }

        @Override
        public Integer minValue() {
            return 0;
        }

        @Override
        public Setting parent() {
            return STATS;
        }
    };

    public static final NestedSetting CLUSTER = new NestedSetting() {
        @Override
        public String name() {
            return "cluster";
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(GRACEFUL_STOP, ROUTING);
        }
    };

    public static final NestedSetting GRACEFUL_STOP = new NestedSetting() {

        @Override
        public String name() { return "graceful_stop"; }

        @Override
        public Setting parent() {
            return CLUSTER;
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    GRACEFUL_STOP_MIN_AVAILABILITY,
                    GRACEFUL_STOP_REALLOCATE,
                    GRACEFUL_STOP_TIMEOUT,
                    GRACEFUL_STOP_FORCE);
        }
    };

    public static final StringSetting GRACEFUL_STOP_MIN_AVAILABILITY = new StringSetting(
            Sets.newHashSet("full", "primaries", "none")
    ) {
        @Override
        public String name() { return "min_availability"; }

        @Override
        public String defaultValue() { return "primaries"; }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }
    };

    public static final BoolSetting GRACEFUL_STOP_REALLOCATE = new BoolSetting() {
        @Override
        public String name() { return "reallocate"; }

        @Override
        public Boolean defaultValue() {
            return true;
        }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }
    };

    public static final TimeSetting GRACEFUL_STOP_TIMEOUT = new TimeSetting() {
        @Override
        public String name() {
            return "timeout";
        }

        @Override
        public TimeValue defaultValue() {
            return new TimeValue(7_200_000);
        }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }
    };

    public static final BoolSetting GRACEFUL_STOP_FORCE = new BoolSetting() {
        @Override
        public String name() {
            return "force";
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }
    };

    public static final NestedSetting ROUTING = new NestedSetting() {
        @Override
        public String name() { return "routing"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    ROUTING_ALLOCATION);
        }

        @Override
        public Setting parent() {
            return CLUSTER;
        }
    };

    public static final NestedSetting ROUTING_ALLOCATION = new NestedSetting() {
        @Override
        public String name() { return "allocation"; }

        @Override
        public Setting parent() {
            return ROUTING;
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    ROUTING_ALLOCATION_ENABLE);
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_ENABLE = new StringSetting(
            Sets.newHashSet("all", "new_primaries")
    ) {
        @Override
        public String name() { return "enable"; }

        @Override
        public String defaultValue() { return "all"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final ImmutableList<Setting> CRATE_SETTINGS = ImmutableList.<Setting>of(STATS, CLUSTER);

}
