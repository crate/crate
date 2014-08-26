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
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.unit.TimeValue;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class CrateSettings {

    public static final IntSetting JOBS_LOG_SIZE = new IntSetting() {
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
    };

    public static final IntSetting OPERATIONS_LOG_SIZE = new IntSetting() {
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
    };

    public static final BoolSetting COLLECT_STATS = new BoolSetting() {
        @Override
        public String name() {
            return "collect_stats";
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }
    };

    public static final NestedSetting GRACEFUL_STOP = new NestedSetting() {
        @Override
        public String name() { return "graceful_stop"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    GRACEFUL_STOP_MIN_AVAILABILITY,
                    GRACEFUL_STOP_REALLOCATE,
                    GRACEFUL_STOP_TIMEOUT,
                    GRACEFUL_STOP_FORCE,
                    GRACEFUL_STOP_IS_DEFAULT);
        }
    };

    public static final StringSetting GRACEFUL_STOP_MIN_AVAILABILITY = new StringSetting() {
        final Set<String> allowedValues = Sets.newHashSet("full", "primaries", "none");

        @Override
        public String name() { return "min_availability"; }

        @Override
        public String defaultValue() { return "primaries"; }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }

        public Validator validator() {
            return new Validator() {
                @Override
                public String validate(String setting, String value) {
                    if (!allowedValues.contains(value)) {
                        return String.format("'%s' is not an allowed value.", value);
                    }
                    return null;
                }
            };
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

    public static final BoolSetting GRACEFUL_STOP_IS_DEFAULT = new BoolSetting() {
        @Override
        public String name() {
            return "is_default";
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

    public static final ImmutableList<Setting> CLUSTER_SETTINGS = ImmutableList.<Setting>of(
            JOBS_LOG_SIZE, OPERATIONS_LOG_SIZE, COLLECT_STATS, GRACEFUL_STOP);

}
