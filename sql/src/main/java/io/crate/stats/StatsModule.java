/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.stats;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

public class StatsModule extends AbstractModule {

    public final static String EXTENDED_STATS_TYPE = "node.stats.extended.type";
    public final static String EXTENDED_STATS_DEFAULT_TYPE = "none";

    private final Settings settings;
    private final Map<String, Class<? extends ExtendedNodeStats>> extendedStatsTypes = new HashMap<>();

    public StatsModule(Settings settings) {
        this.settings = settings;
        addExtendedStatsType("none", ZeroExtendedNodeStats.class);
    }

    public void addExtendedStatsType(String type, Class<? extends ExtendedNodeStats> clazz) {
        if (extendedStatsTypes.put(type, clazz) != null) {
            throw new IllegalArgumentException("Extended node stats type [" + type + "] is already registered");
        }
    }

    @Override
    protected void configure() {
        String statsType = settings.get(EXTENDED_STATS_TYPE, EXTENDED_STATS_DEFAULT_TYPE);
        Class<? extends ExtendedNodeStats> statsClass = extendedStatsTypes.get(statsType);
        if (statsClass == null) {
            throw new IllegalArgumentException("Unknown extended node stats type [" + statsType + "]");
        }

        bind(ExtendedNodeStats.class).to(statsClass).asEagerSingleton();
    }
}
