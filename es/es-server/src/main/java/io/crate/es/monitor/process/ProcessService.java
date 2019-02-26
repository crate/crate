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

package io.crate.es.monitor.process;

import io.crate.es.common.component.AbstractComponent;
import io.crate.es.common.settings.Setting;
import io.crate.es.common.settings.Setting.Property;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.unit.TimeValue;
import io.crate.es.common.util.SingleObjectCache;

public final class ProcessService extends AbstractComponent {

    private final ProcessProbe probe;
    private final ProcessInfo info;
    private final SingleObjectCache<ProcessStats> processStatsCache;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("monitor.process.refresh_interval", TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(1),
            Property.NodeScope);

    public ProcessService(Settings settings) {
        super(settings);
        this.probe = ProcessProbe.getInstance();
        final TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        processStatsCache = new ProcessStatsCache(refreshInterval, probe.processStats());
        this.info = probe.processInfo(refreshInterval.millis());
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    public ProcessInfo info() {
        return this.info;
    }

    public ProcessStats stats() {
        return processStatsCache.getOrRefresh();
    }

    private class ProcessStatsCache extends SingleObjectCache<ProcessStats> {
        ProcessStatsCache(TimeValue interval, ProcessStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected ProcessStats refresh() {
            return probe.processStats();
        }
    }
}
