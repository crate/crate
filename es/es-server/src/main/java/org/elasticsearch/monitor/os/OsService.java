/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.os;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.common.util.concurrent.EsExecutors;

public class OsService extends AbstractComponent {

    private final OsProbe probe;
    private final OsInfo info;
    private final SingleObjectCache<OsStats> osStatsCache;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("monitor.os.refresh_interval", TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(1),
                Property.NodeScope);

    public OsService(Settings settings) {
        super(settings);
        this.probe = OsProbe.getInstance();
        TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.info = probe.osInfo(refreshInterval.millis(), EsExecutors.numberOfProcessors(settings));
        this.osStatsCache = new OsStatsCache(refreshInterval, probe.osStats());
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    public OsInfo info() {
        return this.info;
    }

    public synchronized OsStats stats() {
        return osStatsCache.getOrRefresh();
    }

    private class OsStatsCache extends SingleObjectCache<OsStats> {
        OsStatsCache(TimeValue interval, OsStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected OsStats refresh() {
            return probe.osStats();
        }
    }
}
