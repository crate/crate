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

package io.crate.es.monitor.fs;

import org.apache.logging.log4j.Logger;
import io.crate.es.cluster.ClusterInfo;
import io.crate.es.common.Nullable;
import io.crate.es.common.component.AbstractComponent;
import io.crate.es.common.settings.Setting;
import io.crate.es.common.settings.Setting.Property;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.unit.TimeValue;
import io.crate.es.common.util.SingleObjectCache;
import io.crate.es.env.NodeEnvironment;
import io.crate.es.cluster.ClusterInfoService;

import java.io.IOException;

public class FsService extends AbstractComponent {

    private final FsProbe probe;
    private final TimeValue refreshInterval;
    private final SingleObjectCache<FsInfo> cache;
    private final ClusterInfoService clusterInfoService;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting(
            "monitor.fs.refresh_interval",
            TimeValue.timeValueSeconds(1),
            TimeValue.timeValueSeconds(1),
            Property.NodeScope);

    public FsService(final Settings settings, final NodeEnvironment nodeEnvironment, ClusterInfoService clusterInfoService) {
        super(settings);
        this.probe = new FsProbe(settings, nodeEnvironment);
        this.clusterInfoService = clusterInfoService;
        refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        logger.debug("using refresh_interval [{}]", refreshInterval);
        cache = new FsInfoCache(refreshInterval, stats(probe, null, logger, null));
    }

    public FsInfo stats() {
        return cache.getOrRefresh();
    }

    private static FsInfo stats(FsProbe probe, FsInfo initialValue, Logger logger, @Nullable ClusterInfo clusterInfo) {
        try {
            return probe.stats(initialValue, clusterInfo);
        } catch (IOException e) {
            logger.debug("unexpected exception reading filesystem info", e);
            return null;
        }
    }

    private class FsInfoCache extends SingleObjectCache<FsInfo> {

        private final FsInfo initialValue;

        FsInfoCache(TimeValue interval, FsInfo initialValue) {
            super(interval, initialValue);
            this.initialValue = initialValue;
        }

        @Override
        protected FsInfo refresh() {
            return stats(probe, initialValue, logger, clusterInfoService.getClusterInfo());
        }

    }

}
