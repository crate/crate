/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.udc.service;

import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.unit.TimeValue;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.types.DataTypes;
import io.crate.udc.ping.PingTask;

public class UDCService extends AbstractLifecycleComponent {

    private static final Logger LOGGER = LogManager.getLogger(UDCService.class);

    public static final Setting<Boolean> UDC_ENABLED_SETTING = Setting.boolSetting(
        "udc.enabled", true, Property.NodeScope, Property.Exposed);

    // Explicit generic is required for eclipse JDT, otherwise it won't compile
    public static final Setting<String> UDC_URL_SETTING = new Setting<>(
        "udc.url", "https://udc.crate.io/",
        Function.identity(), DataTypes.STRING, Property.NodeScope, Property.Exposed);

    public static final Setting<TimeValue> UDC_INITIAL_DELAY_SETTING = Setting.positiveTimeSetting(
        "udc.initial_delay", new TimeValue(10, TimeUnit.MINUTES),
        Property.NodeScope,
        Property.Exposed);

    public static final Setting<TimeValue> UDC_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "udc.interval", new TimeValue(24, TimeUnit.HOURS),
        Property.NodeScope,
        Property.Exposed
    );

    private final Timer timer;

    private final ClusterService clusterService;
    private final ExtendedNodeInfo extendedNodeInfo;
    private final Settings settings;

    @Inject
    public UDCService(Settings settings,
                      ExtendedNodeInfo extendedNodeInfo,
                      ClusterService clusterService) {
        this.settings = settings;
        this.extendedNodeInfo = extendedNodeInfo;
        this.clusterService = clusterService;
        this.timer = new Timer("crate-udc");
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        String url = UDC_URL_SETTING.get(settings);
        TimeValue initialDelay = UDC_INITIAL_DELAY_SETTING.get(settings);
        TimeValue interval = UDC_INTERVAL_SETTING.get(settings);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting with delay {} and period {}.", initialDelay.seconds(), interval.seconds());
        }
        PingTask pingTask = new PingTask(clusterService, extendedNodeInfo, url);
        timer.scheduleAtFixedRate(pingTask, initialDelay.millis(), interval.millis());
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        timer.cancel();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        timer.cancel(); // safety net, in case of unlikely weirdness
    }
}
