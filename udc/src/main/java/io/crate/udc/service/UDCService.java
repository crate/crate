/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.ClusterIdService;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.udc.ping.PingTask;
import io.crate.udc.plugin.UDCPlugin;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Timer;

public class UDCService extends AbstractLifecycleComponent {

    private final Timer timer;

    private final ClusterService clusterService;
    private final Provider<ClusterIdService> clusterIdServiceProvider;
    private final ExtendedNodeInfo extendedNodeInfo;

    @Inject
    public UDCService(Settings settings,
                      ExtendedNodeInfo extendedNodeInfo,
                      ClusterService clusterService,
                      Provider<ClusterIdService> clusterIdServiceProvider) {
        super(settings);
        this.extendedNodeInfo = extendedNodeInfo;
        this.clusterService = clusterService;
        this.clusterIdServiceProvider = clusterIdServiceProvider;
        this.timer = new Timer("crate-udc");
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        String url = settings.get(UDCPlugin.URL_SETTING_NAME, UDCPlugin.URL_DEFAULT_SETTING);
        TimeValue initialDelay = settings.getAsTime(UDCPlugin.INITIAL_DELAY_SETTING_NAME, UDCPlugin.INITIAL_DELAY_DEFAULT_SETTING);
        TimeValue interval = settings.getAsTime(UDCPlugin.INTERVAL_SETTING_NAME, UDCPlugin.INTERVAL_DEFAULT_SETTING);

        if (logger.isDebugEnabled()) {
            logger.debug("Starting with delay {} and period {}.", initialDelay.getSeconds(), interval.getSeconds());
        }
        PingTask pingTask = new PingTask(clusterService, clusterIdServiceProvider.get(), extendedNodeInfo, url, settings);
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
