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

package io.crate.operation.reference.sys.check.node;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.MonitorService;

@Singleton
public class LowDiskWatermarkNodesSysCheck extends DiskWatermarkNodesSysCheck {

    static final int ID = 6;
    private static final String DESCRIPTION = "The low disk watermark is exceeded on the node." +
        " The cluster will not allocate new shards to the node. Please check the node disk usage.";

    @Inject
    public LowDiskWatermarkNodesSysCheck(ClusterService clusterService,
                                         Settings settings,
                                         MonitorService monitorService) {
        super(ID, DESCRIPTION, Severity.HIGH, clusterService, monitorService.fsService(), settings);
    }

    @Override
    protected boolean validate(long free, long total) {
        return !(free < diskThresholdSettings.getFreeBytesThresholdLow().getBytes()
            || getFreeDiskAsPercentage(free, total) < diskThresholdSettings.getFreeDiskThresholdLow());
    }
}
