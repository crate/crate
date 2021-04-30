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

package io.crate.expression.reference.sys.check.node;

import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.fs.FsService;


abstract class DiskWatermarkNodesSysCheck extends AbstractSysNodeCheck {

    private final FsService fsService;
    final DiskThresholdSettings diskThresholdSettings;

    DiskWatermarkNodesSysCheck(int id,
                               String description,
                               Severity severity,
                               ClusterService clusterService,
                               FsService fsService,
                               Settings settings) {
        super(id, description, severity);
        this.fsService = fsService;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterService.getClusterSettings());
    }

    @Override
    public boolean isValid() {
        if (!diskThresholdSettings.isEnabled()) {
            return true;
        }
        DiskUsage leastDiskEstimate = fsService.stats().getLeastDiskEstimate();
        if (leastDiskEstimate == null) {
            // DiskUsage can be unavailable during cluster start-up
            return true;
        }
        return isValid(leastDiskEstimate.getFreeBytes(), leastDiskEstimate.getTotalBytes());
    }

    protected abstract boolean isValid(long free, long total);

    static double getFreeDiskAsPercentage(long free, long total) {
        if (total == 0) {
            return 100.0;
        }
        return 100.0 * ((double) free / total);
    }
}
