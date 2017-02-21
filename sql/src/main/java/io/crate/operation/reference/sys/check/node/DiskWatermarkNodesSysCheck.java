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

import com.google.common.annotations.VisibleForTesting;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.fs.FsProbe;

import java.io.IOException;


abstract class DiskWatermarkNodesSysCheck extends AbstractSysNodeCheck {

    private static final ESLogger LOGGER = Loggers.getLogger(DiskWatermarkNodesSysCheck.class);

    private final FsProbe fsProbe;
    private final Provider<DiskThresholdDecider> deciderProvider;

    DiskWatermarkNodesSysCheck(int id,
                               String description,
                               Severity severity,
                               ClusterService clusterService,
                               Provider<DiskThresholdDecider> deciderProvider,
                               FsProbe fsProbe) {
        super(id, description, severity, clusterService);
        this.deciderProvider = deciderProvider;
        this.fsProbe = fsProbe;
    }

    @Override
    public boolean validate() {
        try {
            DiskThresholdDecider decider = deciderProvider.get();
            if (!decider.isEnabled()) return false;

            FsInfo.Path leastAvailablePath = getLeastAvailablePath();
            return validate(decider,
                leastAvailablePath.getAvailable().getBytes(),
                leastAvailablePath.getTotal().getBytes()
            );
        } catch (IOException e) {
            LOGGER.error("Unable to determine the node disk usage while validating high/low disk watermark check: ", e);
            return false;
        }
    }

    protected abstract boolean validate(DiskThresholdDecider decider, long free, long total);

    // if the path with least available disk space violates the check,
    // then there is no reason to run a check against other paths
    @VisibleForTesting
    FsInfo.Path getLeastAvailablePath() throws IOException {
        FsInfo.Path leastAvailablePath = null;
        for (FsInfo.Path info : fsProbe.stats()) {
            if (leastAvailablePath == null) {
                leastAvailablePath = info;
            } else if (leastAvailablePath.getAvailable().getBytes() > info.getAvailable().getBytes()) {
                leastAvailablePath = info;
            }
        }
        assert leastAvailablePath != null : "must be at least one path";
        return leastAvailablePath;
    }

    double getFreeDiskAsPercentage(long free, long total) {
        if (total == 0) {
            return 100.0;
        }
        return 100.0 * ((double) free / total);
    }
}
