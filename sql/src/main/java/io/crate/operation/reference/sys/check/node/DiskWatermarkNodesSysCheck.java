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

import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.StringSetting;
import io.crate.operation.reference.sys.check.AbstractSysNodeCheck;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.service.ClusterService;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.fs.FsProbe;

import java.io.IOException;


abstract class DiskWatermarkNodesSysCheck extends AbstractSysNodeCheck {

    private static final Logger LOGGER = Loggers.getLogger(DiskWatermarkNodesSysCheck.class);

    private final StringSetting watermarkSetting;
    private final Settings settings;
    private final FsProbe fsProbe;
    private final FsInfo fsInfo;

    DiskWatermarkNodesSysCheck(int id,
                               String description,
                               StringSetting watermarkSetting,
                               Severity severity,
                               ClusterService clusterService,
                               Settings settings,
                               FsProbe fsProbe,
                               FsInfo fsInfo) {
        super(id, description, severity, clusterService);
        this.settings = settings;
        this.fsProbe = fsProbe;
        this.fsInfo = fsInfo;
        this.watermarkSetting = watermarkSetting;
    }

    @Override
    public boolean validate() {
        try {
            return !thresholdEnabled() ||
                   validate(fsProbe.stats(fsInfo), thresholdPercentageFromWatermark(), thresholdBytesFromWatermark());
        } catch (IOException e) {
            LOGGER.error("Unable to determine the node disk usage while validating high/low disk watermark check: ", e);
            return false;
        }
    }

    protected boolean validate(FsInfo fsInfo, double diskWatermarkPercents, long diskWatermarkBytes) {
        for (FsInfo.Path path : fsInfo) {
            double usedDiskAsPercentage = 100.0 - (path.getAvailable().getBytes() / (double) path.getTotal().getBytes()) * 100.0;

            // Byte values refer to free disk space
            // Percentage values refer to used disk space
            if ((usedDiskAsPercentage > diskWatermarkPercents)
                || (path.getAvailable().getBytes() < diskWatermarkBytes)) {
                return false;
            }
        }
        return true;
    }

    private double thresholdPercentageFromWatermark() {
        try {
            return RatioValue.parseRatioValue(watermarkSetting.extract(settings)).getAsPercent();
        } catch (ElasticsearchParseException ex) {
            return 100.0;
        }
    }

    private long thresholdBytesFromWatermark() {
        try {
            return ByteSizeValue.parseBytesSizeValue(
                watermarkSetting.extract(settings),
                watermarkSetting.name()
            ).getBytes();
        } catch (ElasticsearchParseException ex) {
            return ByteSizeValue.parseBytesSizeValue("0b", watermarkSetting.name()).getBytes();
        }
    }

    private boolean thresholdEnabled() {
        return CrateSettings.ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED.extract(settings);
    }
}
